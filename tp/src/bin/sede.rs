use actix::{Actor, Addr, AsyncContext, Context, Handler, MailboxError, Message, MessageResult, ResponseFuture};
use matenmutex::messages::cliente_sede::{
    AckComprobanteAlquiler, DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::slot;
use matenmutex::slot::{AsegurarBici, Confirmar, ConsultarBici, Reservar, ResultadoReserva, Slot};
use matenmutex::token::{self, PayloadToken, SECRET_LEN};
use matenmutex::types::{timestamp_actual, Bici, BiciAlquilada};
use serde::Serialize;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::RwLock;
use tokio::time::sleep;
use uuid::Uuid;
use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use matenmutex::messages::sedes::{Heartbeat, MsgSedeASede, SedeInfo, crear_info_dummy, ActualizarDisponibilidad, GetInfo, GetSedesConocidas};
use matenmutex::utils::{DISTANCIA_MINIMA, PREMONTO, PUERTO_BASE, TASA};

pub struct Sede {
    sede_id: u32,
    coords: (f64, f64),
    slots: Vec<Addr<Slot>>,
    token_secret: [u8; SECRET_LEN],
    pending_reservas: std::collections::HashMap<String, (usize, BiciAlquilada)>,
    sedes_conocidas: Vec<SedeInfo>,
    mi_info: SedeInfo,
}



fn id_de_addr(addr: &SocketAddr) -> u32 {
    (addr.port() - PUERTO_BASE) as u32
}

fn addr_de_id(id: u32, ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, PUERTO_BASE + id as u16)
}

impl Sede {
    fn cliente_en_rango(&self, coords: (f64, f64)) -> bool {
        self.calcular_distancia(coords) <= DISTANCIA_MINIMA
    }

    fn cliente_tiene_dinero_solicitar(&self, dinero: u32) -> bool {
        dinero > PREMONTO
    }
    fn calcular_distancia(&self, coords: (f64, f64)) -> f64 {
        let (coords_sede_x, coords_sede_y) = self.coords;
        let (coords_cliente_x, coords_cliente_y) = coords;

        ((coords_sede_x - coords_cliente_x).powi(2) + (coords_sede_y - coords_cliente_y).powi(2)).sqrt()
    }

    fn generar_comprobante(sede_id: u32, cliente_id: u32, token_secret: &[u8; 32], bici_id: String) -> Result<BiciAlquilada, token::TokenError> {
        let alquiler_id = Uuid::new_v4().to_string();
        let inicio_ts = timestamp_actual();

        let payload = PayloadToken {
            bici_id: bici_id.clone(),
            alquiler_id: alquiler_id.clone(),
            cliente_id: cliente_id.to_string(),
            sede_origen_id: sede_id,
            inicio_ts,
        };
        let token_alquiler = token::generar_token(&payload, token_secret)?;

        Ok(BiciAlquilada::new(bici_id, alquiler_id, cliente_id.to_string(), sede_id, inicio_ts, token_alquiler))
    }

    async fn reservar_bici(slots: &Vec<Addr<Slot>>) -> Option<(usize, String)> {

        let mut bici_ganadora: Option<(usize, String)> = None;
        for (idx, slot) in slots.iter().enumerate() {
            match slot.send(Reservar).await {
                Ok(ResultadoReserva::Reservada(bici_id)) => {
                    bici_ganadora = Some((idx, bici_id));
                    break;
                }
                Ok(ResultadoReserva::Rechazada) => continue,
                Err(_) => continue,
            }
        }

        bici_ganadora
    }

}

impl Actor for Sede {
    type Context = Context<Self>;
}

impl Handler<ActualizarDisponibilidad> for Sede {
    type Result = ();

    fn handle(&mut self, _msg: ActualizarDisponibilidad, _ctx: &mut Self::Context) -> Self::Result {
        self.mi_info.slots_disponibles -= 1;
        self.mi_info.ultimo_update_ts = timestamp_actual();
    }
}

impl Handler<GetInfo> for Sede {
    type Result = MessageResult<GetInfo>;

    fn handle(&mut self, _msg: GetInfo, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.mi_info.clone())
    }
}

impl Handler<GetSedesConocidas> for Sede {
    type Result = MessageResult<GetSedesConocidas>;

    fn handle(&mut self, _msg: GetSedesConocidas, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.sedes_conocidas.clone())
    }
}

impl Handler<SolicitarBici> for Sede {
    type Result = ResponseFuture<RespuestaSolicitarBici>;

    fn handle(&mut self, msg: SolicitarBici, ctx: &mut Context<Self>) -> Self::Result {
        println!("Hola Bici");

        if !self.cliente_en_rango(msg.coords) {
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(String::from("Para poder tomar la bici debe estar mas cerca de la sede")) });
        }

        if !self.cliente_tiene_dinero_solicitar(msg.dinero) {
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(String::from("Para poder tomar la bici debe tener minimo x guita")) });
        }

        let slots = self.slots.clone();
        let sede_id = self.sede_id;
        let cliente_id = msg.cliente_id;
        let mut dinero = msg.dinero;
        let token_secret = self.token_secret;
        let sede_addr = ctx.address();

        Box::pin(async move {
            match Self::reservar_bici(&slots).await {
                None => {
                    println!("[sede] Envia AlquilerRechazado: no hay slots disponibles");
                    RespuestaSolicitarBici::AlquilerRechazado(String::from("No hay bicicletas disponibles en este momento"))
                }
                Some((idx, bici_id)) => {
                    match Self::generar_comprobante(sede_id, cliente_id, &token_secret, bici_id) {
                        Err(_) => {
                            println!("[sede] Envia AlquilerRechazado: error generando token");
                            RespuestaSolicitarBici::AlquilerRechazado(String::from("Error interno al generar el token de alquiler"))
                        }
                        Ok(bici) => {
                            println!("[sede] Envia ComprobanteAlquilerEmitido (bici {}, alquiler {})", bici.bici_id(), bici.alquiler_id());
                            let _ = sede_addr.send(GuardarReserva { alquiler_id: bici.alquiler_id().to_string(), slot_idx: idx, bici: bici.clone() }).await;
                            let _ = sede_addr.send(ActualizarDisponibilidad).await;
                            dinero -= PREMONTO;
                            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, dinero)
                        }
                    }
                }
            }
        })
    }
}

struct GuardarReserva {
    alquiler_id: String,
    slot_idx: usize,
    bici: BiciAlquilada,
}

impl actix::Message for GuardarReserva {
    type Result = ();
}

impl Handler<GuardarReserva> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarReserva, _ctx: &mut Context<Self>) {
        self.pending_reservas.insert(msg.alquiler_id, (msg.slot_idx, msg.bici));
    }
}

impl Handler<AckComprobanteAlquiler> for Sede {
    type Result = ResponseFuture<RespuestaSolicitarBici>;

    fn handle(&mut self, msg: AckComprobanteAlquiler, _ctx: &mut Context<Self>) -> Self::Result {
        match self.pending_reservas.remove(&msg.alquiler_id) {
            None => Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(String::from("No hay reserva pendiente para ese alquiler")) }),
            Some((slot_idx, bici)) => {
                let slots = self.slots.clone();
                println!("[sede] Recibe AckComprobanteAlquiler (alquiler {}) confirmando slot {}", msg.alquiler_id, slot_idx);
                Box::pin(async move {
                    let _ = slots[slot_idx].send(Confirmar).await;
                    println!("[sede] Envia AlquilerConfirmado (bici {})", bici.bici_id());
                    RespuestaSolicitarBici::AlquilerConfirmado(bici)
                })
            }
        }
    }
}

impl Handler<DevolverBici> for Sede {
    type Result = ResponseFuture<RespuestaDevolverBici>;

    fn handle(&mut self, msg: DevolverBici, _ctx: &mut Context<Self>) -> Self::Result {
        println!("Chau Bici");

        if !self.cliente_en_rango(msg.coords) {
            return Box::pin(async move {
                RespuestaDevolverBici::DevolucionRechazada(String::from("Para poder devolver la bici debe estar mas cerca de la sede"))
            });
        }

        let dinero = msg.dinero;
        let monto = match preparar_monto(msg.dinero, msg.bici.inicio_ts()) {
            Ok(costo) => costo,
            Err(respuesta) => return Box::pin(async move { respuesta }),
        };

        let slots = self.slots.clone();
        let bici_id = msg.bici.bici_id().to_string();

        Box::pin(async move {
            for slot in slots.iter() {
                match slot.send(AsegurarBici { bici_id: bici_id.clone() }).await {
                    Ok(true) => {

                        return RespuestaDevolverBici::DevolucionOk(Bici { bici_id }, dinero - monto);
                    }
                    _ => continue,
                }
            }

            RespuestaDevolverBici::DevolucionRechazada(String::from("No hay lugar disponible para devolver la bici en este momento"))
        })
    }
}

fn preparar_monto(dinero: u32, tiempo_inicio: u64) -> Result<u32, RespuestaDevolverBici> {
    let fin = timestamp_actual();
    let duracion = fin - tiempo_inicio;
    println!("Duracion del alquiler: {}", duracion);
    let costo = (duracion * TASA as u64) as u32;

    if costo > dinero {
        Err(RespuestaDevolverBici::DevolucionRechazada(String::from("Para poder devolver la bici recarga")))
    } else {
        Ok(costo)
    }
}

impl Handler<PreguntarBicis> for Sede {
    type Result = ResponseFuture<String>;

    fn handle(&mut self, _msg: PreguntarBicis, _ctx: &mut Context<Self>) -> Self::Result {

        let requests: Vec<_> = self
            .slots
            .iter()
            .map(|slot| slot.send(ConsultarBici)) // mensaje a cada slot
            .collect();

        Box::pin(async move {
            let resultados = futures::future::join_all(requests).await;
            let lineas: Vec<String> = resultados.into_iter().filter_map(|r| r.ok()).collect();

            let ocupados = lineas.iter().filter(|l| !l.contains("sin bici")).count();
            let total = lineas.len();

            let mut respuesta = lineas.join("   ");
            respuesta.push_str(&format!("--- {}/{} slots ocupados ---", ocupados, total));
            respuesta
        })
    }
}

impl Handler<Heartbeat> for Sede {
    type Result = ();

    fn handle(&mut self, msg: Heartbeat, _ctx: &mut Context<Self>) -> () {
        println!("[sede] Recibe Heartbeat de {}", msg.sede_id);
        println!("[sede] info: {:?}", msg.info);

        match self.sedes_conocidas.iter_mut().find(|s| s.sede_id == msg.info.sede_id) {
            Some(known) => *known = msg.info,
            None => self.sedes_conocidas.push(msg.info),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, DevolverBici, SolicitarBici};
    use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
    use matenmutex::slot::{crear_slots_con_estados, EstadoSlot};

    fn sede_con_estados(estados: Vec<EstadoSlot>) -> Addr<Sede> {
        let dummy_info = crear_info_dummy();
        let slots = crear_slots_con_estados(estados);
        Sede {
            sede_id: 1,
            coords: (-34.6037, -58.3816),
            slots,
            token_secret: token::cargar_secret("secreto-de-test"),
            pending_reservas: std::collections::HashMap::new(),
            sedes_conocidas: vec![],
            mi_info: dummy_info,
        }
        .start()
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_slot_disponible_confirma_con_bici_id_real() {
        let sede = sede_con_estados(vec![
            EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Disponible {
                bici_id: "1-3".to_string(),
            },
        ]);

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: 999999,
            })
            .await
            .unwrap();

        let bici_alquilada = match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => bici,
            RespuestaSolicitarBici::AlquilerRechazado(motivo) => {
                panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo AlquilerRechazado({motivo})");
            }
            RespuestaSolicitarBici::AlquilerConfirmado(_) => {
                panic!("se esperaba ComprobanteAlquilerEmitido, no AlquilerConfirmado directo");
            }
        };

        assert_eq!(bici_alquilada.bici_id(), "1-3");
        assert!(!bici_alquilada.alquiler_id().is_empty());
        assert!(bici_alquilada.inicio_ts() > 0);

        let alquiler_id = bici_alquilada.alquiler_id().to_string();
        let respuesta_final = sede
            .send(AckComprobanteAlquiler { alquiler_id })
            .await
            .unwrap();

        match respuesta_final {
            RespuestaSolicitarBici::AlquilerConfirmado(bici) => {
                assert_eq!(bici.bici_id(), "1-3");
            }
            other => panic!("se esperaba AlquilerConfirmado, se obtuvo {:?}", other),
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_sin_slots_disponibles_rechaza() {
        let sede = sede_con_estados(vec![
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-2".to_string(),
            },
        ]);

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: 9999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            RespuestaSolicitarBici::AlquilerConfirmado(_) | RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_,_) => {
                panic!("se esperaba AlquilerRechazado");
            }
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_concurrente_sobre_un_unico_slot_disponible_solo_uno_gana() {
        let sede = sede_con_estados(vec![EstadoSlot::Disponible {
            bici_id: "1-1".to_string(),
        }]);

        let sede_a = sede.clone();
        let sede_b = sede.clone();

        let (resp_a, resp_b) = futures::join!(
            sede_a.send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 9999999,
            }),
            sede_b.send(SolicitarBici {
                cliente_id: 2,
                coords: (-34.6037, -58.3816),
                dinero: 9999999,

            })
        );

        let ganadores = [resp_a.unwrap(), resp_b.unwrap()]
            .into_iter()
            .filter(|r| matches!(r, RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_,_)))
            .count();
        let rechazados = 2 - ganadores;

        assert_eq!(ganadores, 1, "exactamente un cliente debe recibir el comprobante");
        assert_eq!(rechazados, 1, "exactamente un cliente debe ser rechazado");
    }

    #[actix_rt::test]
    async fn devolver_bici_con_slot_vacio_asegura_la_bici_y_responde_ok() {
        let sede = sede_con_estados(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-3".to_string(),
            },
        ]);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (-34.6037, -58.3816),
                dinero: 999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionOk(bici, ..) => {
                assert_eq!(bici.bici_id, "9-9");
            }
            RespuestaDevolverBici::DevolucionRechazada(motivo) => {
                panic!("se esperaba DevolucionOk, se obtuvo DevolucionRechazada({motivo})");
            }
        }
    }

    #[actix_rt::test]
    async fn devolver_bici_sin_slots_vacios_rechaza() {
        let sede = sede_con_estados(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Reservado {
                bici_id: "1-2".to_string(),
            },
        ]);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (-34.6037, -58.3816),
                dinero: 999999,

            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionRechazada(_) => {}
            RespuestaDevolverBici::DevolucionOk(_, ..) => {
                panic!("se esperaba DevolucionRechazada, se obtuvo DevolucionOk");
            }
        }
    }

    #[actix_rt::test]
    async fn devolver_bici_cliente_fuera_de_rango_rechaza() {
        let sede = sede_con_estados(vec![EstadoSlot::Vacio]);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (0.0, 0.0),
                dinero: 999999,

            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionRechazada(_) => {}
            RespuestaDevolverBici::DevolucionOk(_, ..) => {
                panic!("se esperaba DevolucionRechazada por estar fuera de rango, se obtuvo DevolucionOk");
            }
        }
    }
    
    
}


async fn formatear_respuesta<T: Serialize>(resultado: Result<T, MailboxError>) -> String {
    match resultado {
        Ok(valor) => match serde_json::to_string(&valor) {
            Ok(json) => json + "\n",
            Err(e) => {
                eprintln!("Error serializando respuesta: {}", e);
                String::from("ERROR:no se pudo serializar la respuesta\n")
            }
        },
        Err(e) => {
            eprintln!("Error enviando mensaje al actor Sede: {}", e);
            String::from("ERROR:la sede no pudo procesar el mensaje\n")
        }
    }
}

#[actix_rt::main]
async fn main() {
    dotenvy::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    let n = match numero_de_sede(args) {
        Some(sede) => sede,
        None => return,
    };

    let token_secret = match cargar_token_secret() {
        Some(token) => token,
        None => return,
    };

    let sede_id = n as u32;
    let num_slots = 20;
    let slots: Vec<Addr<Slot>> = slot::crear_slots(sede_id, num_slots);
    let coords = (-34.6037, -58.3816); //ver dps cambiar coords json?

    let mi_info = crear_sede_info(n, sede_id, coords);

    let sede = crear_sede(token_secret, sede_id, slots, coords, mi_info.clone());

    let public_addr = format!("127.0.0.1:808{}", n);
    let private_addr = format!("127.0.0.1:700{}", n);

    let public_listener = TcpListener::bind(&public_addr).await.unwrap();
    let udp = Arc::new(UdpSocket::bind(&private_addr).await.unwrap());

    println!("Sede {} — publica: {}, interna: {}", n, public_addr, private_addr);

    // receiver
    let udp_rx = udp.clone();
    let sede_rx = sede.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        loop {
            let (len, peer) = match udp_rx.recv_from(&mut buf).await {
                Ok(r) => r,
                Err(e) => { eprintln!("UDP recv error: {}", e); continue; }
            };
            match serde_json::from_slice::<MsgSedeASede>(&buf[..len]) {
                Ok(MsgSedeASede::Heartbeat(msg)) => {
                    sede_rx.send(msg).await.ok();
                }
                Err(e) => eprintln!("UDP parse error from {}: {}", peer, e),
            }
        }
    });

    // sender
    let udp_tx = udp.clone();
    let sede_tx = sede.clone();
    let other_sedes: Vec<u16> = (1..=5).filter(|&s| s != n).collect();
    tokio::spawn(async move {
        let mut bootstrapped = false;

        loop {
            let info = sede_tx.send(GetInfo).await.unwrap();
            let known = sede_tx.send(GetSedesConocidas).await.unwrap(); // new message

            let targets: Vec<String> = if !bootstrapped {
                bootstrapped = true;
                other_sedes.iter().map(|s| format!("127.0.0.1:700{}", s)).collect()
            } else {
                known.iter().map(|s| s.private_addr.clone()).collect()
            };

            let heartbeat = serde_json::to_vec(
                &MsgSedeASede::Heartbeat(Heartbeat { sede_id, info })
            ).unwrap();

            for addr in &targets {
                udp_tx.send_to(&heartbeat, addr.as_str()).await.ok();
            }

            sleep(Duration::from_secs(2)).await;
        }
    });

    loop_pub(sede, public_listener).await;
}

fn crear_sede_info(n: u16, sede_id: u32, coords: (f64, f64)) -> SedeInfo {
    let mi_info = SedeInfo {
        sede_id,
        coords,
        public_addr: format!("127.0.0.1:808{}", n),
        private_addr: format!("127.0.0.1:700{}", n),
        slots_vacios: 99,
        slots_disponibles: 99,
        conectada: true,
        ultimo_update_ts: timestamp_actual(),
    };
    mi_info
}

fn crear_sede(token_secret: [u8; 32], sede_id: u32, slots: Vec<Addr<Slot>>, coords: (f64, f64), mi_info: SedeInfo) -> Addr<Sede> {
    let sede = Sede {
        sede_id: sede_id,
        coords: coords,
        slots: slots,
        token_secret,
        pending_reservas: std::collections::HashMap::new(),
        sedes_conocidas: vec![],
        mi_info,
    }
        .start();
    sede
}

fn cargar_token_secret() -> Option<[u8; 32]> {
    let token_secret_raw = match env::var("TOKEN_SECRET") {
        Ok(raw) => raw,
        Err(_) => {
            eprintln!("Falta la variable de entorno TOKEN_SECRET — no se puede iniciar la sede");
            return None;
        }
    };
    let token_secret = token::cargar_secret(&token_secret_raw);
    Some(token_secret)
}

fn numero_de_sede(args: Vec<String>) -> Option<u16> {
    let n: u16 = match args.get(1).and_then(|arg| arg.parse().ok()) {
        Some(n) => n,
        None => {
            eprintln!("Uso: sede <n> — <n> debe ser un numero de puerto valido (u16)");
            return None;
        }
    };
    Some(n)
}

async fn loop_pub(sede: Addr<Sede>, public_listener: TcpListener) {
    loop {
        let (socket, addr) = match public_listener.accept().await {
            Ok(conexion) => conexion,
            Err(e) => {
                eprintln!("Error aceptando conexion entrante: {}", e);
                continue;
            }
        };
        println!("New connection from {}", addr);
        let sede = sede.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let response = match serde_json::from_str::<MsgClienteASede>(line.trim()) {
                    Ok(MsgClienteASede::SolicitarBici(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::AckComprobanteAlquiler(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::DevolverBici(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::PreguntarBicis(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Err(_) => format!("UNKNOWN:{}\n", line.trim()),
                };

                if let Err(e) = writer.write_all(response.as_bytes()).await {
                    eprintln!("Error escribiendo respuesta a {}: {}", addr, e);
                    break;
                }
            }
            println!("Connection from {} closed", addr);
        });
    }
}


