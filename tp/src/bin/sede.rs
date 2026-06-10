use actix::{Actor, Addr, AsyncContext, Context, Handler, MailboxError, ResponseFuture};
use matenmutex::messages::cliente_sede::{
    AckComprobanteAlquiler, DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::slot;
use matenmutex::slot::{AsegurarBici, Confirmar, ConsultarBici, Reservar, ResultadoReserva, Slot};
use matenmutex::token::{self, PayloadToken, SECRET_LEN};
use matenmutex::types::{timestamp_actual, Bici, BiciAlquilada};
use serde::Serialize;
use std::env;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use uuid::Uuid;
use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::slot::Cancelar;
use matenmutex::common::{DISTANCIA_MINIMA, PREMONTO, TASA};
use std::io;
use tokio::net::TcpStream;

const PROCESADOR_PAGOS_ADDR_DEFAULT: &str = "127.0.0.1:6001";

pub struct Sede {
    sede_id: u32,
    coords: (f64, f64),
    slots: Vec<Addr<Slot>>,
    token_secret: [u8; SECRET_LEN],
    pending_reservas: std::collections::HashMap<String, (usize, BiciAlquilada)>,
    procesador_pagos_addr: String,
}

/// Abre una conexion TCP nueva al ProcesadorPagos, envia un mensaje
/// JSON-lines y espera la respuesta. Es lo mismo que hace enviar_mensaje_a_sede en cliente.rs.
async fn enviar_a_procesador_pagos(
    addr: &str,
    msg: MsgSedeAProcesadorPagos,
) -> Result<MsgProcesadorPagosASede, io::Error> {
    let stream = TcpStream::connect(addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let payload = serde_json::to_string(&msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        + "\n";
    writer.write_all(payload.as_bytes()).await?;

    match lines.next_line().await? {
        Some(l) => serde_json::from_str(&l).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "procesador cerro la conexion")),
    }
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

    fn generar_msg_de_cobro(monto: u32, alquiler_id: String, cliente_id: String) -> MsgSedeAProcesadorPagos {
        let payment_request_id = Uuid::new_v4().to_string();
        let cobro = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id,
            alquiler_id,
            cliente_id,
            monto_final: monto as f64,
        };
        cobro
    }
}

impl Actor for Sede {
    type Context = Context<Self>;
}

impl Handler<SolicitarBici> for Sede {
    type Result = ResponseFuture<RespuestaSolicitarBici>;

    fn handle(&mut self, msg: SolicitarBici, ctx: &mut Context<Self>) -> Self::Result {
        println!("Hola Bici");

        if !self.cliente_en_rango(msg.coords) {
            let motivo = String::from("Para poder tomar la bici debe estar mas cerca de la sede");
            println!("[sede] Envia AlquilerRechazado: {}", motivo);
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(motivo) });
        }

        if !self.cliente_tiene_dinero_solicitar(msg.dinero) {
            let motivo = format!("Para poder tomar la bici debe tener mas dinero (Minimo ${})", PREMONTO);
            println!("[sede] Envia AlquilerRechazado: {}", motivo);
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(motivo) });
        }

        let slots = self.slots.clone();
        let sede_id = self.sede_id;
        let cliente_id = msg.cliente_id;
        let mut dinero = msg.dinero;
        let token_secret = self.token_secret;
        let sede_addr = ctx.address();
        let procesador_pagos_addr = self.procesador_pagos_addr.clone();

        Box::pin(async move {
            match Self::reservar_bici(&slots).await {
                None => {
                    println!("[sede] Envia AlquilerRechazado: no hay slots disponibles");
                    RespuestaSolicitarBici::AlquilerRechazado(String::from("No hay bicicletas disponibles en este momento"))
                }
                Some((idx, bici_id)) => {
                    let payment_request_id = Uuid::new_v4().to_string();
                    let solicitud_pre_autorizacion = MsgSedeAProcesadorPagos::PreAutorizarPago {
                        payment_request_id: payment_request_id.clone(),
                        cliente_id: cliente_id.to_string(),
                        monto_seguridad: PREMONTO as f64,
                    };
                    println!("[sede] Envia {}", solicitud_pre_autorizacion.describir());
                    let preautorizacion = enviar_a_procesador_pagos(&procesador_pagos_addr, solicitud_pre_autorizacion).await;

                    match preautorizacion {
                        Ok(MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id }) => {
                            println!("[sede] Recibe {}", MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id: payment_request_id.clone(), pago_id: pago_id.clone() }.describir());
                            match Self::generar_comprobante(sede_id, cliente_id, &token_secret, bici_id) {
                                Err(_) => {
                                    println!("[sede] Envia AlquilerRechazado: error generando token");
                                    let _ = slots[idx].send(Cancelar).await;
                                    RespuestaSolicitarBici::AlquilerRechazado(String::from("Error interno al generar el token de alquiler"))
                                }
                                Ok(bici) => {
                                    println!("[sede] Envia ComprobanteAlquilerEmitido (bici {}, alquiler {})", bici.bici_id(), bici.alquiler_id());
                                    let _ = sede_addr.send(GuardarReserva { alquiler_id: bici.alquiler_id().to_string(), slot_idx: idx, bici: bici.clone() }).await;
                                    dinero -= PREMONTO;
                                    RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, dinero)
                                }
                            }
                        }
                        Ok(MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo }) => {
                            println!("[sede] Recibe {}", MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo: motivo.clone() }.describir());
                            let _ = slots[idx].send(Cancelar).await;
                            RespuestaSolicitarBici::AlquilerRechazado(format!("El procesador de pagos rechazo la pre-autorizacion: {}", motivo))
                        }
                        Ok(_) => {
                            println!("[sede] Respuesta inesperada del procesador de pagos");
                            let _ = slots[idx].send(Cancelar).await;
                            RespuestaSolicitarBici::AlquilerRechazado(String::from("Respuesta inesperada del procesador de pagos"))
                        }
                        Err(e) => {
                            println!("[sede] Error comunicandose con el procesador de pagos: {}", e);
                            let _ = slots[idx].send(Cancelar).await;
                            RespuestaSolicitarBici::AlquilerRechazado(String::from("No se pudo procesar el pago de pre-autorizacion"))
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
            let motivo = String::from("Para poder devolver la bici debe estar mas cerca de la sede");
            println!("[sede] Envia DevolucionRechazada: {}", motivo);
            return Box::pin(async move { RespuestaDevolverBici::DevolucionRechazada(motivo) });
        }

        let dinero = msg.dinero;
        let monto = match preparar_monto(msg.dinero, msg.bici.inicio_ts()) {
            Ok(costo) => costo,
            Err(respuesta) => {
                if let RespuestaDevolverBici::DevolucionRechazada(ref motivo) = respuesta {
                    println!("[sede] Envia DevolucionRechazada: {}", motivo);
                }
                return Box::pin(async move { respuesta });
            }
        };

        let slots = self.slots.clone();
        let bici_id = msg.bici.bici_id().to_string();
        let procesador_pagos_addr = self.procesador_pagos_addr.clone();
        let alquiler_id = msg.bici.alquiler_id().to_string();
        let cliente_id = msg.bici.cliente_id().to_string();

        Box::pin(async move {
            let cobro = Self::generar_msg_de_cobro(monto, alquiler_id, cliente_id);
            println!("[sede] Envia {}", cobro.describir());

            match enviar_a_procesador_pagos(&procesador_pagos_addr, cobro).await {
                Ok(MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id }) => {
                    println!("[sede] Recibe {}", MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id }.describir());
                }
                Ok(MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo }) => {
                    println!("[sede] Recibe {}", MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo: motivo.clone() }.describir());
                    return RespuestaDevolverBici::DevolucionRechazada(format!("El procesador de pagos rechazo el cobro: {}", motivo));
                }
                Ok(otra) => {
                    println!("[sede] Recibe {}", otra.describir());
                    return RespuestaDevolverBici::DevolucionRechazada(String::from("Respuesta inesperada del procesador de pagos"));
                }
                Err(e) => {
                    println!("[sede] Error comunicandose con el procesador de pagos: {}", e);
                    return RespuestaDevolverBici::DevolucionRechazada(String::from("No se pudo procesar el cobro de la devolucion"));
                }
            }

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

#[cfg(test)]
mod tests {
    use super::*;
    use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, DevolverBici, SolicitarBici};
    use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
    use matenmutex::slot::{crear_slots_con_estados, EstadoSlot};

    fn sede_con_estados(estados: Vec<EstadoSlot>) -> Addr<Sede> {
        sede_con_estados_y_procesador(estados, PROCESADOR_PAGOS_ADDR_DEFAULT.to_string())
    }

    /// Levanta un ProcesadorPagos de prueba que responde siempre
    /// PagoPreAutorizado (o PagoRechazado si rechazar es true) a
    /// cualquier PreAutorizarPago recibido. Devuelve la direccion en la que escucha.
    async fn stub_procesador_pagos(rechazar: bool) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(c) => c,
                    Err(_) => break,
                };
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let resp = match serde_json::from_str::<MsgSedeAProcesadorPagos>(line.trim()) {
                            Ok(MsgSedeAProcesadorPagos::PreAutorizarPago { payment_request_id, .. }) => {
                                if rechazar {
                                    MsgProcesadorPagosASede::PagoRechazado {
                                        payment_request_id,
                                        motivo: "rechazo simulado en test".to_string(),
                                    }
                                } else {
                                    MsgProcesadorPagosASede::PagoPreAutorizado {
                                        payment_request_id,
                                        pago_id: "pago-test".to_string(),
                                    }
                                }
                            }
                            Ok(MsgSedeAProcesadorPagos::CobrarPago { payment_request_id, .. }) => {
                                if rechazar {
                                    MsgProcesadorPagosASede::PagoRechazado {
                                        payment_request_id,
                                        motivo: "rechazo simulado en test".to_string(),
                                    }
                                } else {
                                    MsgProcesadorPagosASede::PagoAceptado {
                                        payment_request_id,
                                        pago_id: "cobro-test".to_string(),
                                    }
                                }
                            }
                            _ => MsgProcesadorPagosASede::PagoRechazado {
                                payment_request_id: String::new(),
                                motivo: "mensaje no soportado por el stub".to_string(),
                            },
                        };
                        let payload = serde_json::to_string(&resp).unwrap() + "\n";
                        if writer.write_all(payload.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        addr
    }

    fn sede_con_estados_y_procesador(estados: Vec<EstadoSlot>, procesador_pagos_addr: String) -> Addr<Sede> {
        let slots = crear_slots_con_estados(estados);
        Sede {
            sede_id: 1,
            coords: (-34.6037, -58.3816),
            slots,
            token_secret: token::cargar_secret("secreto-de-test"),
            pending_reservas: std::collections::HashMap::new(),
            procesador_pagos_addr,
        }
        .start()
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_slot_disponible_confirma_con_bici_id_real() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Disponible {
                bici_id: "1-3".to_string(),
            },
        ], procesador_addr);

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
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![EstadoSlot::Disponible {
            bici_id: "1-1".to_string(),
        }], procesador_addr);

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
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-3".to_string(),
            },
        ], procesador_addr);

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
    async fn devolver_bici_con_cobro_rechazado_no_asegura() {
        let procesador_addr = stub_procesador_pagos(true).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-3".to_string(),
            },
        ], procesador_addr);

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

        // El slot vacio (idx 1) debe seguir vacio: AsegurarBici no debe
        // haberse ejecutado, por lo que Reservar (que solo aplica a slots
        // Disponible) sigue siendo Rechazada sobre el slot Vacio.
        let resultado = sede.send(ConsultarEstadoSlot { idx: 1 }).await.unwrap();
        assert_eq!(resultado, ResultadoReserva::Rechazada);
    }

    #[actix_rt::test]
    async fn devolver_bici_sin_slots_vacios_rechaza() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Reservado {
                bici_id: "1-2".to_string(),
            },
        ], procesador_addr);

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

    #[actix_rt::test]
    async fn solicitar_bici_con_preautorizacion_aceptada_emite_comprobante() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => {
                assert_eq!(bici.bici_id(), "1-1");
            }
            other => panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo {:?}", other),
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_preautorizacion_rechazada_cancela_la_reserva() {
        let procesador_addr = stub_procesador_pagos(true).await;
        let sede = sede_con_estados_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            other => panic!("se esperaba AlquilerRechazado, se obtuvo {:?}", other),
        }

        // El slot debe haber vuelto a Disponible (no quedo Reservado), por
        // lo que una nueva reserva sobre el mismo slot debe ser posible.
        let resultado = sede.send(ConsultarEstadoSlot { idx: 0 }).await.unwrap();
        assert_eq!(resultado, ResultadoReserva::Reservada("1-1".to_string()));
    }
}

#[cfg(test)]
struct ConsultarEstadoSlot {
    idx: usize,
}

#[cfg(test)]
impl actix::Message for ConsultarEstadoSlot {
    type Result = ResultadoReserva;
}

#[cfg(test)]
impl Handler<ConsultarEstadoSlot> for Sede {
    type Result = ResponseFuture<ResultadoReserva>;

    fn handle(&mut self, msg: ConsultarEstadoSlot, _ctx: &mut Context<Self>) -> Self::Result {
        let slot = self.slots[msg.idx].clone();
        Box::pin(async move { slot.send(Reservar).await.unwrap_or(ResultadoReserva::Rechazada) })
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
    let n: u16 = match args.get(1).and_then(|arg| arg.parse().ok()) {
        Some(n) => n,
        None => {
            eprintln!("Uso: sede <n> — <n> debe ser un numero de puerto valido (u16)");
            return;
        }
    };

    let token_secret_raw = match env::var("TOKEN_SECRET") {
        Ok(raw) => raw,
        Err(_) => {
            eprintln!("Falta la variable de entorno TOKEN_SECRET — no se puede iniciar la sede");
            return;
        }
    };
    let token_secret = token::cargar_secret(&token_secret_raw);

    let sede_id = n as u32;
    let num_slots = 20;
    let slots: Vec<Addr<Slot>> = slot::crear_slots(sede_id, num_slots);

    let procesador_pagos_addr = env::var("PROCESADOR_PAGOS_ADDR")
        .unwrap_or_else(|_| PROCESADOR_PAGOS_ADDR_DEFAULT.to_string());

    let sede = Sede {
        sede_id: sede_id,
        coords: (-34.6037, -58.3816),
        slots: slots,
        token_secret,
        pending_reservas: std::collections::HashMap::new(),
        procesador_pagos_addr,
    }
    .start();

    let addr = format!("127.0.0.1:808{}", n);
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("No se pudo iniciar la sede en {}: {}", addr, e);
            return;
        }
    };
    println!("Sede {}, puerto {}", n, addr);

    loop {
        let (socket, addr) = match listener.accept().await {
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
