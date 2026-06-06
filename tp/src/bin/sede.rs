use actix::prelude::*;
use actix::{Actor, Addr, Context, Handler, MessageResult, ResponseFuture};
use matenmutex::messages::clientesede::{
    DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::messages::sede_cliente::RespuestaSolicitarBici;
use matenmutex::slot;
use matenmutex::slot::{ConsultarBici, ReservarBici, Slot, TomarBici};
use matenmutex::types::{Bici, BiciAlquilada};
use std::env;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const DISTANCIA_MINIMA: f64 = 2.0;

pub struct Sede {
    sede_id: u32,
    coords: (f64, f64),
    slots: Vec<Addr<Slot>>,
}

impl Sede {
    fn cliente_en_rango(&self, coords: (f64, f64)) -> bool {
        self.calcular_distancia(coords) <= DISTANCIA_MINIMA
    }

    fn calcular_distancia(&self, coords: (f64, f64)) -> f64 {
        let (coords_sede_x, coords_sede_y) = self.coords;
        let (coords_cliente_x, coords_cliente_y) = coords;

        ((coords_sede_x - coords_cliente_x).powi(2) + (coords_sede_y - coords_cliente_y).powi(2))
            .sqrt()
    }
}

impl Actor for Sede {
    type Context = Context<Self>;
}

impl Handler<SolicitarBici> for Sede {
    type Result = ResponseActFuture<Self, RespuestaSolicitarBici>; //futures actor porque tenes que usar info de self

    fn handle(&mut self, msg: SolicitarBici, _ctx: &mut Context<Self>) -> Self::Result {
        if !self.cliente_en_rango(msg.coords) {
            return Box::pin(
                async {
                    //wrapper future
                    RespuestaSolicitarBici::AlquilerRechazado(
                        "Para poder tomar la bici debe estar mas cerca de la sede".to_string(),
                    )
                }
                .into_actor(self),
            ); //future a actorfuture (responseActFuture)
        }

        let mut consultas = Vec::new();
        for slot in self.slots.iter() {
            consultas.push(slot.send(ConsultarBici))
        }

        let slots_refs: Vec<Addr<Slot>> = self.slots.clone();
        let cliente_id = msg.cliente_id.clone();
        let sede_id = self.sede_id;

        Box::pin(
            async move {
                let resultados = futures::future::join_all(consultas).await;

                let mut slot_disponible = None;
                for (addr, res) in slots_refs.iter().zip(resultados.iter()) {
                    if matches!(res, Ok(estado) if estado.contains("disponible")) {
                        slot_disponible = Some(addr.clone());
                        break;
                    }
                }
                slot_disponible
            }
            .into_actor(self)
            .then(move |slot_opt, actor, _ctx| {
                // move toma ownership (para cliente_id y sede_id)
                async move {
                    match slot_opt {
                        None => RespuestaSolicitarBici::AlquilerRechazado(
                            // no hay bicis disponibles
                            "No hay bicis disponibles".to_string(),
                        ),
                        Some(slot) => {
                            // 2. Reservar el slot
                            let reserva = slot.send(ReservarBici).await;

                            match reserva {
                                Ok(Ok(())) => {
                                    let toma = slot.send(TomarBici).await;
                                    match toma {
                                        Ok(Ok(bici_id)) => {
                                            RespuestaSolicitarBici::AlquilerConfirmado(
                                                BiciAlquilada::new(
                                                    bici_id,
                                                    "1".to_string(),
                                                    cliente_id.to_string(),
                                                    sede_id,
                                                    0,
                                                    "1".to_string(),
                                                ),
                                            )
                                        }
                                        _ => RespuestaSolicitarBici::AlquilerRechazado( //vemos si quedan o se van
                                            "Error al tomar la bici".to_string(),
                                        ),
                                    }
                                }
                                _ => RespuestaSolicitarBici::AlquilerRechazado(
                                    //race condition?
                                    "No se pudo reservar el slot".to_string(),
                                ),
                            }
                        }
                    }
                }
                .into_actor(actor)
            }),
        )
    }
}

impl Handler<DevolverBici> for Sede {
    type Result = Option<Bici>;

    fn handle(&mut self, _msg: DevolverBici, _ctx: &mut Context<Self>) -> Self::Result {
        println!("Chau Bici");

        Some(Bici {
            bici_id: "Hola".parse().unwrap(),
        })
    }
}

impl Handler<PreguntarBicis> for Sede {
    type Result = ResponseFuture<String>;

    fn handle(&mut self, _msg: PreguntarBicis, _ctx: &mut Context<Self>) -> Self::Result {
        let futs: Vec<_> = self
            .slots
            .iter()
            .map(|slot| slot.send(ConsultarBici)) // mensaje a cada slot
            .collect();

        Box::pin(async move {
            let resultados = futures::future::join_all(futs).await;
            let lineas: Vec<String> = resultados.into_iter().filter_map(|r| r.ok()).collect();

            let ocupados = lineas.iter().filter(|l| !l.contains("sin bici")).count();
            let total = lineas.len();

            let mut respuesta = lineas.join("   ");
            respuesta.push_str(&format!("--- {}/{} slots ocupados ---", ocupados, total));
            respuesta
        })
    }
}

#[actix_rt::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let n: u16 = args[1].parse().unwrap(); //acordarse todos los unwraps

    let sede_id = n as u32;
    let num_slots = 20;
    let slots: Vec<Addr<Slot>> = slot::crear_slots(sede_id, num_slots);

    let sede = Sede {
        sede_id: sede_id,
        coords: (-34.6037, -58.3816),
        slots: slots,
    }
    .start();

    let addr = format!("127.0.0.1:808{}", n);
    let listener = TcpListener::bind(&addr).await.unwrap();
    println!("Sede {}, puerto {}", n, addr);

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("New connection from {}", addr);
        let sede = sede.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let response = match serde_json::from_str::<MsgClienteASede>(line.trim()) {
                    Ok(MsgClienteASede::SolicitarBici(msg)) => {
                        let result = sede.send(msg).await.unwrap();
                        serde_json::to_string(&result).unwrap() + "\n"
                    }
                    Ok(MsgClienteASede::DevolverBici(msg)) => {
                        let result = sede.send(msg).await.unwrap();
                        serde_json::to_string(&result).unwrap() + "\n"
                    }
                    Ok(MsgClienteASede::PreguntarBicis(msg)) => {
                        let result = sede.send(msg).await.unwrap();
                        serde_json::to_string(&result).unwrap() + "\n"
                    }
                    Err(_) => format!("UNKNOWN:{}\n", line.trim()),
                };
                writer.write_all(response.as_bytes()).await.unwrap();
            }
            println!("Connection from {} closed", addr);
        });
    }
}
