use actix::{Actor, Addr, Context, Handler, MessageResult, ResponseFuture};
use matenmutex::messages::clientesede::{
    DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::slot;
use matenmutex::slot::{ConsultarBici, Slot};
use matenmutex::types::{Bici, BiciAlquilada};
use std::env;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use matenmutex::messages::sede_cliente::RespuestaSolicitarBici;

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

        ((coords_sede_x - coords_cliente_x).powi(2) + (coords_sede_y - coords_cliente_y).powi(2)).sqrt()
    }
}

impl Actor for Sede {
    type Context = Context<Self>;
}

impl Handler<SolicitarBici> for Sede {
    type Result = MessageResult<SolicitarBici>;

    fn handle(&mut self, msg: SolicitarBici, ctx: &mut Context<Self>) -> Self::Result {
        println!("Hola Bici");
        if !self.cliente_en_rango(msg.coords) {
            return MessageResult(RespuestaSolicitarBici::AlquilerRechazado(
                String::from("Para poder tomar la bici debe estar mas cerca de la sede")
            ));
        }

        MessageResult(RespuestaSolicitarBici::AlquilerConfirmado(
            BiciAlquilada::new(
                "1".to_string(),
                "1".to_string(),
                msg.cliente_id.to_string(),
                self.sede_id,
                0,
                "1".to_string()
            )
        ))
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
