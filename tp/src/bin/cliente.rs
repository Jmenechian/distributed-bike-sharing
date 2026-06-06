use actix::{Actor, Context};
use matenmutex::messages::clientesede::{
    DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::types::{Bici, BiciAlquilada};
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub struct Cliente {
    cliente_id: u32,
    coords: (f64, f64),
    writer: Option<OwnedWriteHalf>,
    reader: Option<Lines<BufReader<OwnedReadHalf>>>,
    bici_activa: Option<BiciAlquilada>,
}

impl Actor for Cliente {
    type Context = Context<Self>;
}

impl Cliente {
    fn new(cliente_id: u32, coords: (f64, f64)) -> Self {
        Cliente {
            cliente_id,
            coords,
            writer: None,
            reader: None,
            bici_activa: None,
        }
    }

    async fn conectar(&mut self, n: &str) -> Result<(), io::Error> {
        let addr = format!("127.0.0.1:808{}", n);
        let stream = TcpStream::connect(&addr).await?;
        let (read_half, write_half) = stream.into_split();
        self.writer = Some(write_half);
        self.reader = Some(BufReader::new(read_half).lines());
        println!("Conectado a sede {}", n);
        Ok(())
    }

    async fn solicitar_bici(&mut self) -> Result<(), io::Error> {
        let cliente_id = self.cliente_id;
        let coords = self.coords;
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::SolicitarBici(SolicitarBici { cliente_id, coords });
        enviar_mensaje_a_sede(writer, reader, msg).await
    }

    async fn devolver_bici(&mut self, bici_id: &str, x: f64, y: f64) -> Result<(), io::Error> {
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::DevolverBici(DevolverBici {
            bici: Bici {
                bici_id: bici_id.to_string(),
            },
            coords: (x, y),
        });
        enviar_mensaje_a_sede(writer, reader, msg).await
    }

    async fn info_bicis(&mut self) -> Result<(), io::Error> {
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::PreguntarBicis(PreguntarBicis {});
        enviar_mensaje_a_sede(writer, reader, msg).await
    }

    fn validar_conexion(
        &mut self,
    ) -> Result<(&mut OwnedWriteHalf, &mut Lines<BufReader<OwnedReadHalf>>), io::Error> {
        match (&mut self.writer, &mut self.reader) {
            (Some(w), Some(r)) => Ok((w, r)),
            _ => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "No hay conexion activa. Conectarse con: conectar_sede <n>",
            )),
        }
    }
}

async fn enviar_mensaje_a_sede(
    writer: &mut OwnedWriteHalf,
    reader: &mut Lines<BufReader<OwnedReadHalf>>,
    msg: MsgClienteASede,
) -> Result<(), io::Error> {
    let payload = serde_json::to_string(&msg).unwrap() + "\n";
    writer.write_all(payload.as_bytes()).await?;
    if let Ok(Some(response)) = reader.next_line().await {
        println!("Sede: {}", response); // SolicitarBici Devuelve un JSON, habria que handlearlo para que quede mejor
    }
    Ok(())
}

#[actix_rt::main]
async fn main() {
    let mut cliente = Cliente::new(42, (-34.6037, -58.3816));

    println!(
        "Comandos: 'conectar_sede <n>' | 'solicitar_bici' | 'devolver_bici <bici_id> <x> <y>' | 'info_bicis' | 'quit'"
    );

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let input = line.trim().to_string();
        let parts: Vec<&str> = input.split_whitespace().collect();

        let result = match parts.as_slice() {
            ["conectar_sede", n] => cliente.conectar(n).await,
            ["solicitar_bici"] => cliente.solicitar_bici().await,
            ["devolver_bici", bici_id, x, y] => match (x.parse::<f64>(), y.parse::<f64>()) {
                (Ok(x), Ok(y)) => cliente.devolver_bici(bici_id, x, y).await,
                _ => {
                    println!("Coordenadas inválidas");
                    continue;
                }
            },
            ["info_bicis"] => cliente.info_bicis().await,
            ["quit"] => {
                println!("Bye!");
                break;
            }
            _ => {
                println!("Unknown command: '{}'", input);
                continue;
            }
        };

        if let Err(e) = result {
            println!("Error: {}", e);
        }
    }
}
