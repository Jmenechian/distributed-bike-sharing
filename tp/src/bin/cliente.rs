use actix::{Actor, Context};
use matenmutex::messages::clientesede::{
    DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::types::{Bici, BiciAlquilada};
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use matenmutex::messages::sede_cliente::{RespuestaDevolucionBici, RespuestaSolicitarBici};

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

        if self.bici_activa.is_some() {
            println!("Ya tenes una bici paparulo");
            return Ok(());
        }

        let cliente_id = self.cliente_id;
        let coords = self.coords;
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::SolicitarBici(SolicitarBici { cliente_id, coords });

        let respuesta = enviar_mensaje_a_sede::<RespuestaSolicitarBici>(writer, reader, msg).await?;

        match respuesta {
            RespuestaSolicitarBici::AlquilerConfirmado(bici) => {
                self.bici_activa = Some(bici);
                println!("Alquiler confirmado, Bici: {}", self.bici_activa.as_ref().unwrap().bici_id);
            }
            RespuestaSolicitarBici::AlquilerRechazado(razon) => {
                println!("Alquiler Rechazado, {}", razon);
            }
        }

        Ok(())
    }

    async fn devolver_bici(&mut self) -> Result<(), io::Error> {

        if self.bici_activa.is_none() {
            println!("Primero conseguite una bici");
            return Ok(());
        }

        let bici = self.bici_activa.take().unwrap();
        let coords = self.coords;
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::DevolverBici(DevolverBici{bici, coords}); //que pasa si error? perdi el ownership de bici

        let respuesta = enviar_mensaje_a_sede::<RespuestaDevolucionBici>(writer, reader, msg).await?;

        match respuesta {
            RespuestaDevolucionBici::DevolucionOk {monto, duracion} => {
                println!("devolviste la bici");
                if self.bici_activa.is_some() {
                    println!("guarda");
                }
            }
            RespuestaDevolucionBici::DevolucionRechazada(razon) => {
                println!("Devolucion Rechazada, {}", razon);
            }
        }

        Ok(())

    }

    async fn info_bicis(&mut self) -> Result<(), io::Error> { //fixear (anda pero no ta mal hecho
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::PreguntarBicis(PreguntarBicis {});
        let respuesta = enviar_mensaje_a_sede::<RespuestaSolicitarBici>(writer, reader, msg).await?;

        match respuesta {
            RespuestaSolicitarBici::AlquilerConfirmado(bici) => {
                self.bici_activa = Some(bici);
                println!("Alquiler confirmado, Bici: {}", self.bici_activa.as_ref().unwrap().bici_id);
            }
            RespuestaSolicitarBici::AlquilerRechazado(razon) => {
                println!("Alquiler Rechazado, {}", razon);
            }
        }
        Ok(())
    }

    fn mostrar_bici(&mut self) {
        if self.bici_activa.is_some() {
            println!("Bici actual: {}", self.bici_activa.as_ref().unwrap().bici_id);
        } else {
            println!("No Bici activa");
        }
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

// async fn enviar_mensaje_a_sede(
//     writer: &mut OwnedWriteHalf,
//     reader: &mut Lines<BufReader<OwnedReadHalf>>,
//     msg: MsgClienteASede,
// ) -> Result<RespuestaSolicitarBici, io::Error> {
//     let payload = serde_json::to_string(&msg).unwrap() + "\n";
//     writer.write_all(payload.as_bytes()).await?;
//
//     let response = reader.next_line().await?
//         .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Rompio algo"))?;
//
//     serde_json::from_str(&response).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
// }

async fn enviar_mensaje_a_sede<T>( //mas generico
    writer: &mut OwnedWriteHalf,
    reader: &mut Lines<BufReader<OwnedReadHalf>>,
    msg: MsgClienteASede,
) -> Result<T, io::Error>
where
    T: serde::de::DeserializeOwned,
{
    let payload = serde_json::to_string(&msg).unwrap() + "\n";
    writer.write_all(payload.as_bytes()).await?;
    let response = reader.next_line().await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Rompio algo"))?;
    serde_json::from_str(&response)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[actix_rt::main]
async fn main() {
    let mut cliente = Cliente::new(42, (-34.6037, -58.3816));

    println!(
        "Comandos: 'conectar_sede <n>' | 'solicitar_bici' | 'devolver_bici <bici_id> <x> <y>' | 'info_bicis' | 'mi_bici' | 'quit'"
    );

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let input = line.trim().to_string();
        let parts: Vec<&str> = input.split_whitespace().collect();

        let result = match parts.as_slice() {
            ["conectar_sede", n] => cliente.conectar(n).await,
            ["mi_bici"] => Ok(cliente.mostrar_bici()),
            ["solicitar_bici"] => cliente.solicitar_bici().await,
            ["devolver_bici"] => cliente.devolver_bici().await,
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
