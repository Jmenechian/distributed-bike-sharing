use actix::{Actor, Context};
use matenmutex::messages::cliente_sede::{
    DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use matenmutex::types::BiciAlquilada;
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
        let linea = enviar_mensaje_a_sede(writer, reader, msg).await?;

        if let Some(linea) = linea {
            match serde_json::from_str::<RespuestaSolicitarBici>(&linea) {
                Ok(respuesta) => self.manejar_respuesta_solicitar(respuesta),
                Err(_) => println!("Sede: {}", linea),
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
        let msg = MsgClienteASede::DevolverBici(DevolverBici { bici, coords });
        let linea = enviar_mensaje_a_sede(writer, reader, msg).await?;

        if let Some(linea) = linea {
            match serde_json::from_str::<RespuestaDevolverBici>(&linea) {
                Ok(respuesta) => self.manejar_respuesta_devolver(respuesta),
                Err(_) => println!("Sede: {}", linea),
            }
        }

        Ok(())
    }

    async fn info_bicis(&mut self) -> Result<(), io::Error> {
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::PreguntarBicis(PreguntarBicis {});
        if let Some(linea) = enviar_mensaje_a_sede(writer, reader, msg).await? {
            println!("Sede: {}", linea);
        }
        Ok(())
    }

    fn manejar_respuesta_solicitar(&mut self, respuesta: RespuestaSolicitarBici) {
        match respuesta {
            RespuestaSolicitarBici::AlquilerConfirmado(bici_alquilada) => {
                println!("Alquiler confirmado: bici {}", bici_alquilada.bici_id());
                self.bici_activa = Some(bici_alquilada);
            }
            RespuestaSolicitarBici::AlquilerRechazado(motivo) => {
                println!("Alquiler rechazado: {motivo}");
            }
        }
    }

    fn manejar_respuesta_devolver(&mut self, respuesta: RespuestaDevolverBici) {
        match respuesta {
            RespuestaDevolverBici::DevolucionOk(bici) => {
                println!("Devolucion exitosa: bici {}", bici.bici_id);
                self.bici_activa = None;
            }
            RespuestaDevolverBici::DevolucionRechazada(motivo) => {
                println!("Devolucion rechazada: {motivo}");
            }
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

async fn enviar_mensaje_a_sede(
    writer: &mut OwnedWriteHalf,
    reader: &mut Lines<BufReader<OwnedReadHalf>>,
    msg: MsgClienteASede,
) -> Result<Option<String>, io::Error> {
    let payload = match serde_json::to_string(&msg) {
        Ok(json) => json + "\n",
        Err(e) => {
            eprintln!("Error serializando mensaje para la sede: {}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };
    writer.write_all(payload.as_bytes()).await?;
    reader.next_line().await
}

#[actix_rt::main]
async fn main() {
    let mut cliente = Cliente::new(42, (-34.6037, -58.3816));

    println!(
        "Comandos: 'conectar_sede <n>' | 'solicitar_bici' | 'devolver_bici' | 'info_bicis' | 'quit'"
    );

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let input = line.trim().to_string();
        let parts: Vec<&str> = input.split_whitespace().collect();

        let result = match parts.as_slice() {
            ["conectar_sede", n] => cliente.conectar(n).await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use matenmutex::types::Bici;

    fn cliente_de_prueba() -> Cliente {
        Cliente::new(1, (-34.6037, -58.3816))
    }

    fn bici_alquilada_de_prueba(bici_id: &str) -> BiciAlquilada {
        BiciAlquilada::new(
            bici_id.to_string(),
            "alq-1".to_string(),
            "1".to_string(),
            1,
            1000,
            "tok-1".to_string(),
        )
    }

    #[test]
    fn alquiler_confirmado_actualiza_bici_activa() {
        let mut cliente = cliente_de_prueba();

        cliente.manejar_respuesta_solicitar(RespuestaSolicitarBici::AlquilerConfirmado(
            bici_alquilada_de_prueba("1-1"),
        ));

        assert_eq!(
            cliente.bici_activa.as_ref().map(|b| b.bici_id()),
            Some("1-1")
        );
    }

    #[test]
    fn alquiler_rechazado_no_modifica_bici_activa() {
        let mut cliente = cliente_de_prueba();

        cliente.manejar_respuesta_solicitar(RespuestaSolicitarBici::AlquilerRechazado(
            "fuera de rango".to_string(),
        ));

        assert!(cliente.bici_activa.is_none());
    }

    #[test]
    fn devolucion_ok_vacia_bici_activa() {
        let mut cliente = cliente_de_prueba();
        cliente.bici_activa = Some(bici_alquilada_de_prueba("1-1"));

        cliente.manejar_respuesta_devolver(RespuestaDevolverBici::DevolucionOk(Bici {
            bici_id: "1-1".to_string(),
        }));

        assert!(cliente.bici_activa.is_none());
    }

    #[test]
    fn devolucion_rechazada_no_modifica_bici_activa() {
        let mut cliente = cliente_de_prueba();
        cliente.bici_activa = Some(bici_alquilada_de_prueba("1-1"));

        cliente.manejar_respuesta_devolver(RespuestaDevolverBici::DevolucionRechazada(
            "no hay lugar".to_string(),
        ));

        assert_eq!(
            cliente.bici_activa.as_ref().map(|b| b.bici_id()),
            Some("1-1")
        );
    }
}
