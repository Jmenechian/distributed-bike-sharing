use matenmutex::messages::cliente_sede::{Handshake, MsgClienteASede, RespuestaHandshake};
use matenmutex::messages::sede_sede::SedeInfo;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use super::Cliente;

pub(crate) async fn enviar_mensaje_a_sede(
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

impl Cliente {
    pub(crate) async fn conectar(&mut self, n: &str) -> Result<(), io::Error> {
        let addr = format!("127.0.0.1:808{}", n);
        let stream = TcpStream::connect(&addr).await?;
        let (read_half, write_half) = stream.into_split();
        let mut writer = write_half;
        let mut reader = BufReader::new(read_half).lines();

        let handshake = MsgClienteASede::Handshake(Handshake { coords: self.coords });
        let payload = serde_json::to_string(&handshake)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            + "\n";
        writer.write_all(payload.as_bytes()).await?;

        match reader.next_line().await {
            Ok(Some(line)) => match serde_json::from_str::<RespuestaHandshake>(&line) {
                Ok(RespuestaHandshake::Aceptado(coords)) => {
                    self.writer = Some(writer);
                    self.reader = Some(reader);
                    let sede_id: u32 = n.parse().unwrap_or(0);
                    self.sede_conectada = Some(SedeInfo {
                        sede_id,
                        coords,
                        ..Default::default()
                    });
                    println!("Conectado a sede {}", n);
                    Ok(())
                }
                Ok(RespuestaHandshake::Rechazado(razon)) => {
                    Err(io::Error::new(io::ErrorKind::ConnectionRefused, razon))
                }
                Err(e) => {
                    Err(io::Error::new(io::ErrorKind::InvalidData, format!("Handshake invalido: {}", e)))
                }
            },
            _ => Err(io::Error::new(io::ErrorKind::ConnectionAborted, "La sede cerro la conexion durante el handshake")),
        }
    }

    pub(crate) fn validar_conexion(
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
