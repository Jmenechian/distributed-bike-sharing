use matenmutex::common::{calcular_distancia, DISTANCIA_MINIMA};
use matenmutex::messages::cliente_sede::{Handshake, MsgClienteASede, RespuestaHandshake};
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
        let sede_id: u32 = n.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("'{}' no es un numero de sede valido", n))
        })?;

        let sede_coords = self.sedes_posiciones.get(&sede_id)
            .ok_or_else(|| io::Error::other("sede desconocida"))?;

        if !self.estado
            && calcular_distancia(*sede_coords, self.coords) > DISTANCIA_MINIMA {
                return Err(io::Error::other("estoy offline y fuera de rango"));
            }

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
                Ok(RespuestaHandshake::Aceptado) => {
                    self.writer = Some(writer);
                    self.reader = Some(reader);
                    self.sede_conectada = sede_id;
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
