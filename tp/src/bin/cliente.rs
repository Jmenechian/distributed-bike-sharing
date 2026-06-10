use actix::{Actor, Context};
use matenmutex::messages::cliente_sede::{
    AckComprobanteAlquiler, DevolverBici, MsgClienteASede, PreguntarBicis, SolicitarBici,
};
use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use matenmutex::types::BiciAlquilada;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const GREEN: &str = "\x1b[32m";
const BOLD_CYAN: &str = "\x1b[1;36m";

pub struct Cliente {
    cliente_id: u32,
    coords: (f64, f64),
    writer: Option<OwnedWriteHalf>,
    reader: Option<Lines<BufReader<OwnedReadHalf>>>,
    bici_activa: Option<BiciAlquilada>,
    dinero: u32,
}

impl Actor for Cliente {
    type Context = Context<Self>;
}

impl Cliente {
    fn new(cliente_id: u32, coords: (f64, f64), dinero: u32) -> Self {
        Cliente {
            cliente_id,
            coords,
            writer: None,
            reader: None,
            bici_activa: None,
            dinero: dinero,
        }
    }

    fn mostrar_bici(&mut self) {
        if self.bici_activa.is_some() {
            println!("[cliente] Mi bici es: {}", self.bici_activa.as_ref().unwrap().bici_id);
        } else {
            println!("[cliente] No tengo bici");
        }
    }

    fn mostrar_dinero(&mut self) {
        println!("[cliente] Mi dinero es: {}", self.dinero);
    }

    fn recargar_dinero(&mut self, monto: u32) {
        self.dinero += monto;
        println!("[cliente] Recarga de ${}. Dinero actual: ${}", monto, self.dinero);
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
        let dinero = self.dinero;
        let (writer, reader) = self.validar_conexion()?;

        let respuesta = Self::enviar_solicitar_bici(cliente_id, coords, dinero, writer, reader).await?;
        let (bici,dinero) = Self::descifrar_respuesta_de_solicitar_bici(respuesta)?; //aca ya esta bien el dinero, si confirma vamos

        let respuesta = self.enviar_ack_comprobante(bici).await?;
        self.descifrar_respuesta_de_ack_comprobante(respuesta, dinero);

        Ok(())
    }

    async fn enviar_solicitar_bici(cliente_id: u32, coords: (f64, f64), dinero: u32, writer: &mut OwnedWriteHalf, reader: &mut Lines<BufReader<OwnedReadHalf>>) -> Result<Option<String>, io::Error> {
        let msg = MsgClienteASede::SolicitarBici(SolicitarBici { cliente_id, coords, dinero});
        println!("[cliente] Envia SolicitarBici");
        let respuesta = enviar_mensaje_a_sede(writer, reader, msg).await?;
        Ok(respuesta)
    }

    async fn enviar_ack_comprobante(&mut self, bici: BiciAlquilada) -> Result<Option<String>, io::Error> {
        let alquiler_id = bici.alquiler_id().to_string();
        let (writer, reader) = self.validar_conexion()?;
        let ack = MsgClienteASede::AckComprobanteAlquiler(AckComprobanteAlquiler { alquiler_id: alquiler_id.clone() });
        println!("[cliente] Envia AckComprobanteAlquiler (alquiler {})", alquiler_id);
        let respuesta = enviar_mensaje_a_sede(writer, reader, ack).await?;
        Ok(respuesta)
    }

    fn descifrar_respuesta_de_ack_comprobante(&mut self, respuesta: Option<String>, dinero:  u32) {
        if let Some(respuesta) = respuesta {
            match serde_json::from_str::<RespuestaSolicitarBici>(&respuesta) {
                Ok(RespuestaSolicitarBici::AlquilerConfirmado(bici_confirmada)) => {
                    println!("Alquiler confirmado: bici {}", bici_confirmada.bici_id());
                    println!("Dinero actual: {} -> {}", self.dinero, dinero);
                    self.dinero = dinero; //recien aca que es el ultimo control
                    self.bici_activa = Some(bici_confirmada);
                }
                Ok(RespuestaSolicitarBici::AlquilerRechazado(motivo)) => {
                    println!("[cliente] Recibe AlquilerRechazado (motivo {})", motivo);
                }
                _ => println!("Sede: {}", respuesta),
            }
        }
    }

    fn descifrar_respuesta_de_solicitar_bici(respuesta: Option<String>) -> Result<(BiciAlquilada, u32), io::Error> {
        match respuesta.as_deref().and_then(|l| serde_json::from_str::<RespuestaSolicitarBici>(l).ok()) {
            Some(RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, dinero)) => {
                println!("[cliente] Recibe ComprobanteAlquilerEmitido (bici {}, alquiler {})", bici.bici_id(), bici.alquiler_id());
                Ok((bici,dinero))
            }
            Some(RespuestaSolicitarBici::AlquilerRechazado(motivo)) => {
                println!("[cliente] Recibe AlquilerRechazado (motivo {})", motivo);
                Err(io::Error::new(io::ErrorKind::Other, format!("Alquiler rechazado: {motivo}")))
            }
            _ => { Err(io::Error::new(io::ErrorKind::Other, "Respuesta inesperada de la sede")) }
        }
    }

    async fn devolver_bici(&mut self) -> Result<(), io::Error> {
        if self.bici_activa.is_none() {
            return Err(io::Error::new(io::ErrorKind::Other, "No hay bici activa para devolver"));
        }

        let bici = self.bici_activa.take().unwrap();
        let coords = self.coords;
        let dinero = self.dinero;
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::DevolverBici(DevolverBici { bici, coords, dinero});
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


    fn manejar_respuesta_devolver(&mut self, respuesta: RespuestaDevolverBici) {
        match respuesta {
            RespuestaDevolverBici::DevolucionOk(bici, dinero) => {
                println!("Devolucion exitosa: bici {}", bici.bici_id);
                println!("Dinero actual: {} -> {}", self.dinero, dinero);

                self.dinero = dinero;
                self.bici_activa = None;
            }
            RespuestaDevolverBici::DevolucionRechazada(motivo) => {
                println!("[cliente] Recibe DevolucionRechazada (motivo {})", motivo);
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
    let mut cliente = Cliente::new(42, (-34.6037, -58.3816), 1000);

    println!(
        "\n{BOLD_CYAN}=== Mate&Mutex CLI ==={RESET}\n\
         {BOLD}Comandos disponibles:{RESET}\n\
         {GREEN}  conectar_sede <n>{RESET}  - Conectar a una sede\n\
         {GREEN}  solicitar_bici{RESET}     - Solicitar una bici\n\
         {GREEN}  devolver_bici{RESET}      - Devolver la bici\n\
         {GREEN}  info_bicis{RESET}         - Ver bicis disponibles\n\
         {GREEN}  recarga <n>{RESET}        - Recargar dinero\n\
         {GREEN}  quit{RESET}               - Salir\n"
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
            ["bici"] => Ok(cliente.mostrar_bici()),
            ["dinero"] => Ok(cliente.mostrar_dinero()),
            ["recarga", n] => Ok(cliente.recargar_dinero(n.parse().unwrap())),
            ["quit"] => break,
            _ => {
                println!("Comando Invalido: '{}'", input);
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
    use matenmutex::common::PREMONTO;

    fn cliente_de_prueba() -> Cliente {
        Cliente::new(1, (-34.6037, -58.3816), 9999999)
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
        cliente.bici_activa = Some(bici_alquilada_de_prueba("1-1"));
        assert_eq!(cliente.bici_activa.as_ref().map(|b| b.bici_id()), Some("1-1"));
    }

    #[test]
    fn alquiler_rechazado_no_modifica_bici_activa() {
        let cliente = cliente_de_prueba();
        assert!(cliente.bici_activa.is_none());
    }

    #[test]
    fn devolucion_ok_vacia_bici_activa() {
        let mut cliente = cliente_de_prueba();
        cliente.bici_activa = Some(bici_alquilada_de_prueba("1-1"));

        cliente.manejar_respuesta_devolver(RespuestaDevolverBici::DevolucionOk(Bici {
            bici_id: "1-1".to_string(),
        }, 0));

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
