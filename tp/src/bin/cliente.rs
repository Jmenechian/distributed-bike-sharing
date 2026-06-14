use actix::{Actor, Context};
use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, DevolverBici, MsgClienteASede, PedirSedes, PreguntarBicis, SolicitarBici};
use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use matenmutex::types::BiciAlquilada;
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io;
use std::io::BufReader as StdBufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use matenmutex::common::calcular_distancia;
use matenmutex::messages::sede_sede::SedeInfo;
use std::io::Write;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const GREEN: &str = "\x1b[32m";
const BOLD_CYAN: &str = "\x1b[1;36m";
const CONFIG_FILE: usize = 1;

#[derive(Deserialize)]
struct ClienteConfig {
    cliente_id: u32,
    coord_x: f64,
    coord_y: f64,
    dinero: f64,
}

pub struct Cliente {
    cliente_id: u32,
    coords: (f64, f64),
    writer: Option<OwnedWriteHalf>,
    reader: Option<Lines<BufReader<OwnedReadHalf>>>,
    bici_activa: Option<BiciAlquilada>,
    dinero: f64,
    sedes_conocidas: Vec<SedeInfo>,
}

impl Actor for Cliente {
    type Context = Context<Self>;
}

impl Cliente {
    fn new(cliente_id: u32, coords: (f64, f64), dinero: f64) -> Self {
        Cliente {
            cliente_id,
            coords,
            writer: None,
            reader: None,
            bici_activa: None,
            dinero,
            sedes_conocidas: vec![],
        }
    }

    fn from_json(file_path: &str) -> Result<Cliente, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let config: ClienteConfig = serde_json::from_reader(StdBufReader::new(file))?;
        Ok(Cliente::new(config.cliente_id, (config.coord_x, config.coord_y), config.dinero))
    }

    fn mover(&mut self, str_x: &str, str_y: &str) -> Result<(), io::Error> {
        let x = str_x.parse::<f64>().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("Coordenada x invalida: '{str_x}' debe representar un número.")))?;
        let y = str_y.parse::<f64>().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("Coordenada y invalida: '{str_y}' debe representar un número.")))?;
        self.coords = (x, y);
        Ok(())
    }

    fn mostrar_bici(&mut self) {
        if let Some(bici) = &self.bici_activa {
            println!("[cliente] Mi bici es: {}", bici.bici_id);
        } else {
            println!("[cliente] No tengo bici");
        }
    }

    fn mostrar_dinero(&mut self) {
        println!("[cliente] Mi dinero es: ${:.2}", self.dinero);
    }

    fn recargar_dinero(&mut self, monto: &str) -> Result<(), io::Error> {
        let monto: f64 = monto.parse()
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput,format!("'{}' no es un numero valido", monto),)
            })?;

        self.dinero += monto;

        println!("[cliente] Recarga de ${:.2}. Dinero actual: ${:.2}", monto, self.dinero);

        Ok(())
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

    fn print_sedes(sedes: &[SedeInfo], coords: (f64, f64)) {
        let mut ordenadas: Vec<&SedeInfo> = sedes.iter().collect();
        ordenadas.sort_by(|a, b| {
            calcular_distancia(a.coords, coords)
                .total_cmp(&calcular_distancia(b.coords, coords))
        });
        for sede in ordenadas {
            let dist = calcular_distancia(sede.coords, coords);
            let estado = if sede.conectada { "online" } else { "offline" };
            println!(
                "Sede {} | coords: ({:.4}, {:.4}) | estado: {} | distancia: {:.2}m | slots disponibles: {} | slots vacios {}",
                sede.sede_id, sede.coords.0, sede.coords.1, estado, dist, sede.slots_disponibles, sede.slots_vacios
            )
        }
    }
    async fn pedir_sedes(&mut self) -> Result<(), io::Error> {
        let coords = self.coords;
        let (writer, reader) = self.validar_conexion()?;
        let respuesta = Self::enviar_info_sedes_cercanas(coords, writer, reader).await?;

        if let Some(json) = respuesta {
            match serde_json::from_str::<Vec<SedeInfo>>(&json) {
                Ok(sedes) => {
                    self.sedes_conocidas = sedes;
                }
                Err(e) => {
                    eprintln!("[error] fallo al deserializar sedes: {} | json: {}", e, json);
                }
            }
        } else {
            eprintln!("[warn] respuesta vacia, no se recibieron sedes");
        }

        Cliente::print_sedes(&self.sedes_conocidas, coords);
        Ok(())
    }

    async fn solicitar_bici(&mut self) -> Result<(), io::Error> {
        if self.bici_activa.is_some() {
            println!("[cliente] Ya tenes una bici");
            return Ok(());
        }

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

    async fn enviar_solicitar_bici(cliente_id: u32, coords: (f64, f64), dinero: f64, writer: &mut OwnedWriteHalf, reader: &mut Lines<BufReader<OwnedReadHalf>>) -> Result<Option<String>, io::Error> {
        let msg = MsgClienteASede::SolicitarBici(SolicitarBici { cliente_id, coords, dinero});
        let respuesta = enviar_mensaje_a_sede(writer, reader, msg).await?;
        Ok(respuesta)
    }

    async fn enviar_info_sedes_cercanas(coords: (f64, f64), writer: &mut OwnedWriteHalf, reader: &mut Lines<BufReader<OwnedReadHalf>>) -> Result<Option<String>, io::Error> {
        let msg = MsgClienteASede::PedirSedes(PedirSedes {coords});
        let respuesta = enviar_mensaje_a_sede(writer, reader, msg).await?;
        Ok(respuesta)
    }

    async fn enviar_ack_comprobante(&mut self, bici: BiciAlquilada) -> Result<Option<String>, io::Error> {
        let alquiler_id = bici.alquiler_id().to_string();
        let (writer, reader) = self.validar_conexion()?;
        let ack = MsgClienteASede::AckComprobanteAlquiler(AckComprobanteAlquiler { alquiler_id: alquiler_id.clone() });
        let respuesta = enviar_mensaje_a_sede(writer, reader, ack).await?;
        Ok(respuesta)
    }

    fn descifrar_respuesta_de_ack_comprobante(&mut self, respuesta: Option<String>, dinero: f64) {
        if let Some(respuesta) = respuesta {
            match serde_json::from_str::<RespuestaSolicitarBici>(&respuesta) {
                Ok(RespuestaSolicitarBici::AlquilerConfirmado(bici_confirmada)) => {
                    println!("Alquiler confirmado: bici {}", bici_confirmada.bici_id());
                    println!("Dinero actual: ${:.2} -> ${:.2}", self.dinero, dinero);
                    self.dinero = dinero; //recien aca que es el ultimo control
                    self.bici_activa = Some(bici_confirmada);
                }
                Ok(RespuestaSolicitarBici::AlquilerRechazado(motivo)) => {
                    println!("Alquiler rechazado: {}", motivo);
                }
                _ => println!("Sede: {}", respuesta),
            }
        }
    }

    fn descifrar_respuesta_de_solicitar_bici(respuesta: Option<String>) -> Result<(BiciAlquilada, f64), io::Error> {
        match respuesta.as_deref().and_then(|l| serde_json::from_str::<RespuestaSolicitarBici>(l).ok()) {
            Some(RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, dinero)) => {
                Ok((bici,dinero))
            }
            Some(RespuestaSolicitarBici::AlquilerRechazado(motivo)) => {
                Err(io::Error::new(io::ErrorKind::Other, format!("Alquiler rechazado: {motivo}")))
            }
            _ => { Err(io::Error::new(io::ErrorKind::Other, "Respuesta inesperada de la sede")) }
        }
    }

    async fn devolver_bici(&mut self) -> Result<(), io::Error> {
        if self.bici_activa.is_none() {
            return Err(io::Error::new(io::ErrorKind::Other, "No hay bici activa para devolver"));
        }

        let bici = self.bici_activa.clone().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "No hay bici activa para devolver")
        })?;
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
            match serde_json::from_str::<String>(&linea) {
                Ok(texto) => println!("{}", texto),
                Err(_) => println!("{}", linea),
            }
        }
        Ok(())
    }


    fn manejar_respuesta_devolver(&mut self, respuesta: RespuestaDevolverBici) {
        match respuesta {
            RespuestaDevolverBici::DevolucionOk(bici, dinero) => {
                println!("Devolucion exitosa: bici {}", bici.bici_id);
                println!("Dinero actual: ${:.2} -> ${:.2}", self.dinero, dinero);

                self.dinero = dinero;
                self.bici_activa = None;
            }
            RespuestaDevolverBici::DevolucionRechazada(motivo) => {
                println!("Devolucion rechazada: {}", motivo);
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
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Uso: cargo run --bin cliente <path/a/config.json>");
        return;
    }
    let mut cliente = match Cliente::from_json(&args[CONFIG_FILE]) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error al cargar la config: {e}");
            return;
        }
    };

    listar_comandos();

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let input = line.trim().to_string();
        let parts: Vec<&str> = input.split_whitespace().collect();

        let result = match parts.as_slice() {
            ["mover", x, y] => cliente.mover(x, y),
            ["conectar_sede", n] => cliente.conectar(n).await,
            ["solicitar_bici"] => cliente.solicitar_bici().await,
            ["devolver_bici"] => cliente.devolver_bici().await,
            ["info_bicis"] => cliente.info_bicis().await,
            ["bici"] => Ok(cliente.mostrar_bici()),
            ["ver_dinero"] => Ok(cliente.mostrar_dinero()),
            ["recarga", n] => cliente.recargar_dinero(n),
            ["pedir_sedes"] => cliente.pedir_sedes().await,
            ["ver_comandos"] => Ok(listar_comandos()),
            ["clear"] => Ok(limpiar_pantalla()),
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

fn listar_comandos() {
    println!(
        "\n{BOLD_CYAN}=== Mate&Mutex CLI ==={RESET}\n\
         {BOLD}Comandos disponibles:{RESET}\n\
         {GREEN}  mover <x> <y>{RESET}                  - Moverse una distancia\n\
         {GREEN}  conectar_sede <n>{RESET}              - Conectar a una sede\n\
         {GREEN}  solicitar_bici{RESET}                 - Solicitar una bici\n\
         {GREEN}  devolver_bici{RESET}                  - Devolver la bici\n\
         {GREEN}  info_bicis{RESET}                     - Ver bicis disponibles\n\
         {GREEN}  recarga <n>{RESET}                    - Recargar dinero\n\
         {GREEN}  ver_dinero{RESET}                     - Ver el dinero actual\n\
         {GREEN}  pedir_sedes{RESET}                    - Pide Sede Cercanas\n\
         {GREEN}  ver_comandos{RESET}                  - Ver esta lista de comandos\n\
         {GREEN}  clear{RESET}                          - Limpiar la pantalla\n\
         {GREEN}  quit{RESET}                           - Salir\n"
    );
}

/// Limpia la pantalla de la terminal (como el comando `clear`).
fn limpiar_pantalla() {
    print!("\x1b[2J\x1b[H");
    let _ = io::stdout().flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use matenmutex::types::Bici;

    fn cliente_de_prueba() -> Cliente {
        Cliente::new(1, (-34.6037, -58.3816), 9999999.0)
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
        }, 0.0));

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

    #[test]
    fn mover_del_cliente_es_valido() {
        let mut cliente = cliente_de_prueba();
        let expected_x = 40.0;
        let expected_y = 0.0;
        assert!(cliente.mover(&expected_x.to_string(), &expected_y.to_string()).is_ok());
        assert_eq!(cliente.coords.0, expected_x);
        assert_eq!(cliente.coords.1, expected_y);
    }

    #[test]
    fn mover_con_valores_invalidos() {
        let mut cliente = cliente_de_prueba();
        let invalid_x = "test";
        let invalid_y = "test";

        assert!(cliente.mover(&invalid_x, &invalid_y).is_err());
    }
}
