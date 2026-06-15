use actix::{Actor, Context};
use matenmutex::common::{calcular_distancia, DISTANCIA_MINIMA};
use matenmutex::messages::sede_sede::SedeInfo;
use matenmutex::types::BiciAlquilada;
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io;
use std::io::BufReader as StdBufReader;
use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

mod operaciones;
mod persistencia;
mod red_cliente;

use persistencia::{cargar_estado_cliente, estado_path_de_id};

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

pub(crate) struct Cliente {
    cliente_id: u32,
    coords: (f64, f64),
    writer: Option<OwnedWriteHalf>,
    reader: Option<Lines<BufReader<OwnedReadHalf>>>,
    bici_activa: Option<BiciAlquilada>,
    dinero: f64,
    sedes_conocidas: Vec<SedeInfo>,
    estado: bool,
    sede_conectada: Option<SedeInfo>,
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
            estado: true,
            sede_conectada: None,
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
        self.persistir_estado();

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
        if !self.estado {
            return Err(io::Error::new(io::ErrorKind::Other, "[error] estoy offline"));
        }

        let monto: f64 = monto.parse()
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput,format!("'{}' no es un numero valido", monto),)
            })?;

        self.dinero += monto;

        println!("[cliente] Recarga de ${:.2}. Dinero actual: ${:.2}", monto, self.dinero);
        self.persistir_estado();

        Ok(())
    }

    fn esta_en_rango(&self) -> bool {
        match &self.sede_conectada {
            Some(sede) => calcular_distancia(sede.coords, self.coords) <= DISTANCIA_MINIMA,
            None => true,
        }
    }

    fn online(&mut self) {
        self.estado = true;
        println!("[cliente] Estoy online");
    }

    fn offline(&mut self) {
        self.estado = false;
        println!("[cliente] Estoy offline");
    }
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

    let estado_path = estado_path_de_id(cliente.cliente_id);
    if let Some(estado) = cargar_estado_cliente(&estado_path) {
        cliente.coords = estado.coords;
        if let Some(bici) = estado.bici_activa {
            cliente.bici_activa = Some(bici);
        }
        cliente.dinero = estado.dinero;
        cliente.sedes_conocidas = estado.sedes_conocidas;
    }

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
            ["online"] => Ok(cliente.online()),
            ["offline"] => Ok(cliente.offline()),
            ["ver_dinero"] => Ok(cliente.mostrar_dinero()),
            ["recarga", n] => cliente.recargar_dinero(n),
            ["pedir_sedes"] => cliente.pedir_sedes().await,
            ["ver_comandos"] => Ok(listar_comandos()),
            ["clear"] => Ok({
                limpiar_pantalla();
                listar_comandos();
            }),
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
         {GREEN}  online{RESET}                         - Cambiar a modo online\n\
         {GREEN}  offline{RESET}                        - Cambiar a modo offline\n\
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

fn limpiar_pantalla() {
    print!("\x1b[2J\x1b[H");
    let _ = io::stdout().flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cliente_de_prueba() -> Cliente {
        Cliente::new(1, (-34.6037, -58.3816), 9999999.0)
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
