use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use matenmutex::messages::sede_procesador::MsgSedeAProcesadorPagos;
use matenmutex::types::{PagoProcesado, TipoOperacionPago, TransaccionPago2PC};
use matenmutex::utils::tiempos::{Tiempos, TiemposBuilder};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader as TokioBufReader};
use tokio::net::{TcpListener, UdpSocket};

mod bully;
mod dos_pc;
mod pagos;
mod persistencia;
#[cfg(test)]
mod test_helpers;

use bully::{InfoNodo, RolActual};
use dos_pc::addr_2pc_de_id;
use persistencia::{cargar_estado, estado_path_de_id};

const CONFIG_FILE: usize = 1;
const MINIMAL_LENGTH: usize = 2;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const GREEN: &str = "\x1b[32m";
const BOLD_CYAN: &str = "\x1b[1;36m";

pub(crate) const PUERTO_BASE: u16 = 6000;
pub(crate) const PUERTO_BASE_2PC: u16 = PUERTO_BASE + 1000;

pub(crate) fn id_de_addr(addr: &SocketAddr) -> u32 {
    (addr.port() - PUERTO_BASE) as u32
}

pub(crate) fn addr_de_id(id: u32, ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, PUERTO_BASE + id as u16)
}

#[derive(Deserialize)]
enum RolConfig {
    Lider {
        replicas: Vec<SocketAddr>,
    },
    Replica {
        peers: Vec<SocketAddr>,
        lider_addr: SocketAddr,
    },
}

#[derive(Deserialize)]
struct ProcesadorPagosConfig {
    procesador_id: u32,
    public_addr: SocketAddr,
    private_addr: SocketAddr,
    rol: RolConfig,
    #[serde(default)]
    rechazar_si_monto_supera: Option<f64>,
}

struct ProcesadorPagos {
    procesador_id: u32,
    ip: IpAddr,
    public_listener: TcpListener,
    bus_2pc: TcpListener,
    socket: UdpSocket,
    peers: Vec<SocketAddr>,
    liveness: Mutex<HashMap<SocketAddr, InfoNodo>>,
    rol_actual: Mutex<RolActual>,
    recibio_ok: Mutex<bool>,
    tiempos: Tiempos,
    pagos_procesados: Mutex<Vec<PagoProcesado>>,
    transacciones_en_curso: Mutex<Vec<TransaccionPago2PC>>,
    preparados_replica: Mutex<HashMap<String, MsgSedeAProcesadorPagos>>,
    rechazar_si_monto_supera: Option<f64>,
    estado_path: String,
}

impl ProcesadorPagos {
    async fn from_json(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let config: ProcesadorPagosConfig = serde_json::from_reader(BufReader::new(file))?;

        let public_listener = TcpListener::bind(config.public_addr).await?;
        let bus_2pc = TcpListener::bind(addr_2pc_de_id(config.procesador_id, config.private_addr.ip())).await?;
        let socket = UdpSocket::bind(config.private_addr).await?;

        let (peers, rol_actual) = match config.rol {
            RolConfig::Lider { replicas } => (replicas, RolActual::Lider),
            RolConfig::Replica { peers, lider_addr } => {
                (peers, RolActual::Replica { lider_id: id_de_addr(&lider_addr) })
            }
        };

        let estado_path = estado_path_de_id(config.procesador_id);
        let estado = cargar_estado(&estado_path)?;

        Ok(Self {
            procesador_id: config.procesador_id,
            ip: config.private_addr.ip(),
            public_listener,
            bus_2pc,
            socket,
            peers,
            liveness: Mutex::new(HashMap::new()),
            rol_actual: Mutex::new(rol_actual),
            recibio_ok: Mutex::new(false),
            tiempos: TiemposBuilder::new().build()?,
            pagos_procesados: Mutex::new(estado.pagos_procesados),
            transacciones_en_curso: Mutex::new(estado.transacciones_en_curso),
            preparados_replica: Mutex::new(estado.preparados_replica),
            rechazar_si_monto_supera: config.rechazar_si_monto_supera,
            estado_path,
        })
    }

    async fn init(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        matenmutex::utils::logger::init(&format!("procesador{}", self.procesador_id));
        tokio::spawn(self.clone().heartbeat_loop());
        tokio::spawn(self.clone().monitor_loop());
        tokio::spawn(self.clone().accept_loop_sedes());
        tokio::spawn(self.clone().accept_loop_2pc());
        tokio::spawn(self.clone().cli_loop());
        self.recv_loop().await
    }

    async fn cli_loop(self: Arc<Self>) {
        listar_comandos_procesador(self.procesador_id);
        let stdin = tokio::io::stdin();
        let mut lines = TokioBufReader::new(stdin).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            match line.trim() {
                "info" => self.imprimir_info(),
                "ver_comandos" => listar_comandos_procesador(self.procesador_id),
                "clear" => {
                    limpiar_pantalla();
                    listar_comandos_procesador(self.procesador_id);
                }
                "" => {}
                otro => println!("Comando invalido: '{}' (info | ver_comandos | clear)", otro),
            }
        }
    }

    fn imprimir_info(&self) {
        let (rol, lider) = match self.rol() {
            Some(RolActual::Lider) => ("LIDER".to_string(), None),
            Some(RolActual::Replica { lider_id }) => ("REPLICA".to_string(), Some(lider_id)),
            Some(RolActual::Candidato) => ("CANDIDATO".to_string(), None),
            None => ("DESCONOCIDO".to_string(), None),
        };
        let vivos: Vec<u32> = self.peers_vivos().iter().map(id_de_addr).collect();
        let todos: Vec<u32> = self.peers.iter().map(id_de_addr).collect();
        let addr_pub = self.public_listener.local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "?".to_string());

        let (total, pre, cobros, reembolsos) = {
            let pagos = self.pagos_procesados.lock().unwrap_or_else(|e| e.into_inner());
            let cuenta = |t: TipoOperacionPago| pagos.iter().filter(|p| p.tipo == t).count();
            (
                pagos.len(),
                cuenta(TipoOperacionPago::PreAutorizacion),
                cuenta(TipoOperacionPago::CobroFinal),
                cuenta(TipoOperacionPago::ReembolsoPenalizacion),
            )
        };
        let en_curso = self.transacciones_en_curso.lock().unwrap_or_else(|e| e.into_inner()).len();

        println!("\n{BOLD_CYAN}=== Info ProcesadorPagos {} ==={RESET}", self.procesador_id);
        println!("  Rol:                {rol}");
        if let Some(lider_id) = lider {
            println!("  Líder:              {lider_id}");
        }
        println!("  Dirección pública:  {addr_pub}");
        println!("  Peers vivos:        {vivos:?} de {todos:?}");
        println!("  Pagos procesados:   {total} (preautorizaciones: {pre}, cobros: {cobros}, reembolsos: {reembolsos})");
        println!("  Transacciones 2PC en curso: {en_curso}");
        println!();
    }
}

fn listar_comandos_procesador(procesador_id: u32) {
    println!(
        "\n{BOLD_CYAN}=== ProcesadorPagos {procesador_id} — CLI ==={RESET}\n\
         {BOLD}Comandos disponibles:{RESET}\n\
         {GREEN}  info{RESET}          - Ver el estado de este procesador\n\
         {GREEN}  ver_comandos{RESET}  - Ver esta lista de comandos\n\
         {GREEN}  clear{RESET}         - Limpiar la pantalla\n"
    );
}

fn limpiar_pantalla() {
    print!("\x1b[2J\x1b[H");
    let _ = std::io::stdout().flush();
}

#[tokio::main]
async fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() < MINIMAL_LENGTH {
        println!("Cantidad de argumentos invalida. Ejecutar el comando como: 'cargo run --bin procesador_pagos <path/a/archivo_config.json>");
        return;
    }

    let file_path = &args[CONFIG_FILE];

    match ProcesadorPagos::from_json(file_path).await {
        Ok(procesador) => {
            if let Err(err) = Arc::new(procesador).init().await {
                println!("{}", err);
            }
        }
        Err(err) => println!("{}", err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    #[test]
    fn test_id_de_addr_deriva_el_id_del_puerto() {
        let addr: SocketAddr = "127.0.0.1:6002".parse().unwrap();
        assert_eq!(id_de_addr(&addr), 2);
    }

    #[test]
    fn test_addr_de_id_construye_el_puerto_esperado() {
        assert_eq!(addr_de_id(3, ip()), "127.0.0.1:6003".parse().unwrap());
    }

    #[test]
    fn test_id_y_addr_son_inversas() {
        for id in 1..=10 {
            assert_eq!(id_de_addr(&addr_de_id(id, ip())), id);
        }
    }
}
