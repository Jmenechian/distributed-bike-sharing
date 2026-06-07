use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};

const CONFIG_FILE: usize = 1;
const MINIMAL_LENGTH: usize = 2;

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
}

enum Rol {
    Lider {
        replicas: HashMap<SocketAddr, Option<TcpStream>>,
    },
    Replica {
        lider: TcpStream,
    },
}

struct ProcesadorPagos {
    procesador_id: u32,
    public_listener: TcpListener,
    private_listener: TcpListener,
    rol: Rol,
}

impl ProcesadorPagos {
    async fn from_json(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let config: ProcesadorPagosConfig = serde_json::from_reader(BufReader::new(file))?;

        let public_listener = TcpListener::bind(config.public_addr).await?;
        let private_listener = TcpListener::bind(config.private_addr).await?;

        let rol = match config.rol {
            RolConfig::Lider { replicas } => {
                let replicas = replicas.into_iter().map(|addr| (addr, None)).collect();
                Rol::Lider { replicas }
            }
            RolConfig::Replica { lider_addr, peers: _ } => {
                let lider = TcpStream::connect(lider_addr).await?;
                Rol::Replica { lider }
            }
        };

        Ok(Self {
            procesador_id: config.procesador_id,
            public_listener,
            private_listener,
            rol,
        })
    }

    async fn init(self) {
        match self.rol {
            Rol::Lider { replicas: _replicas } => {
                todo!("aceptar las conexiones de las replicas")
            }
            Rol::Replica { lider: _lider } => {
                // comunicarse con el lider ¿?
            }
        }
    }
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
        Ok(procesador) => procesador.init().await,
        Err(err) => println!("{}", err),
    }
}

