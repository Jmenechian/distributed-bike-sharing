use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use matenmutex::messages::procesadores::MsgProcesador;
use serde::Deserialize;
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::interval;

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

struct Lider {
    socket: UdpSocket,
    replicas: Vec<SocketAddr>,
}

struct Replica {
    socket: UdpSocket,
    lider_addr: SocketAddr,
    peers: Vec<SocketAddr>,
}

enum Rol {
    Lider(Lider),
    Replica(Replica),
}

struct ProcesadorPagos {
    procesador_id: u32,
    public_listener: TcpListener,
    private_addr: SocketAddr,
    rol_config: RolConfig,
}

impl ProcesadorPagos {
    async fn from_json(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let config: ProcesadorPagosConfig = serde_json::from_reader(BufReader::new(file))?;

        let public_listener = TcpListener::bind(config.public_addr).await?;

        Ok(Self {
            procesador_id: config.procesador_id,
            public_listener,
            private_addr: config.private_addr,
            rol_config: config.rol,
        })
    }

    async fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rol = self.get_rol_from_config().await?;

        let (socket, destinos, etiqueta) = match rol {
            Rol::Lider(lider) => (Arc::new(lider.socket), lider.replicas, "LIDER"),
            Rol::Replica(replica) => (Arc::new(replica.socket), vec![replica.lider_addr], "REPLICA"),
        };

        // Tarea que envía heartbeats a todos los destinos cada 5 segundos
        let socket_hb = socket.clone();
        let procesador_id = self.procesador_id;
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let msg = MsgProcesador::Heartbeat { procesador_id };
                let payload = match serde_json::to_vec(&msg) {
                    Ok(payload) => payload,
                    Err(e) => {
                        println!("[{}]: Error serializando heartbeat: {}", etiqueta, e);
                        continue;
                    }
                };
                for destino in &destinos {
                    match socket_hb.send_to(&payload, destino).await {
                        Ok(_) => {
                            println!("[{}]: Mensaje enviado a {}: {:?}", etiqueta, destino, msg);
                        }
                        Err(e) => {
                            println!("[{}]: Error enviando heartbeat a {}: {}", etiqueta, destino, e);
                        }
                    }
                }
            }
        });

        // Loop receptor: procesa los mensajes entrantes (incluidos los heartbeats de otros)
        let mut buf = [0u8; 1024];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((size, from)) => {
                    match serde_json::from_slice::<MsgProcesador>(&buf[..size]) {
                        Ok(msg) => {
                            println!("[{}]: Mensaje recibido de {}: {:?}", etiqueta, from, msg);
                        }
                        Err(e) => {
                            println!("[{}]: Mensaje invalido de {}: {}", etiqueta, from, e);
                        }
                    }
                }
                Err(e) => {
                    println!("[{}]: Fallo al recibir datagrama: {}", etiqueta, e);
                }
            }
        }
    }

    async fn get_rol_from_config(&self) -> Result<Rol, Box<dyn Error>> {
        let socket = UdpSocket::bind(self.private_addr).await?;
        Ok(match &self.rol_config {
            RolConfig::Lider { replicas } => {
                Rol::Lider(Lider {
                    socket,
                    replicas: replicas.clone(),
                })
            },
            RolConfig::Replica { lider_addr, peers } => {
                Rol::Replica(Replica {
                    socket,
                    lider_addr: *lider_addr,
                    peers: peers.clone(),
                })
            }
        })
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
        Ok(procesador) => {
            if let Err(err) = procesador.init().await {
                println!("{}", err);
            }
        }
        Err(err) => println!("{}", err),
    }
}

