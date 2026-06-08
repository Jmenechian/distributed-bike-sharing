use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use matenmutex::messages::procesadores::MsgProcesador;
use serde::Deserialize;
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::interval;

const CONFIG_FILE: usize = 1;
const MINIMAL_LENGTH: usize = 2;
const HEARTBEAT_INTERVAL: u64 = 5;
const MONITOR_INTERVAL: u64 = 5;
const LIVENESS_TIMEOUT: u64 = 15;

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
    liveness: Arc<Mutex<HashMap<SocketAddr, Instant>>>,
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
            liveness: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rol = self.get_rol_from_config().await?;

        let (socket, destinos, etiqueta) = match rol {
            Rol::Lider(lider) => (Arc::new(lider.socket), lider.replicas, "LIDER"),
            Rol::Replica(replica) => (Arc::new(replica.socket), vec![replica.lider_addr], "REPLICA"),
        };

        self.seed_liveness(&destinos, etiqueta);

        // Loop de Heartbeat para "avisar" que estoy vivo
        tokio::spawn(Self::heartbeat_loop(
            socket.clone(),
            destinos,
            self.procesador_id,
            etiqueta,
        ));

        // Chequear que nodos estan vivos cada cierto tiempo
        tokio::spawn(Self::monitor_loop(self.liveness.clone(), etiqueta));

        // Loop principal, recibe y procesa los mensajes entrantes.
        self.recv_loop(socket, etiqueta).await
    }

    /// Siembra el liveness con los destinos esperados (tomados del config), así
    /// el monitor puede detectar como caído incluso a un nodo que nunca arrancó.
    /// El instante inicial les da una ventana de gracia igual al timeout.
    fn seed_liveness(&self, destinos: &[SocketAddr], etiqueta: &str) {
        let ahora = Instant::now();
        match self.liveness.lock() {
            Ok(mut mapa) => {
                for destino in destinos {
                    mapa.insert(*destino, ahora);
                }
            }
            Err(e) => println!("[{}]: Mutex de liveness envenenado: {}", etiqueta, e),
        }
    }

    /// Envía un Heartbeat a todos los destinos cada HEARTBEAT_INTERVAL segundos.
    async fn heartbeat_loop(
        socket: Arc<UdpSocket>,
        destinos: Vec<SocketAddr>,
        procesador_id: u32,
        etiqueta: &'static str,
    ) {
        let mut ticker = interval(Duration::from_secs(HEARTBEAT_INTERVAL));
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
                match socket.send_to(&payload, destino).await {
                    Ok(_) => {
                        println!("[{}]: Mensaje enviado a {}: {:?}", etiqueta, destino, msg);
                    }
                    Err(e) => {
                        println!("[{}]: Error enviando heartbeat a {}: {}", etiqueta, destino, e);
                    }
                }
            }
        }
    }

    /// Revisa cada MONITOR_INTERVAL segundos quién dejó de dar señales de vida
    /// y loguea como CAIDO a los que superaron LIVENESS_TIMEOUT sin noticias.
    async fn monitor_loop(liveness: Arc<Mutex<HashMap<SocketAddr, Instant>>>, etiqueta: &'static str) {
        let mut ticker = interval(Duration::from_secs(MONITOR_INTERVAL));
        loop {
            ticker.tick().await;
            let ahora = Instant::now();
            let caidos: Vec<SocketAddr> = {
                let mapa = match liveness.lock() {
                    Ok(mapa) => mapa,
                    Err(e) => {
                        println!("[{}]: Mutex de liveness envenenado: {}", etiqueta, e);
                        continue;
                    }
                };
                mapa.iter()
                    .filter(|(_, visto)| {
                        ahora.duration_since(**visto) > Duration::from_secs(LIVENESS_TIMEOUT)
                    })
                    .map(|(addr, _)| *addr)
                    .collect()
            };
            for addr in caidos {
                println!(
                    "[{}]: Procesador en {} no responde hace mas de {}s, se considera CAIDO",
                    etiqueta, addr, LIVENESS_TIMEOUT
                );
            }
        }
    }

    /// Loop receptor: procesa los mensajes entrantes (incluidos los heartbeats de otros).
    async fn recv_loop(
        &self,
        socket: Arc<UdpSocket>,
        etiqueta: &'static str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = [0u8; 1024];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((size, from)) => {
                    match serde_json::from_slice::<MsgProcesador>(&buf[..size]) {
                        Ok(msg) => {
                            Self::handle_msg(&socket, self.procesador_id, etiqueta, &self.liveness, from, msg).await;
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

    /// Procesa un mensaje entrante. Ante un Heartbeat responde con su HeartbeatAck
    /// al emisor y ante un HeartbeatAck registra que el destino sigue vivo. Cualquier
    /// mensaje recibido refresca el estado de liveness del procesador emisor.
    async fn handle_msg(
        socket: &UdpSocket,
        procesador_id: u32,
        etiqueta: &str,
        liveness: &Arc<Mutex<HashMap<SocketAddr, Instant>>>,
        from: SocketAddr,
        msg: MsgProcesador,
    ) {
        println!("[{}]: Mensaje recibido de {}: {:?}", etiqueta, from, msg);

        Self::actualizar_liveness(etiqueta, liveness, from);

        match msg {
            MsgProcesador::Heartbeat { procesador_id: emisor_id } => {
                Self::enviar_heartbeat_ack(socket, procesador_id, etiqueta, from, emisor_id).await;
            }
            MsgProcesador::HeartbeatAck { procesador_id: emisor_id } => {
                println!(
                    "[{}]: HeartbeatAck recibido del procesador {} ({})",
                    etiqueta, emisor_id, from
                );
            }
            MsgProcesador::Hola { procesador_id: emisor_id } => {
                println!("[{}]: Hola recibido del procesador {} ({})", etiqueta, emisor_id, from);
            }
        }
    }

    async fn enviar_heartbeat_ack(socket: &UdpSocket, procesador_id: u32, etiqueta: &str, from: SocketAddr, emisor_id: u32) {
        let ack = MsgProcesador::HeartbeatAck { procesador_id };
        let payload = match serde_json::to_vec(&ack) {
            Ok(payload) => payload,
            Err(e) => {
                println!("[{}]: Error serializando heartbeat ack: {}", etiqueta, e);
                return;
            }
        };
        match socket.send_to(&payload, from).await {
            Ok(_) => {
                println!(
                    "[{}]: HeartbeatAck enviado a procesador {} ({})",
                    etiqueta, emisor_id, from
                );
            }
            Err(e) => {
                println!("[{}]: Error enviando heartbeat ack a {}: {}", etiqueta, from, e);
            }
        }
    }

    fn actualizar_liveness(etiqueta: &str, liveness: &Arc<Mutex<HashMap<SocketAddr, Instant>>>, from: SocketAddr) {
        match liveness.lock() {
            Ok(mut mapa) => {
                mapa.insert(from, Instant::now());
            }
            Err(e) => {
                println!("[{}]: Mutex de liveness envenenado: {}", etiqueta, e);
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

