use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
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
const TIMEOUT_OK: u64 = 3;
const TIMEOUT_COORDINADOR: u64 = 8;
const PUERTO_BASE: u16 = 6000;

fn id_de_addr(addr: &SocketAddr) -> u32 {
    (addr.port() - PUERTO_BASE) as u32
}

fn addr_de_id(id: u32, ip: IpAddr) -> SocketAddr {
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

/// Rol del nodo en runtime (a diferencia de `Rol`, que solo agrupa el socket y
/// las direcciones del config). Muta durante la vida del proceso a medida que
/// el algoritmo bully resuelve quién es el líder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RolActual {
    Lider,
    Candidato,
    Replica { lider_id: u32 },
}
/// Etiqueta de log del nodo: "<ROL> <id>", con el rol vigente en este momento.
/// Se recalcula en cada log para que refleje los cambios de rol (i.e: una
/// réplica que gana una elección pasa a imprimir "LIDER <id>"). Si el mutex
/// está envenenado no se puede confiar en el rol, así que cae a "NODO <id>".
fn formato_etiqueta(rol_actual: &Mutex<RolActual>, procesador_id: u32) -> String {
    let nombre = match rol_actual.lock() {
        Ok(rol) => match *rol {
            RolActual::Lider => "LIDER",
            RolActual::Candidato => "CANDIDATO",
            RolActual::Replica { .. } => "REPLICA",
        },
        Err(_) => "NODO",
    };
    format!("{} {}", nombre, procesador_id)
}

/// Contexto compartido para el algoritmo bully: agrupa lo que las distintas
/// tareas (monitor, receptor y la propia elección) necesitan para votar y
/// proclamar líder. Se comparte por `Arc`
struct ContextoBully {
    procesador_id: u32,
    ip: IpAddr,
    socket: Arc<UdpSocket>,
    peers: Vec<SocketAddr>,
    rol_actual: Arc<Mutex<RolActual>>,
    recibio_ok: Arc<Mutex<bool>>,
}

impl ContextoBully {
    fn etiqueta(&self) -> String {
        formato_etiqueta(&self.rol_actual, self.procesador_id)
    }

    fn rol(&self) -> Option<RolActual> {
        match self.rol_actual.lock() {
            Ok(rol) => Some(*rol),
            Err(e) => {
                println!("[NODO {}]: Mutex de rol_actual envenenado: {}", self.procesador_id, e);
                None
            }
        }
    }

    fn set_rol(&self, nuevo: RolActual) {
        match self.rol_actual.lock() {
            Ok(mut rol) => *rol = nuevo,
            Err(e) => println!("[NODO {}]: Mutex de rol_actual envenenado: {}", self.procesador_id, e),
        }
    }

    /// Si el nodo es Replica, lo pasa a Candidato y devuelve true (hay que iniciar
    /// elección). Si ya es Candidato o Lider, no hace nada y devuelve false.
    fn intentar_pasar_a_candidato(&self) -> bool {
        match self.rol_actual.lock() {
            Ok(mut rol) => {
                if matches!(*rol, RolActual::Replica { .. }) {
                    *rol = RolActual::Candidato;
                    true
                } else {
                    false
                }
            }
            Err(e) => {
                println!("[NODO {}]: Mutex de rol_actual envenenado: {}", self.procesador_id, e);
                false
            }
        }
    }

    fn recibio_ok(&self) -> bool {
        match self.recibio_ok.lock() {
            Ok(flag) => *flag,
            Err(e) => {
                println!("[NODO {}]: Mutex de recibio_ok envenenado: {}", self.procesador_id, e);
                false
            }
        }
    }

    fn set_recibio_ok(&self, valor: bool) {
        match self.recibio_ok.lock() {
            Ok(mut flag) => *flag = valor,
            Err(e) => println!("[NODO {}]: Mutex de recibio_ok envenenado: {}", self.procesador_id, e),
        }
    }

    /// Direcciones de los peers con id mayor al propio (a quienes mandar Eleccion).
    fn peers_mayores(&self) -> Vec<SocketAddr> {
        self.peers
            .iter()
            .copied()
            .filter(|addr| id_de_addr(addr) > self.procesador_id)
            .collect()
    }

    /// Direcciones con las que el nodo intercambia heartbeats
    fn objetivos_heartbeat(&self) -> Vec<SocketAddr> {
        match self.rol() {
            Some(RolActual::Lider) => self.peers.clone(),
            Some(RolActual::Replica { lider_id }) => vec![addr_de_id(lider_id, self.ip)],
            _ => Vec::new(),
        }
    }

    /// Serializa `msg` y lo envía a cada destino por UDP.
    async fn enviar_a(&self, destinos: &[SocketAddr], msg: &MsgProcesador) {
        let etiqueta = self.etiqueta();
        let payload = match serde_json::to_vec(msg) {
            Ok(payload) => payload,
            Err(e) => {
                println!("[{}]: Error serializando {:?}: {}", etiqueta, msg, e);
                return;
            }
        };
        for destino in destinos {
            match self.socket.send_to(&payload, destino).await {
                Ok(_) => println!("[{}]: {:?} enviado a {}", etiqueta, msg, destino),
                Err(e) => println!("[{}]: Error enviando {:?} a {}: {}", etiqueta, msg, destino, e),
            }
        }
    }

    /// Inicia una elección si el nodo no estaba ya participando de una (o siendo
    /// líder). Lanza la máquina de estados de bully en una tarea aparte.
    fn lanzar_eleccion(ctx: &Arc<ContextoBully>) {
        if ctx.intentar_pasar_a_candidato() {
            println!("[{}]: Inicio una eleccion (bully)", ctx.etiqueta());
            tokio::spawn(ctx.clone().correr_eleccion());
        }
    }

    /// Máquina de estados de bully para el candidato: manda `Eleccion` a los de
    /// id mayor; si ninguno responde `Ok`, gana y se proclama; si alguno responde,
    /// espera el `Coordinador` y, si no llega a tiempo, reintenta.
    async fn correr_eleccion(self: Arc<Self>) {
        loop {
            self.set_recibio_ok(false);

            let mayores = self.peers_mayores();
            if mayores.is_empty() { // Soy el de mayor id vivo: gano sin necesidad de preguntar.
                self.proclamarse_lider().await;
                return;
            }

            let eleccion = MsgProcesador::Eleccion { procesador_id: self.procesador_id };
            self.enviar_a(&mayores, &eleccion).await;

            tokio::time::sleep(Duration::from_secs(TIMEOUT_OK)).await;
            if !self.recibio_ok() {
                self.proclamarse_lider().await;
                return;
            }

            tokio::time::sleep(Duration::from_secs(TIMEOUT_COORDINADOR)).await;
            if !matches!(self.rol(), Some(RolActual::Candidato)) {
                return;
            }
            println!("[{}]: No llego Coordinador a tiempo, reinicio la eleccion", self.etiqueta());
        }
    }

    /// Se autoproclama líder y avisa a todos los peers con un `Coordinador`.
    async fn proclamarse_lider(&self) {
        self.set_rol(RolActual::Lider);
        println!("[{}]: Gane la eleccion: me proclamo LIDER", self.etiqueta());
        let coordinador = MsgProcesador::Coordinador { lider_id: self.procesador_id };
        self.enviar_a(&self.peers, &coordinador).await;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EstadoNodo {
    Vivo,
    Caido,
}

struct InfoNodo {
    ultimo_visto: Instant,
    estado: EstadoNodo,
}

struct ProcesadorPagos {
    procesador_id: u32,
    public_listener: TcpListener,
    private_addr: SocketAddr,
    rol_config: RolConfig,
    liveness: Arc<Mutex<HashMap<SocketAddr, InfoNodo>>>,
    rol_actual: Arc<Mutex<RolActual>>,
}

impl ProcesadorPagos {
    async fn from_json(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let config: ProcesadorPagosConfig = serde_json::from_reader(BufReader::new(file))?;

        let public_listener = TcpListener::bind(config.public_addr).await?;

        // Rol inicial derivado del config: el líder arranca como Lider; una
        // réplica arranca siguiendo al líder cuyo id deriva de su lider_addr.
        let rol_actual = match &config.rol {
            RolConfig::Lider { .. } => RolActual::Lider,
            RolConfig::Replica { lider_addr, .. } => {
                RolActual::Replica { lider_id: id_de_addr(lider_addr) }
            }
        };

        Ok(Self {
            procesador_id: config.procesador_id,
            public_listener,
            private_addr: config.private_addr,
            rol_config: config.rol,
            liveness: Arc::new(Mutex::new(HashMap::new())),
            rol_actual: Arc::new(Mutex::new(rol_actual)),
        })
    }

    /// Etiqueta de log de este nodo, con su rol vigente. Ver `formato_etiqueta`.
    fn etiqueta(&self) -> String {
        formato_etiqueta(&self.rol_actual, self.procesador_id)
    }

    async fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rol = self.get_rol_from_config().await?;

        let (socket, peers) = match rol {
            Rol::Lider(lider) => (Arc::new(lider.socket), lider.replicas),
            Rol::Replica(replica) => (Arc::new(replica.socket), replica.peers),
        };

        let bully = Arc::new(ContextoBully {
            procesador_id: self.procesador_id,
            ip: self.private_addr.ip(),
            socket,
            peers,
            rol_actual: self.rol_actual.clone(),
            recibio_ok: Arc::new(Mutex::new(false)),
        });

        // Loop de Heartbeat para "avisar" que estoy vivo
        tokio::spawn(Self::heartbeat_loop(bully.clone()));

        // Chequear que nodos estan vivos cada cierto tiempo
        tokio::spawn(Self::monitor_loop(self.liveness.clone(), bully.clone()));

        // Loop principal, recibe y procesa los mensajes entrantes.
        self.recv_loop(bully).await
    }

    /// Envía un Heartbeat a sus objetivos actuales cada HEARTBEAT_INTERVAL segundos.
    /// Los objetivos se recalculan en cada tick según el rol vigente, así que el
    /// destino cambia solo cuando el nodo cambia de rol (p. ej. tras una elección).
    async fn heartbeat_loop(bully: Arc<ContextoBully>) {
        let mut ticker = interval(Duration::from_secs(HEARTBEAT_INTERVAL));
        loop {
            ticker.tick().await;
            let msg = MsgProcesador::Heartbeat { procesador_id: bully.procesador_id };
            bully.enviar_a(&bully.objetivos_heartbeat(), &msg).await;
        }
    }

    /// Revisa cada MONITOR_INTERVAL segundos el estado de cada nodo y detecta las
    /// transiciones: un nodo pasa a CAIDO si superó LIVENESS_TIMEOUT sin noticias,
    /// o vuelve a VIVO si dio señales de vida. Solo loguea cuando el estado cambia.
    async fn monitor_loop(
        liveness: Arc<Mutex<HashMap<SocketAddr, InfoNodo>>>,
        bully: Arc<ContextoBully>,
    ) {
        let mut ticker = interval(Duration::from_secs(MONITOR_INTERVAL));
        loop {
            ticker.tick().await;
            let ahora = Instant::now();
            let etiqueta = bully.etiqueta();
            let objetivos = bully.objetivos_heartbeat();
            let transiciones = Self::revisar_liveness(&liveness, &objetivos, &etiqueta, ahora);
            for (addr, estado) in transiciones {
                match estado {
                    EstadoNodo::Caido => {
                        println!(
                            "[{}]: Procesador en {} se considera CAIDO (sin noticias hace mas de {}s)",
                            etiqueta, addr, LIVENESS_TIMEOUT
                        );
                        Self::manejar_caida(&bully, &etiqueta, addr);
                    }
                    EstadoNodo::Vivo => println!(
                        "[{}]: Procesador en {} volvio a estar VIVO",
                        etiqueta, addr
                    ),
                }
            }
        }
    }

    /// Reacciona a la caída de un nodo: si el que cayó es el líder que este nodo
    /// sigue, dispara una elección de bully. Si no, no hace nada.
    fn manejar_caida(bully: &Arc<ContextoBully>, etiqueta: &str, addr: SocketAddr) {
        let id_caido = id_de_addr(&addr);
        let es_mi_lider = matches!(
            bully.rol(),
            Some(RolActual::Replica { lider_id }) if lider_id == id_caido
        );
        if es_mi_lider {
            println!("[{}]: El lider {} cayo", etiqueta, id_caido);
            ContextoBully::lanzar_eleccion(bully);
        }
    }

    /// Reconcilia el liveness con los `objetivos` que el nodo monitorea ahora
    /// (agrega los nuevos con una ventana de gracia, descarta los que ya no
    /// corresponden) y devuelve los que cambiaron de estado vivo<->caido. Hace
    /// todo bajo un solo lock para que el set monitoreado siga al rol actual.
    fn revisar_liveness(
        liveness: &Arc<Mutex<HashMap<SocketAddr, InfoNodo>>>,
        objetivos: &[SocketAddr],
        etiqueta: &str,
        ahora: Instant,
    ) -> Vec<(SocketAddr, EstadoNodo)> {
        let mut mapa = match liveness.lock() {
            Ok(mapa) => mapa,
            Err(e) => {
                println!("[{}]: Mutex de liveness envenenado: {}", etiqueta, e);
                return Vec::new();
            }
        };

        for objetivo in objetivos {
            mapa.entry(*objetivo)
                .or_insert(InfoNodo { ultimo_visto: ahora, estado: EstadoNodo::Vivo });
        }
        mapa.retain(|addr, _| objetivos.contains(addr));

        let mut cambios = Vec::new();
        for (addr, info) in mapa.iter_mut() {
            let vencido =
                ahora.duration_since(info.ultimo_visto) > Duration::from_secs(LIVENESS_TIMEOUT);
            let nuevo_estado = if vencido { EstadoNodo::Caido } else { EstadoNodo::Vivo };
            if nuevo_estado != info.estado {
                info.estado = nuevo_estado;
                cambios.push((*addr, nuevo_estado));
            }
        }
        cambios
    }

    /// Loop receptor: procesa los mensajes entrantes (incluidos los heartbeats de otros).
    async fn recv_loop(
        &self,
        bully: Arc<ContextoBully>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = [0u8; 1024];
        loop {
            match bully.socket.recv_from(&mut buf).await {
                Ok((size, from)) => {
                    match serde_json::from_slice::<MsgProcesador>(&buf[..size]) {
                        Ok(msg) => {
                            Self::handle_msg(&bully, &self.liveness, from, msg).await;
                        }
                        Err(e) => {
                            println!("[{}]: Mensaje invalido de {}: {}", bully.etiqueta(), from, e);
                        }
                    }
                }
                Err(e) => {
                    println!("[{}]: Fallo al recibir datagrama: {}", bully.etiqueta(), e);
                }
            }
        }
    }

    /// Procesa un mensaje entrante. Refresca el liveness del emisor y, según el
    /// tipo, responde el heartbeat o avanza la máquina de bully.
    async fn handle_msg(
        bully: &Arc<ContextoBully>,
        liveness: &Arc<Mutex<HashMap<SocketAddr, InfoNodo>>>,
        from: SocketAddr,
        msg: MsgProcesador,
    ) {
        let etiqueta = bully.etiqueta();
        println!("[{}]: Mensaje recibido de {}: {:?}", etiqueta, from, msg);

        Self::actualizar_liveness(&etiqueta, liveness, from);

        match msg {
            MsgProcesador::Heartbeat { .. } => {
                Self::enviar_heartbeat_ack(bully, from).await;
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
            MsgProcesador::Eleccion { .. } => {
                Self::manejar_eleccion(bully, from).await;
            }
            MsgProcesador::Ok { procesador_id: emisor_id } => {
                println!("[{}]: Ok de {} ({}): hay un nodo mayor vivo", etiqueta, emisor_id, from);
                bully.set_recibio_ok(true);
            }
            MsgProcesador::Coordinador { lider_id } => {
                println!("[{}]: Coordinador: el nuevo lider es {} ({})", etiqueta, lider_id, from);
                bully.set_rol(RolActual::Replica { lider_id });
            }
        }
    }

    /// Responde a una `Eleccion` de un id menor: confirma con `Ok` y, si seguía a
    /// un líder, inicia su propia elección; si ya era líder, reafirma con `Coordinador`.
    async fn manejar_eleccion(bully: &Arc<ContextoBully>, from: SocketAddr) {
        let ok = MsgProcesador::Ok { procesador_id: bully.procesador_id };
        bully.enviar_a(&[from], &ok).await;

        match bully.rol() {
            Some(RolActual::Lider) => {
                let coordinador = MsgProcesador::Coordinador { lider_id: bully.procesador_id };
                bully.enviar_a(&[from], &coordinador).await;
            }
            Some(RolActual::Replica { .. }) => {
                ContextoBully::lanzar_eleccion(bully);
            }
            _ => {} // El rol es candidato => ya estoy en una elección
        }
    }

    async fn enviar_heartbeat_ack(bully: &Arc<ContextoBully>, from: SocketAddr) {
        let ack = MsgProcesador::HeartbeatAck { procesador_id: bully.procesador_id };
        bully.enviar_a(&[from], &ack).await;
    }

    /// Refresca la marca de "última señal de vida" del emisor. No cambia el
    /// estado catalogado: esa transición la decide el monitor en su próximo tick.
    fn actualizar_liveness(etiqueta: &str, liveness: &Arc<Mutex<HashMap<SocketAddr, InfoNodo>>>, from: SocketAddr) {
        match liveness.lock() {
            Ok(mut mapa) => {
                mapa.entry(from)
                    .and_modify(|info| info.ultimo_visto = Instant::now())
                    .or_insert(InfoNodo { ultimo_visto: Instant::now(), estado: EstadoNodo::Vivo });
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn id_de_addr_deriva_el_id_del_puerto() {
        let addr: SocketAddr = "127.0.0.1:6002".parse().unwrap();
        assert_eq!(id_de_addr(&addr), 2);
    }

    #[test]
    fn addr_de_id_construye_el_puerto_esperado() {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(addr_de_id(3, ip), "127.0.0.1:6003".parse().unwrap());
    }

    #[test]
    fn id_y_addr_son_inversas() {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        for id in 1..=10 {
            assert_eq!(id_de_addr(&addr_de_id(id, ip)), id);
        }
    }
}

