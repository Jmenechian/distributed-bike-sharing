use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use matenmutex::messages::procesadores::MsgProcesador;
use serde::Deserialize;
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::interval;
use matenmutex::utils::tiempos::{TiemposBuilder, Tiempos};
const CONFIG_FILE: usize = 1;
const MINIMAL_LENGTH: usize = 2;
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

/// Rol del nodo en runtime. Muta según lo resuelve bully.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RolActual {
    Lider,
    Candidato,
    Replica { lider_id: u32 },
}

/// Etiqueta de log "<ROL> <id>"; "NODO" si el mutex se esta envenenado
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EstadoNodo {
    Vivo,
    Caido,
}

struct InfoNodo {
    ultimo_visto: Instant,
    estado: EstadoNodo,
}

fn peers_con_id_mayor(peers: &[SocketAddr], mi_id: u32) -> Vec<SocketAddr> {
    peers
        .iter()
        .copied()
        .filter(|addr| id_de_addr(addr) > mi_id)
        .collect()
}

/// Con quién intercambia heartbeats según el rol: líder→peers, réplica→su líder, candidato→nadie.
fn objetivos_para_rol(rol: RolActual, peers: &[SocketAddr], ip: IpAddr) -> Vec<SocketAddr> {
    match rol {
        RolActual::Lider => peers.to_vec(),
        RolActual::Replica { lider_id } => vec![addr_de_id(lider_id, ip)],
        RolActual::Candidato => Vec::new(),
    }
}

/// Sincroniza `mapa` con `objetivos` (siembra nuevos, descarta viejos) y devuelve los cambios vivo/caído.
fn reconciliar_y_detectar(
    mapa: &mut HashMap<SocketAddr, InfoNodo>,
    objetivos: &[SocketAddr],
    ahora: Instant,
    timeout: Duration,
) -> Vec<(SocketAddr, EstadoNodo)> {
    for objetivo in objetivos {
        mapa.entry(*objetivo)
            .or_insert(InfoNodo { ultimo_visto: ahora, estado: EstadoNodo::Vivo });
    }
    mapa.retain(|addr, _| objetivos.contains(addr));

    let mut cambios = Vec::new();
    for (addr, info) in mapa.iter_mut() {
        let vencido = ahora.duration_since(info.ultimo_visto) > timeout;
        let nuevo_estado = if vencido { EstadoNodo::Caido } else { EstadoNodo::Vivo };
        if nuevo_estado != info.estado {
            info.estado = nuevo_estado;
            cambios.push((*addr, nuevo_estado));
        }
    }
    cambios
}

struct ProcesadorPagos {
    procesador_id: u32,
    ip: IpAddr,
    public_listener: TcpListener,
    socket: UdpSocket,
    peers: Vec<SocketAddr>,
    liveness: Mutex<HashMap<SocketAddr, InfoNodo>>,
    rol_actual: Mutex<RolActual>,
    recibio_ok: Mutex<bool>,
    tiempos: Tiempos,
}

impl ProcesadorPagos {
    async fn from_json(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let config: ProcesadorPagosConfig = serde_json::from_reader(BufReader::new(file))?;

        let public_listener = TcpListener::bind(config.public_addr).await?;
        let socket = UdpSocket::bind(config.private_addr).await?;

        let (peers, rol_actual) = match config.rol {
            RolConfig::Lider { replicas } => (replicas, RolActual::Lider),
            RolConfig::Replica { peers, lider_addr } => {
                (peers, RolActual::Replica { lider_id: id_de_addr(&lider_addr) })
            }
        };

        Ok(Self {
            procesador_id: config.procesador_id,
            ip: config.private_addr.ip(),
            public_listener,
            socket,
            peers,
            liveness: Mutex::new(HashMap::new()),
            rol_actual: Mutex::new(rol_actual),
            recibio_ok: Mutex::new(false),
            tiempos: TiemposBuilder::new().build()?,
        })
    }

    async fn init(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        tokio::spawn(self.clone().heartbeat_loop());
        tokio::spawn(self.clone().monitor_loop());
        self.recv_loop().await
    }

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

    /// Objetivos según el rol, se recalcula en cada tick y se reconfigura tras una elección.
    fn objetivos_heartbeat(&self) -> Vec<SocketAddr> {
        match self.rol() {
            Some(rol) => objetivos_para_rol(rol, &self.peers, self.ip),
            None => Vec::new(),
        }
    }

    async fn heartbeat_loop(self: Arc<Self>) {
        let mut ticker = interval(self.tiempos.heartbeat);
        loop {
            ticker.tick().await;
            let msg = MsgProcesador::Heartbeat { procesador_id: self.procesador_id };
            self.enviar_a(&self.objetivos_heartbeat(), &msg).await;
        }
    }

    /// Loguea las transiciones vivo/caído periódicamente.
    async fn monitor_loop(self: Arc<Self>) {
        let mut ticker = interval(self.tiempos.monitor);
        loop {
            ticker.tick().await;
            let ahora = Instant::now();
            let etiqueta = self.etiqueta();
            let objetivos = self.objetivos_heartbeat();
            let transiciones = self.revisar_liveness(&objetivos, &etiqueta, ahora);
            for (addr, estado) in transiciones {
                match estado {
                    EstadoNodo::Caido => {
                        println!(
                            "[{}]: Procesador en {} se considera CAIDO (sin noticias hace mas de {}s)",
                            etiqueta, addr, self.tiempos.liveness_timeout.as_secs()
                        );
                        Self::handlear_caida(&self, &etiqueta, addr);
                    }
                    EstadoNodo::Vivo => println!(
                        "[{}]: Procesador en {} volvio a estar VIVO",
                        etiqueta, addr
                    ),
                }
            }
        }
    }

    fn revisar_liveness(
        &self,
        objetivos: &[SocketAddr],
        etiqueta: &str,
        ahora: Instant,
    ) -> Vec<(SocketAddr, EstadoNodo)> {
        let mut mapa = match self.liveness.lock() {
            Ok(mapa) => mapa,
            Err(e) => {
                println!("[{}]: Mutex de liveness envenenado: {}", etiqueta, e);
                return Vec::new();
            }
        };
        reconciliar_y_detectar(&mut mapa, objetivos, ahora, self.tiempos.liveness_timeout)
    }

    /// Refresca la última señal de vida del emisor. El estado lo decide el monitor.
    fn actualizar_liveness(&self, etiqueta: &str, from: SocketAddr) {
        match self.liveness.lock() {
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

    fn handlear_caida(nodo: &Arc<Self>, etiqueta: &str, addr: SocketAddr) {
        let id_caido = id_de_addr(&addr);
        let es_mi_lider = matches!(
            nodo.rol(),
            Some(RolActual::Replica { lider_id }) if lider_id == id_caido
        );
        if es_mi_lider {
            println!("[{}]: El lider {} cayo", etiqueta, id_caido);
            Self::lanzar_eleccion(nodo);
        }
    }

    async fn recv_loop(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = [0u8; 1024];
        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((size, from)) => {
                    match serde_json::from_slice::<MsgProcesador>(&buf[..size]) {
                        Ok(msg) => {
                            Self::handle_msg(&self, from, msg).await;
                        }
                        Err(e) => {
                            println!("[{}]: Mensaje invalido de {}: {}", self.etiqueta(), from, e);
                        }
                    }
                }
                Err(e) => {
                    println!("[{}]: Fallo al recibir datagrama: {}", self.etiqueta(), e);
                }
            }
        }
    }

    /// Maneja los mensajes recibidos y actua en base a ellos. Siempre que se recibe un
    /// mensaje se refresca el liveness de un nodo
    async fn handle_msg(nodo: &Arc<Self>, from: SocketAddr, msg: MsgProcesador) {
        let etiqueta = nodo.etiqueta();
        println!("[{}]: Mensaje recibido de {}: {:?}", etiqueta, from, msg);

        nodo.actualizar_liveness(&etiqueta, from);

        match msg {
            MsgProcesador::Heartbeat { .. } => {
                nodo.enviar_heartbeat_ack(from).await;
            }
            MsgProcesador::HeartbeatAck { procesador_id: emisor_id } => {
                println!(
                    "[{}]: HeartbeatAck recibido del procesador {} ({})",
                    etiqueta, emisor_id, from
                );
            }
            MsgProcesador::Eleccion { .. } => {
                Self::handlear_eleccion(nodo, from).await;
            }
            MsgProcesador::Ok { procesador_id: emisor_id } => {
                println!("[{}]: Ok de {} ({}): hay un nodo mayor vivo", etiqueta, emisor_id, from);
                nodo.set_recibio_ok(true);
            }
            MsgProcesador::Coordinador { lider_id } => {
                println!("[{}]: Coordinador: el nuevo lider es {} ({})", etiqueta, lider_id, from);
                nodo.set_rol(RolActual::Replica { lider_id });
            }
        }
    }

    async fn enviar_heartbeat_ack(&self, from: SocketAddr) {
        let ack = MsgProcesador::HeartbeatAck { procesador_id: self.procesador_id };
        self.enviar_a(&[from], &ack).await;
    }

    fn peers_mayores(&self) -> Vec<SocketAddr> {
        peers_con_id_mayor(&self.peers, self.procesador_id)
    }

    fn lanzar_eleccion(nodo: &Arc<Self>) {
        if nodo.intentar_pasar_a_candidato() {
            println!("[{}]: Inicio una eleccion (bully)", nodo.etiqueta());
            tokio::spawn(nodo.clone().correr_eleccion());
        }
    }

    /// Bully (candidato): manda Eleccion a los mayores; sin Ok gana, con Ok espera
    /// el Coordinador y, si no llega a tiempo, reintenta.
    async fn correr_eleccion(self: Arc<Self>) {
        loop {
            self.set_recibio_ok(false);

            let mayores = self.peers_mayores();
            if mayores.is_empty() {
                self.proclamarse_lider().await;
                return;
            }

            let eleccion = MsgProcesador::Eleccion { procesador_id: self.procesador_id };
            self.enviar_a(&mayores, &eleccion).await;

            tokio::time::sleep(self.tiempos.espera_ok).await;
            if !self.recibio_ok() {
                self.proclamarse_lider().await;
                return;
            }

            tokio::time::sleep(self.tiempos.espera_coordinador).await;
            if !matches!(self.rol(), Some(RolActual::Candidato)) {
                return;
            }
            println!("[{}]: No llego Coordinador a tiempo, reinicio la eleccion", self.etiqueta());
        }
    }

    /// Se proclama líder y lo difunde con Coordinador.
    async fn proclamarse_lider(&self) {
        self.set_rol(RolActual::Lider);
        println!("[{}]: Gane la eleccion: me proclamo LIDER", self.etiqueta());
        let coordinador = MsgProcesador::Coordinador { lider_id: self.procesador_id };
        self.enviar_a(&self.peers, &coordinador).await;
    }

    /// Responde Ok.
    /// Si era líder reafirma Coordinador,
    /// si era réplica arranca su elección.
    async fn handlear_eleccion(nodo: &Arc<Self>, from: SocketAddr) {
        let ok = MsgProcesador::Ok { procesador_id: nodo.procesador_id };
        nodo.enviar_a(&[from], &ok).await;

        match nodo.rol() {
            Some(RolActual::Lider) => {
                let coordinador = MsgProcesador::Coordinador { lider_id: nodo.procesador_id };
                nodo.enviar_a(&[from], &coordinador).await;
            }
            Some(RolActual::Replica { .. }) => {
                Self::lanzar_eleccion(nodo);
            }
            _ => {}
        }
    }

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

    // Valores mas cortos para poder testear
    const HEARTBEAT_INTERVAL: u64 = 1;
    const MONITOR_INTERVAL: u64 = 1;
    const LIVENESS_TIMEOUT: u64 = 3;
    const TIMEOUT_OK: u64 = 1;
    const TIMEOUT_COORDINADOR: u64 = 2;

    fn ip() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    fn addr(id: u32) -> SocketAddr {
        addr_de_id(id, ip())
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

    #[test]
    fn test_peers_con_id_mayor_filtra_los_mayores() {
        let peers = vec![addr(1), addr(3), addr(4)];
        assert_eq!(peers_con_id_mayor(&peers, 2), vec![addr(3), addr(4)]);
    }

    #[test]
    fn test_peers_con_id_mayor_vacio_para_el_de_mayor_id() {
        let peers = vec![addr(1), addr(2), addr(3)];
        assert!(peers_con_id_mayor(&peers, 4).is_empty());
    }

    #[test]
    fn test_objetivos_de_lider_son_todos_los_peers() {
        let peers = vec![addr(2), addr(3), addr(4)];
        assert_eq!(objetivos_para_rol(RolActual::Lider, &peers, ip()), peers);
    }

    #[test]
    fn test_objetivos_de_replica_es_solo_su_lider() {
        let peers = vec![addr(1), addr(2), addr(3)];
        let objetivos = objetivos_para_rol(RolActual::Replica { lider_id: 1 }, &peers, ip());
        assert_eq!(objetivos, vec![addr(1)]);
    }

    #[test]
    fn test_objetivos_de_candidato_es_vacio() {
        let peers = vec![addr(1), addr(2)];
        assert!(objetivos_para_rol(RolActual::Candidato, &peers, ip()).is_empty());
    }

    #[test]
    fn test_reconciliar_siembra_objetivo_nuevo_sin_transicion() {
        let mut mapa = HashMap::new();
        let cambios = reconciliar_y_detectar(&mut mapa, &[addr(2)], Instant::now(), Duration::from_secs(3));
        assert!(cambios.is_empty());
        assert!(mapa.contains_key(&addr(2)));
    }

    #[test]
    fn test_reconciliar_detecta_caido_una_sola_vez() {
        let timeout = Duration::from_secs(3);
        let t0 = Instant::now();
        let mut mapa = HashMap::new();
        mapa.insert(addr(2), InfoNodo { ultimo_visto: t0, estado: EstadoNodo::Vivo });

        let ahora = t0 + Duration::from_secs(10);
        let cambios = reconciliar_y_detectar(&mut mapa, &[addr(2)], ahora, timeout);
        assert_eq!(cambios, vec![(addr(2), EstadoNodo::Caido)]);

        let cambios = reconciliar_y_detectar(&mut mapa, &[addr(2)], ahora, timeout);
        assert!(cambios.is_empty());
    }

    #[test]
    fn test_reconciliar_detecta_recuperacion() {
        let ahora = Instant::now();
        let mut mapa = HashMap::new();
        // Caído pero con señal reciente (la refrescó actualizar_liveness).
        mapa.insert(addr(2), InfoNodo { ultimo_visto: ahora, estado: EstadoNodo::Caido });
        let cambios = reconciliar_y_detectar(&mut mapa, &[addr(2)], ahora, Duration::from_secs(3));
        assert_eq!(cambios, vec![(addr(2), EstadoNodo::Vivo)]);
    }

    #[test]
    fn test_reconciliar_da_de_baja_lo_que_dejo_de_ser_objetivo() {
        let ahora = Instant::now();
        let mut mapa = HashMap::new();
        mapa.insert(addr(3), InfoNodo { ultimo_visto: ahora, estado: EstadoNodo::Vivo });
        reconciliar_y_detectar(&mut mapa, &[addr(2)], ahora, Duration::from_secs(3));
        assert!(!mapa.contains_key(&addr(3)));
        assert!(mapa.contains_key(&addr(2)));
    }

    #[test]
    fn test_formato_etiqueta_segun_rol() {
        assert_eq!(formato_etiqueta(&Mutex::new(RolActual::Lider), 4), "LIDER 4");
        assert_eq!(formato_etiqueta(&Mutex::new(RolActual::Candidato), 2), "CANDIDATO 2");
        assert_eq!(formato_etiqueta(&Mutex::new(RolActual::Replica { lider_id: 1 }), 2), "REPLICA 2");
    }

    // Integración end-to-end: cluster real con timeouts cortos inyectados. Usa los
    // puertos fijos 6001-6004.

    fn tiempos_test() -> Tiempos {
        TiemposBuilder::new()
            .heartbeat(Duration::from_secs(HEARTBEAT_INTERVAL))
            .monitor(Duration::from_secs(MONITOR_INTERVAL))
            .liveness_timeout(Duration::from_secs(LIVENESS_TIMEOUT))
            .espera_ok(Duration::from_secs(TIMEOUT_OK))
            .espera_coordinador(Duration::from_secs(TIMEOUT_COORDINADOR))
            .build()
            .expect("tiempos de test validos")
    }

    async fn nodo_de_prueba(id: u32, cantidad: u32, rol: RolActual) -> Arc<ProcesadorPagos> {
        let socket = UdpSocket::bind(addr(id)).await.expect("bind UDP de prueba");
        let public_listener = TcpListener::bind((ip(), 0)).await.expect("bind TCP de prueba");
        let peers = (1..=cantidad).filter(|j| *j != id).map(addr).collect();
        Arc::new(ProcesadorPagos {
            procesador_id: id,
            ip: ip(),
            public_listener,
            socket,
            peers,
            liveness: Mutex::new(HashMap::new()),
            rol_actual: Mutex::new(rol),
            recibio_ok: Mutex::new(false),
            tiempos: tiempos_test(),
        })
    }

    // Lanza los 3 loops y devuelve sus handles
    fn spawn_loops(nodo: &Arc<ProcesadorPagos>) -> Vec<tokio::task::JoinHandle<()>> {
        let receptor = nodo.clone();
        vec![
            tokio::spawn(nodo.clone().heartbeat_loop()),
            tokio::spawn(nodo.clone().monitor_loop()),
            tokio::spawn(async move { let _ = receptor.recv_loop().await; }),
        ]
    }

    fn rol_de(nodo: &Arc<ProcesadorPagos>) -> RolActual {
        *nodo.rol_actual.lock().unwrap()
    }

    fn matar(handles: &[tokio::task::JoinHandle<()>]) {
        for h in handles {
            h.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_elige_y_reelige_al_de_mayor_id_en_cascada() {
        use tokio::time::sleep;

        let n1 = nodo_de_prueba(1, 4, RolActual::Lider).await;
        let n2 = nodo_de_prueba(2, 4, RolActual::Replica { lider_id: 1 }).await;
        let n3 = nodo_de_prueba(3, 4, RolActual::Replica { lider_id: 1 }).await;
        let n4 = nodo_de_prueba(4, 4, RolActual::Replica { lider_id: 1 }).await;

        let h1 = spawn_loops(&n1);
        let h2 = spawn_loops(&n2);
        let h3 = spawn_loops(&n3);
        let h4 = spawn_loops(&n4);

        sleep(Duration::from_secs(3)).await;

        // Cae el líder 1.
        matar(&h1);
        sleep(Duration::from_secs(8)).await;

        assert_eq!(rol_de(&n4), RolActual::Lider, "el de mayor id (4) debe ganar");
        assert_eq!(rol_de(&n2), RolActual::Replica { lider_id: 4 });
        assert_eq!(rol_de(&n3), RolActual::Replica { lider_id: 4 });

        // Cae el nuevo líder 4: re-elección al 3.
        matar(&h4);
        sleep(Duration::from_secs(8)).await;

        assert_eq!(rol_de(&n3), RolActual::Lider, "tras caer el 4, gana el 3");
        assert_eq!(rol_de(&n2), RolActual::Replica { lider_id: 3 });

        matar(&h2);
        matar(&h3);
    }
}
