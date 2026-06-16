use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use matenmutex::messages::sede_procesador::MsgSedeAProcesadorPagos;
use matenmutex::utils::tiempos::{Tiempos, TiemposBuilder};
use tokio::net::{TcpListener, UdpSocket};

use super::bully::RolActual;
use super::dos_pc::addr_2pc_de_id;
use super::{addr_de_id, ProcesadorPagos};

pub(crate) const HEARTBEAT_INTERVAL: u64 = 1;
pub(crate) const MONITOR_INTERVAL: u64 = 1;
pub(crate) const LIVENESS_TIMEOUT: u64 = 3;
pub(crate) const TIMEOUT_OK: u64 = 1;
pub(crate) const TIMEOUT_COORDINADOR: u64 = 2;

pub(crate) fn ip() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

pub(crate) fn addr(id: u32) -> SocketAddr {
    addr_de_id(id, ip())
}

pub(crate) fn tiempos_test() -> Tiempos {
    TiemposBuilder::new()
        .heartbeat(Duration::from_secs(HEARTBEAT_INTERVAL))
        .monitor(Duration::from_secs(MONITOR_INTERVAL))
        .liveness_timeout(Duration::from_secs(LIVENESS_TIMEOUT))
        .espera_ok(Duration::from_secs(TIMEOUT_OK))
        .espera_coordinador(Duration::from_secs(TIMEOUT_COORDINADOR))
        .build()
        .expect("tiempos de test validos")
}

pub(crate) async fn nodo_de_prueba(id: u32, cantidad: u32, rol: RolActual) -> Arc<ProcesadorPagos> {
    let socket = UdpSocket::bind(addr(id)).await.expect("bind UDP de prueba");
    let public_listener = TcpListener::bind((ip(), 0)).await.expect("bind TCP de prueba");
    let bus_2pc = TcpListener::bind((ip(), 0)).await.expect("bind TCP 2PC de prueba");
    let peers = (1..=cantidad).filter(|j| *j != id).map(addr).collect();
    Arc::new(ProcesadorPagos {
        procesador_id: id,
        ip: ip(),
        public_listener,
        bus_2pc,
        socket,
        peers,
        liveness: Mutex::new(HashMap::new()),
        rol_actual: Mutex::new(rol),
        recibio_ok: Mutex::new(false),
        tiempos: tiempos_test(),
        pagos_procesados: Mutex::new(Vec::new()),
        transacciones_en_curso: Mutex::new(Vec::new()),
        preparados_replica: Mutex::new(HashMap::new()),
        rechazar_si_monto_supera: None,
        estado_path: String::new(),
    })
}

pub(crate) async fn nodo_de_prueba_con_limite(id: u32, cantidad: u32, rol: RolActual, limite: Option<f64>) -> Arc<ProcesadorPagos> {
    let nodo = nodo_de_prueba(id, cantidad, rol).await;
    Arc::new(ProcesadorPagos {
        rechazar_si_monto_supera: limite,
        ..Arc::try_unwrap(nodo).unwrap_or_else(|_| panic!("nodo en uso"))
    })
}

pub(crate) fn spawn_loops(nodo: &Arc<ProcesadorPagos>) -> Vec<tokio::task::JoinHandle<()>> {
    let receptor = nodo.clone();
    vec![
        tokio::spawn(nodo.clone().heartbeat_loop()),
        tokio::spawn(nodo.clone().monitor_loop()),
        tokio::spawn(async move { let _ = receptor.recv_loop().await; }),
    ]
}

pub(crate) fn rol_de(nodo: &Arc<ProcesadorPagos>) -> RolActual {
    *nodo.rol_actual.lock().unwrap()
}

pub(crate) fn matar(handles: &[tokio::task::JoinHandle<()>]) {
    for h in handles {
        h.abort();
    }
}

pub(crate) async fn nodo_2pc_de_prueba_en_addr_canonica(id: u32, rol: RolActual, peers: Vec<u32>, limite: Option<f64>) -> Arc<ProcesadorPagos> {
    let socket = UdpSocket::bind((ip(), 0)).await.expect("bind UDP de prueba");
    let public_listener = TcpListener::bind((ip(), 0)).await.expect("bind TCP de prueba");
    let bus_2pc = TcpListener::bind(addr_2pc_de_id(id, ip())).await.expect("bind TCP 2PC canonico de prueba");
    let peers_addrs = peers.iter().map(|p| addr(*p)).collect();
    let nodo = Arc::new(ProcesadorPagos {
        procesador_id: id,
        ip: ip(),
        public_listener,
        bus_2pc,
        socket,
        peers: peers_addrs,
        liveness: Mutex::new(HashMap::new()),
        rol_actual: Mutex::new(rol),
        recibio_ok: Mutex::new(false),
        tiempos: tiempos_test(),
        pagos_procesados: Mutex::new(Vec::new()),
        transacciones_en_curso: Mutex::new(Vec::new()),
        preparados_replica: Mutex::new(HashMap::new()),
        rechazar_si_monto_supera: limite,
        estado_path: String::new(),
    });
    tokio::spawn(nodo.clone().accept_loop_2pc());
    nodo
}

pub(crate) fn cobrar_msg(payment_request_id: &str, monto: f64) -> MsgSedeAProcesadorPagos {
    MsgSedeAProcesadorPagos::CobrarPago {
        payment_request_id: payment_request_id.to_string(),
        alquiler_id: "alq-2pc".to_string(),
        cliente_id: "c-2pc".to_string(),
        monto_final: monto,
    }
}
