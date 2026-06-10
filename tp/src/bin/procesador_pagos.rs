use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use matenmutex::messages::procesadores::MsgProcesador;
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::types::{EstadoPago, PagoProcesado, TipoOperacionPago};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::interval;
use uuid::Uuid;
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
    #[serde(default)] // Usamos esto para que si las configs no vienen con el campo no rompa, usamos el campo para forzar el rechazo.
    rechazar_si_monto_supera: Option<f64>,
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
    pagos_procesados: Mutex<Vec<PagoProcesado>>,
    rechazar_si_monto_supera: Option<f64>,
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
            pagos_procesados: Mutex::new(Vec::new()),
            rechazar_si_monto_supera: config.rechazar_si_monto_supera,
        })
    }

    async fn init(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        tokio::spawn(self.clone().heartbeat_loop());
        tokio::spawn(self.clone().monitor_loop());
        tokio::spawn(self.clone().accept_loop_sedes());
        self.recv_loop().await
    }

    async fn accept_loop_sedes(self: Arc<Self>) {
        loop {
            match self.public_listener.accept().await {
                Ok((socket, addr)) => {
                    println!("[NODO {}]: Nueva conexion de Sede desde {}", self.procesador_id, addr);
                    tokio::spawn(self.clone().atender_sede(socket));
                }
                Err(e) => println!("[{}]: error aceptando sede: {}", self.etiqueta(), e),
            }
        }
    }

    async fn atender_sede(self: Arc<Self>, socket: TcpStream) {
        let (reader, mut writer) = socket.into_split();
        let mut lines = TokioBufReader::new(reader).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let resp = match serde_json::from_str::<MsgSedeAProcesadorPagos>(line.trim()) {
                Ok(msg) => {
                    println!("[procesador_pagos] Recibe {}", msg.describir());
                    self.procesar_pago(msg)
                }
                Err(_) => {
                    println!("[procesador_pagos] Recibe mensaje invalido: {}", line.trim());
                    MsgProcesadorPagosASede::PagoRechazado {
                        payment_request_id: String::new(),
                        motivo: format!("mensaje invalido: {}", line.trim()),
                    }
                }
            };
            println!("[procesador_pagos] Envia {}", resp.describir());
            let payload = match serde_json::to_string(&resp) {
                Ok(json) => json + "\n",
                Err(_) => continue,
            };
            if writer.write_all(payload.as_bytes()).await.is_err() {
                break;
            }
        }
    }

    /// Procesa un pago de forma sincronica e idempotente. El lock de
    /// `pagos_procesados` se toma y se libera enteramente dentro de esta
    /// funcion (nunca se mantiene a traves de un `.await`).
    fn procesar_pago(&self, msg: MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
        let payment_request_id = msg.payment_request_id().to_string();
        let mut pagos = match self.pagos_procesados.lock() {
            Ok(g) => g,
            Err(_) => {
                return MsgProcesadorPagosASede::PagoRechazado {
                    payment_request_id,
                    motivo: "estado de pagos no disponible".into(),
                }
            }
        };

        if let Some(pago) = pagos.iter().find(|p| p.payment_request_id == payment_request_id) {
            return respuesta_desde_pago(pago);
        }

        let (pago, respuesta) = self.decidir_operacion(&msg);
        pagos.push(pago);
        respuesta
    }

    /// Decision determinista de aceptar/rechazar + construccion del registro
    /// persistido y la respuesta correspondiente. Pura (no toca el Mutex).
    fn decidir_operacion(&self, msg: &MsgSedeAProcesadorPagos) -> (PagoProcesado, MsgProcesadorPagosASede) {
        match msg {
            MsgSedeAProcesadorPagos::PreAutorizarPago { payment_request_id, cliente_id, monto_seguridad } => {
                self.resolver_operacion(
                    payment_request_id,
                    None,
                    cliente_id,
                    *monto_seguridad,
                    TipoOperacionPago::PreAutorizacion,
                    EstadoPago::PreAutorizado,
                    "monto_seguridad excede el limite configurado",
                    |payment_request_id, pago_id| MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id },
                )
            }
            MsgSedeAProcesadorPagos::CobrarPago { payment_request_id, alquiler_id, cliente_id, monto_final } => {
                self.resolver_operacion(
                    payment_request_id,
                    Some(alquiler_id.clone()),
                    cliente_id,
                    *monto_final,
                    TipoOperacionPago::CobroFinal,
                    EstadoPago::Aceptado,
                    "monto_final excede el limite configurado",
                    |payment_request_id, pago_id| MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id },
                )
            }
            MsgSedeAProcesadorPagos::ReembolsarPago { payment_request_id, alquiler_id, cliente_id, pago_original_id: _, monto_reembolso } => {
                self.resolver_operacion(
                    payment_request_id,
                    Some(alquiler_id.clone()),
                    cliente_id,
                    *monto_reembolso,
                    TipoOperacionPago::ReembolsoPenalizacion,
                    EstadoPago::Reembolsado,
                    "monto_reembolso excede el limite configurado",
                    |payment_request_id, pago_id| MsgProcesadorPagosASede::PagoReembolsado { payment_request_id, pago_id },
                )
            }
        }
    }

    /// Construye el `PagoProcesado` a persistir y la respuesta correspondiente,
    /// aplicando la decision determinista de aceptar/rechazar segun
    /// `monto_excede_limite`. Comun a las 3 operaciones de pago.
    fn resolver_operacion(
        &self,
        payment_request_id: &str,
        alquiler_id: Option<String>,
        cliente_id: &str,
        monto: f64,
        tipo: TipoOperacionPago,
        estado_aceptado: EstadoPago,
        motivo_rechazo: &str,
        respuesta_aceptada: impl FnOnce(String, String) -> MsgProcesadorPagosASede,
    ) -> (PagoProcesado, MsgProcesadorPagosASede) {
        if self.monto_excede_limite(monto) {
            let pago = PagoProcesado {
                payment_request_id: payment_request_id.to_string(),
                pago_id: String::new(),
                alquiler_id,
                cliente_id: cliente_id.to_string(),
                monto,
                tipo,
                estado: EstadoPago::Rechazado,
            };
            let respuesta = MsgProcesadorPagosASede::PagoRechazado {
                payment_request_id: payment_request_id.to_string(),
                motivo: motivo_rechazo.to_string(),
            };
            (pago, respuesta)
        } else {
            let pago_id = Uuid::new_v4().to_string();
            let pago = PagoProcesado {
                payment_request_id: payment_request_id.to_string(),
                pago_id: pago_id.clone(),
                alquiler_id,
                cliente_id: cliente_id.to_string(),
                monto,
                tipo,
                estado: estado_aceptado,
            };
            let respuesta = respuesta_aceptada(payment_request_id.to_string(), pago_id);
            (pago, respuesta)
        }
    }

    fn monto_excede_limite(&self, monto: f64) -> bool {
        match self.rechazar_si_monto_supera {
            Some(limite) => monto > limite,
            None => false,
        }
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

/// Reconstruye la respuesta persistida para una PagoProcesado existente
/// (replay idempotente, sin re-ejecutar la simulacion).
fn respuesta_desde_pago(pago: &PagoProcesado) -> MsgProcesadorPagosASede {
    let payment_request_id = pago.payment_request_id.clone();
    match pago.estado {
        EstadoPago::PreAutorizado => MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id: pago.pago_id.clone() },
        EstadoPago::Aceptado => MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id: pago.pago_id.clone() },
        EstadoPago::Reembolsado => MsgProcesadorPagosASede::PagoReembolsado { payment_request_id, pago_id: pago.pago_id.clone() },
        EstadoPago::Rechazado => MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo: "operacion previamente rechazada".to_string() },
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
            pagos_procesados: Mutex::new(Vec::new()),
            rechazar_si_monto_supera: None,
        })
    }

    async fn nodo_de_prueba_con_limite(id: u32, cantidad: u32, rol: RolActual, limite: Option<f64>) -> Arc<ProcesadorPagos> {
        let nodo = nodo_de_prueba(id, cantidad, rol).await;
        Arc::new(ProcesadorPagos {
            rechazar_si_monto_supera: limite,
            ..Arc::try_unwrap(nodo).unwrap_or_else(|_| panic!("nodo en uso"))
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

    // ---- Pagos: tests unitarios sobre procesar_pago / decidir_operacion ----

    #[tokio::test]
    async fn pre_autorizar_pago_aceptado_persiste_y_responde() {
        let nodo = nodo_de_prueba(10, 1, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        });

        match respuesta {
            MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "pr-1");
                assert!(!pago_id.is_empty());
            }
            other => panic!("se esperaba PagoPreAutorizado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        let pago = &pagos[0];
        assert_eq!(pago.payment_request_id, "pr-1");
        assert_eq!(pago.alquiler_id, None);
        assert_eq!(pago.tipo, TipoOperacionPago::PreAutorizacion);
        assert_eq!(pago.estado, EstadoPago::PreAutorizado);
        assert_eq!(pago.monto, 100.0);
    }

    #[tokio::test]
    async fn cobrar_pago_aceptado_persiste_y_responde() {
        let nodo = nodo_de_prueba(11, 1, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "pr-3".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 200.0,
        });

        match respuesta {
            MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "pr-3");
                assert!(!pago_id.is_empty());
            }
            other => panic!("se esperaba PagoAceptado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        let pago = &pagos[0];
        assert_eq!(pago.alquiler_id, Some("alq-1".to_string()));
        assert_eq!(pago.tipo, TipoOperacionPago::CobroFinal);
        assert_eq!(pago.estado, EstadoPago::Aceptado);
    }

    #[tokio::test]
    async fn reembolsar_pago_aceptado_persiste_y_responde() {
        let nodo = nodo_de_prueba(12, 1, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "pr-5".to_string(),
            alquiler_id: "alq-3".to_string(),
            cliente_id: "c-3".to_string(),
            pago_original_id: "pago-orig-1".to_string(),
            monto_reembolso: 30.0,
        });

        match respuesta {
            MsgProcesadorPagosASede::PagoReembolsado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "pr-5");
                assert!(!pago_id.is_empty());
            }
            other => panic!("se esperaba PagoReembolsado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        let pago = &pagos[0];
        assert_eq!(pago.tipo, TipoOperacionPago::ReembolsoPenalizacion);
        assert_eq!(pago.estado, EstadoPago::Reembolsado);
        assert_eq!(pago.monto, 30.0);
    }

    #[tokio::test]
    async fn payment_request_id_repetido_es_idempotente() {
        let nodo = nodo_de_prueba(13, 1, RolActual::Lider).await;

        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-7".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };

        let r1 = nodo.procesar_pago(msg.clone());
        let r2 = nodo.procesar_pago(msg);

        assert_eq!(r1, r2);
        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
    }

    #[tokio::test]
    async fn monto_que_excede_el_limite_es_rechazado() {
        let nodo = nodo_de_prueba_con_limite(14, 1, RolActual::Lider, Some(50.0)).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-10".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        });

        match respuesta {
            MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo } => {
                assert_eq!(payment_request_id, "pr-10");
                assert!(!motivo.is_empty());
            }
            other => panic!("se esperaba PagoRechazado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        assert_eq!(pagos[0].estado, EstadoPago::Rechazado);
        assert_eq!(pagos[0].tipo, TipoOperacionPago::PreAutorizacion);
    }

    #[tokio::test]
    async fn monto_dentro_del_limite_es_aceptado() {
        let nodo = nodo_de_prueba_con_limite(15, 1, RolActual::Lider, Some(50.0)).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-11".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 50.0,
        });

        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
    }

    // ---- Pagos: integracion sobre el accept loop TCP ----

    #[tokio::test(flavor = "multi_thread")]
    async fn accept_loop_responde_pre_autorizar_pago() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let nodo = nodo_de_prueba(16, 1, RolActual::Lider).await;
        let addr = nodo.public_listener.local_addr().unwrap();
        tokio::spawn(nodo.clone().accept_loop_sedes());

        let stream = TcpStream::connect(addr).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-acc-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };
        let payload = serde_json::to_string(&msg).unwrap() + "\n";
        writer.write_all(payload.as_bytes()).await.unwrap();

        let line = lines.next_line().await.unwrap().unwrap();
        let respuesta: MsgProcesadorPagosASede = serde_json::from_str(&line).unwrap();
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn accept_loop_no_crashea_con_json_invalido_y_sigue_respondiendo() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let nodo = nodo_de_prueba(17, 1, RolActual::Lider).await;
        let addr = nodo.public_listener.local_addr().unwrap();
        tokio::spawn(nodo.clone().accept_loop_sedes());

        // Conexion 1: linea invalida.
        let stream1 = TcpStream::connect(addr).await.unwrap();
        let (reader1, mut writer1) = stream1.into_split();
        let mut lines1 = BufReader::new(reader1).lines();
        writer1.write_all(b"not json\n").await.unwrap();
        let line1 = lines1.next_line().await.unwrap().unwrap();
        let respuesta1: MsgProcesadorPagosASede = serde_json::from_str(&line1).unwrap();
        assert!(matches!(respuesta1, MsgProcesadorPagosASede::PagoRechazado { .. }));

        // Conexion 2: mensaje valido, el procesador sigue respondiendo.
        let stream2 = TcpStream::connect(addr).await.unwrap();
        let (reader2, mut writer2) = stream2.into_split();
        let mut lines2 = BufReader::new(reader2).lines();
        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-acc-2".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };
        let payload = serde_json::to_string(&msg).unwrap() + "\n";
        writer2.write_all(payload.as_bytes()).await.unwrap();
        let line2 = lines2.next_line().await.unwrap().unwrap();
        let respuesta2: MsgProcesadorPagosASede = serde_json::from_str(&line2).unwrap();
        assert!(matches!(respuesta2, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
    }

    /// End-to-end: PreAutorizar -> Cobrar -> Reembolsar sobre un unico
    /// ProcesadorPagos, mas un reintento idempotente del primer mensaje.
    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_preautorizar_cobrar_reembolsar_e_idempotencia() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let nodo = nodo_de_prueba(18, 1, RolActual::Lider).await;
        let addr = nodo.public_listener.local_addr().unwrap();
        tokio::spawn(nodo.clone().accept_loop_sedes());

        async fn enviar(addr: SocketAddr, msg: &MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            let payload = serde_json::to_string(msg).unwrap() + "\n";
            writer.write_all(payload.as_bytes()).await.unwrap();
            let line = lines.next_line().await.unwrap().unwrap();
            serde_json::from_str(&line).unwrap()
        }

        // 1. PreAutorizarPago
        let preauth_msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "e2e-pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };
        let r1 = enviar(addr, &preauth_msg).await;
        let pago_id_1 = match &r1 {
            MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "e2e-pr-1");
                pago_id.clone()
            }
            other => panic!("se esperaba PagoPreAutorizado, se obtuvo {:?}", other),
        };

        // 2. CobrarPago
        let cobrar_msg = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "e2e-pr-2".to_string(),
            alquiler_id: "alq-e2e-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 250.0,
        };
        let r2 = enviar(addr, &cobrar_msg).await;
        assert!(matches!(r2, MsgProcesadorPagosASede::PagoAceptado { .. }));

        // 3. ReembolsarPago
        let reembolsar_msg = MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "e2e-pr-3".to_string(),
            alquiler_id: "alq-e2e-1".to_string(),
            cliente_id: "c-1".to_string(),
            pago_original_id: "e2e-pr-2".to_string(),
            monto_reembolso: 20.0,
        };
        let r3 = enviar(addr, &reembolsar_msg).await;
        assert!(matches!(r3, MsgProcesadorPagosASede::PagoReembolsado { .. }));

        // 4. Reenvio del primer PreAutorizarPago: respuesta identica, sin duplicar.
        let r1_retry = enviar(addr, &preauth_msg).await;
        match r1_retry {
            MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "e2e-pr-1");
                assert_eq!(pago_id, pago_id_1);
            }
            other => panic!("se esperaba PagoPreAutorizado identico, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 3);
    }
}
