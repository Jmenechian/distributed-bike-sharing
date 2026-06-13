use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use matenmutex::log;
use matenmutex::messages::procesador_procesador_pagos::MsgProcesadorAProcesador;
use matenmutex::messages::procesadores::MsgProcesador;
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::types::{EstadoPago, EstadoTransaccionPago, PagoProcesado, TipoOperacionPago, TransaccionPago2PC, Voto2PC};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::interval;
use uuid::Uuid;
use matenmutex::utils::tiempos::{TiemposBuilder, Tiempos};
const CONFIG_FILE: usize = 1;
const MINIMAL_LENGTH: usize = 2;

const PUERTO_BASE: u16 = 6000;
const PUERTO_BASE_2PC: u16 = PUERTO_BASE + 1000;

fn id_de_addr(addr: &SocketAddr) -> u32 {
    (addr.port() - PUERTO_BASE) as u32
}

fn addr_de_id(id: u32, ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, PUERTO_BASE + id as u16)
}

/// Direccion del listener TCP dedicado a 2PC entre procesadores: PUERTO_BASE + 1000 + id.
fn addr_2pc_de_id(id: u32, ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, PUERTO_BASE_2PC + id as u16)
}

/// Extrae el monto relevante de un mensaje de Sede a ProcesadorPagos,
/// segun la operacion (monto_seguridad/monto_final/monto_reembolso).
fn monto_de(msg: &MsgSedeAProcesadorPagos) -> f64 {
    match msg {
        MsgSedeAProcesadorPagos::PreAutorizarPago { monto_seguridad, .. } => *monto_seguridad,
        MsgSedeAProcesadorPagos::CobrarPago { monto_final, .. } => *monto_final,
        MsgSedeAProcesadorPagos::ReembolsarPago { monto_reembolso, .. } => *monto_reembolso,
    }
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

/// Snapshot del estado durable de un nodo, para recuperarse tras un crash.
#[derive(Serialize, Deserialize, Default)]
struct EstadoPersistente {
    pagos_procesados: Vec<PagoProcesado>,
    transacciones_en_curso: Vec<TransaccionPago2PC>,
    preparados_replica: HashMap<String, MsgSedeAProcesadorPagos>,
}

/// Path del archivo de estado de un nodo, derivado de su id.
fn estado_path_de_id(procesador_id: u32) -> String {
    format!("estado_procesador_{}.json", procesador_id)
}

/// Lee el snapshot de disco. Si el archivo no existe, arranca con estado vacio (primer boot).
/// Cualquier otra falla (no se puede abrir, o existe pero esta corrupto) corta el arranque:
/// arrancar vacio pisaria el estado durable en el primer persistir().
fn cargar_estado(path: &str) -> Result<EstadoPersistente, Box<dyn std::error::Error>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(EstadoPersistente::default()),
        Err(e) => return Err(format!("no se pudo abrir el estado en {path}: {e}").into()),
    };
    serde_json::from_reader(BufReader::new(file))
        .map_err(|e| format!("estado corrupto en {path}: {e}").into())
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

/// Sincroniza mapa con objetivos (agrega los nuevos, saca los viejos) y devuelve que cambio entre vivo y caido.
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

    /// Vuelca el estado durable a disco de forma atomica (escribe a un .tmp y renombra).
    /// Los errores se loguean: una falla de persistencia no debe tumbar al nodo.
    fn persistir(&self) {
        if self.estado_path.is_empty() {
            return;
        }
        let snapshot = EstadoPersistente {
            pagos_procesados: self.pagos_procesados.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            transacciones_en_curso: self.transacciones_en_curso.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            preparados_replica: self.preparados_replica.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        };
        let json = match serde_json::to_vec_pretty(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                log!("[persistencia] no se pudo serializar el estado: {e}");
                return;
            }
        };
        let tmp = format!("{}.tmp", self.estado_path);
        if let Err(e) = std::fs::write(&tmp, &json).and_then(|_| std::fs::rename(&tmp, &self.estado_path)) {
            log!("[persistencia] no se pudo escribir {}: {e}", self.estado_path);
        }
    }

    async fn init(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        matenmutex::utils::logger::init(&format!("procesador{}", self.procesador_id));
        tokio::spawn(self.clone().heartbeat_loop());
        tokio::spawn(self.clone().monitor_loop());
        tokio::spawn(self.clone().accept_loop_sedes());
        tokio::spawn(self.clone().accept_loop_2pc());
        self.recv_loop().await
    }

    async fn accept_loop_sedes(self: Arc<Self>) {
        loop {
            match self.public_listener.accept().await {
                Ok((socket, addr)) => {
                    log!("[NODO {}]: Nueva conexion de Sede desde {}", self.procesador_id, addr);
                    tokio::spawn(self.clone().atender_sede(socket));
                }
                Err(e) => log!("[{}]: error aceptando sede: {}", self.etiqueta(), e),
            }
        }
    }

    async fn atender_sede(self: Arc<Self>, socket: TcpStream) {
        let (reader, mut writer) = socket.into_split();
        let mut lines = TokioBufReader::new(reader).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let resp = match serde_json::from_str::<MsgSedeAProcesadorPagos>(line.trim()) {
                Ok(msg) => {
                    log!("[procesador_pagos] Recibe {}", msg.describir());
                    self.procesar_pago(msg).await
                }
                Err(_) => {
                    log!("[procesador_pagos] Recibe mensaje invalido: {}", line.trim());
                    MsgProcesadorPagosASede::PagoRechazado {
                        payment_request_id: String::new(),
                        motivo: format!("mensaje invalido: {}", line.trim()),
                    }
                }
            };
            log!("[procesador_pagos] Envia {}", resp.describir());
            let payload = match serde_json::to_string(&resp) {
                Ok(json) => json + "\n",
                Err(_) => continue,
            };
            if writer.write_all(payload.as_bytes()).await.is_err() {
                break;
            }
        }
    }

    /// Procesa un pago de forma idempotente. La verificacion de idempotencia
    /// toma y libera el lock de pagos_procesados antes de cualquier await.
    /// Si el nodo es lider y tiene replicas configuradas, corre una ronda 2PC
    /// real antes de persistir (coordinar_2pc_como_lider). En caso contrario
    /// sigue el camino de commit local sincronico.
    async fn procesar_pago(self: &Arc<Self>, msg: MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
        let payment_request_id = msg.payment_request_id().to_string();
        {
            let pagos = match self.pagos_procesados.lock() {
                Ok(guard) => guard,
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
        }

        match self.rol() {
            Some(RolActual::Lider) if !self.peers.is_empty() => self.coordinar_2pc_como_lider(msg).await,
            Some(RolActual::Lider) => self.procesar_pago_local(&msg), // Caso lider sin replicas configuradas.
            _ => MsgProcesadorPagosASede::NoSoyLider { lider_addr: None },
        }
    }

    /// Commit local sincronico, sin 2PC. La idempotencia ya fue verificada por el caller.
    /// Toma y libera el lock de pagos_procesados sin awaits.
    fn procesar_pago_local(&self, msg: &MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
        let mut pagos = match self.pagos_procesados.lock() {
            Ok(g) => g,
            Err(_) => {
                return MsgProcesadorPagosASede::PagoRechazado {
                    payment_request_id: msg.payment_request_id().to_string(),
                    motivo: "estado de pagos no disponible".into(),
                }
            }
        };

        if let Some(pago) = pagos.iter().find(|p| p.payment_request_id == msg.payment_request_id()) {
            return respuesta_desde_pago(pago);
        }

        let (pago, respuesta) = self.decidir_operacion(msg);
        pagos.push(pago);
        drop(pagos);
        self.persistir();
        respuesta
    }

    /// Orquesta una ronda 2PC con las replicas configuradas (peers) antes de persistir el pago.
    /// El lider primero calcula su propio voto local, si es Abort, corta sin tocar la red ni
    /// transacciones_en_curso (P1).
    /// Si es Commit, arma una TransaccionPago2PC (participantes = ids de las replicas, sin incluir al lider),
    /// envia PrepararPago a cada replica con timeout (espera_ok), aplica los eventos resultantes
    /// via aplicar_evento_2pc, y al llegar a un estado terminal hace broadcast de
    /// CommitPago/AbortPago (esperando AckDecisionPago de forma best-effort).
    async fn coordinar_2pc_como_lider(self: &Arc<Self>, msg: MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
        let payment_request_id = msg.payment_request_id().to_string();

        // El lider calcula su propio voto local. Si es Abort, corta aca: no se crea TransaccionPago2PC, no se toca la red.
        if self.monto_excede_limite(monto_de(&msg)) {
            let (pago, respuesta) = self.decidir_operacion(&msg);
            if let Ok(mut pagos) = self.pagos_procesados.lock() {
                pagos.push(pago);
            }
            self.persistir();
            return respuesta;
        }

        // Solo coordinamos con las replicas vivas: una replica caida no debe bloquear el pago.
        let replicas = self.peers_vivos();
        if replicas.is_empty() {
            return self.procesar_pago_local(&msg);
        }

        // Arma la transaccion con los ids de las replicas vivas como participantes (el lider NUNCA se incluye a si mismo).
        let participantes: Vec<u32> = replicas.iter().map(id_de_addr).collect();
        let (pago_provisorio, _) = self.decidir_operacion(&msg);
        {
            let mut transacciones = match self.transacciones_en_curso.lock() {
                Ok(g) => g,
                Err(_) => {
                    return MsgProcesadorPagosASede::PagoRechazado {
                        payment_request_id,
                        motivo: "estado de transacciones no disponible".into(),
                    }
                }
            };
            transacciones.push(TransaccionPago2PC::nueva(payment_request_id.clone(), pago_provisorio, participantes.clone()));
        }

        self.enviar_preparar_pago_a_replicas(&replicas, &msg, &payment_request_id).await;

        let estado_final = self.obtener_estado_final(&payment_request_id);

        // broadcast de la decision a los demas y los acks se esperan best-effort.
        self.broadcast_de_decision(&replicas, &payment_request_id, estado_final).await;

        // Persiste si Commit, o devuelve el rechazo si Abort.
        let respuesta = if estado_final == EstadoTransaccionPago::Commit {
            let (pago, respuesta) = self.decidir_operacion(&msg);
            if let Ok(mut pagos) = self.pagos_procesados.lock() {
                pagos.push(pago);
            }
            respuesta
        } else {
            MsgProcesadorPagosASede::PagoRechazado {
                payment_request_id: payment_request_id.clone(),
                motivo: "transaccion 2PC abortada por las replicas".to_string(),
            }
        };

        // Limpia la transaccion en curso.
        if let Ok(mut transacciones) = self.transacciones_en_curso.lock() {
            transacciones.retain(|t| t.payment_request_id != payment_request_id);
        }

        self.persistir();
        respuesta
    }

    async fn broadcast_de_decision(self: &Arc<Self>, replicas: &[SocketAddr], payment_request_id: &String, estado_final: EstadoTransaccionPago) {
        let decision = match estado_final {
            EstadoTransaccionPago::Commit => MsgProcesadorAProcesador::CommitPago { payment_request_id: payment_request_id.clone() },
            _ => MsgProcesadorAProcesador::AbortPago { payment_request_id: payment_request_id.clone() },
        };
        for peer in replicas {
            let peer_id = id_de_addr(peer);
            let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
            let _ = self.solicitar_2pc(addr_2pc, &decision).await;
        }
    }

    // Lee y devuelve el estado final alcanzado.
    fn obtener_estado_final(self: &Arc<Self>, payment_request_id: &String) -> EstadoTransaccionPago {
        let estado_final = match self.transacciones_en_curso.lock() {
            Ok(transacciones) => transacciones
                .iter()
                .find(|t| t.payment_request_id == *payment_request_id)
                .map(|t| t.estado),
            Err(_) => None,
        };
        let estado_final = estado_final.unwrap_or(EstadoTransaccionPago::Abort);
        estado_final
    }

    // Envia PrepararPago a cada replica, con timeout, aplicando el evento resultante (voto o timeout) sobre la transaccion.
    async fn enviar_preparar_pago_a_replicas(self: &Arc<Self>, replicas: &[SocketAddr], msg: &MsgSedeAProcesadorPagos, payment_request_id: &String) {
        for peer in replicas {
            let peer_id = id_de_addr(peer);
            let preparar = MsgProcesadorAProcesador::PrepararPago {
                payment_request_id: payment_request_id.clone(),
                pago: msg.clone(),
            };
            let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
            let respuesta = self.solicitar_2pc(addr_2pc, &preparar).await;

            let evento = match respuesta {
                Some(MsgProcesadorAProcesador::VotoPago { votante_id, voto, .. }) => {
                    EventoVoto2PC::Voto { votante_id, voto }
                }
                _ => EventoVoto2PC::Timeout,
            };

            if let Ok(mut transacciones) = self.transacciones_en_curso.lock() {
                if let Some(tx) = transacciones.iter_mut().find(|t| t.payment_request_id == *payment_request_id) {
                    aplicar_evento_2pc(tx, evento);
                }
            }
        }
    }

    /// Una ida y vuelta TCP de 2PC: abre una conexion nueva, envia msg como
    /// JSON con salto de linea, y espera una respuesta JSON tambien con salto
    /// de linea, acotada por self.tiempos.espera_ok. Devuelve None ante
    /// fallo de conexion, envio, timeout o deserializacion.
    async fn solicitar_2pc(&self, addr: SocketAddr, msg: &MsgProcesadorAProcesador) -> Option<MsgProcesadorAProcesador> {
        log!("[{}]: 2PC envia {} a {}", self.etiqueta(), msg.describir(), addr);

        let respuesta: Option<MsgProcesadorAProcesador> = tokio::time::timeout(self.tiempos.espera_ok, async {
            let stream = TcpStream::connect(addr).await.ok()?;
            let (reader, mut writer) = stream.into_split();
            let payload = serde_json::to_string(msg).ok()? + "\n";
            writer.write_all(payload.as_bytes()).await.ok()?;

            let mut lines = TokioBufReader::new(reader).lines();
            let line = lines.next_line().await.ok()??;
            serde_json::from_str(line.trim()).ok()
        })
        .await
        .ok()
        .flatten();

        match &respuesta {
            Some(resp) => log!("[{}]: 2PC recibe {} de {}", self.etiqueta(), resp.describir(), addr),
            None => log!("[{}]: 2PC sin respuesta de {} (timeout o conexion fallida)", self.etiqueta(), addr),
        }

        respuesta
    }

    /// Acepta conexiones de 2PC entrantes (otros procesadores) y delega cada
    /// una a atender_procesador. Mismo patron que accept_loop_sedes.
    async fn accept_loop_2pc(self: Arc<Self>) {
        loop {
            match self.bus_2pc.accept().await {
                Ok((socket, addr)) => {
                    log!("[{}]: Nueva conexion 2PC desde {}", self.etiqueta(), addr);
                    tokio::spawn(self.clone().atender_procesador(socket));
                }
                Err(e) => log!("[{}]: error aceptando conexion 2PC: {}", self.etiqueta(), e),
            }
        }
    }

    /// Lee mensajes 2PC linea por linea de socket, los despacha a
    /// manejar_mensaje_procesador_a_procesador y escribe la respuesta.
    /// Mismo patron que atender_sede.
    async fn atender_procesador(self: Arc<Self>, socket: TcpStream) {
        let (reader, mut writer) = socket.into_split();
        let mut lines = TokioBufReader::new(reader).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let resp = match serde_json::from_str::<MsgProcesadorAProcesador>(line.trim()) {
                Ok(msg) => {
                    log!("[{}]: 2PC recibe {}", self.etiqueta(), msg.describir());
                    self.manejar_mensaje_procesador_a_procesador(msg)
                }
                Err(_) => {
                    log!("[{}]: 2PC recibe mensaje invalido: {}", self.etiqueta(), line.trim());
                    None
                }
            };

            let Some(resp) = resp else { continue };
            log!("[{}]: 2PC envia {}", self.etiqueta(), resp.describir());
            let payload = match serde_json::to_string(&resp) {
                Ok(json) => json + "\n",
                Err(_) => continue,
            };
            if writer.write_all(payload.as_bytes()).await.is_err() {
                break;
            }
        }
    }

    /// Logica de la replica para los mensajes 2PC del lider. PrepararPago
    /// evalua el voto local SIN mutar pagos_procesados. CommitPago/AbortPago
    /// aplican (idempotentemente) o descartan la operacion. Otras variantes
    /// (recibidas por una replica) quedan fuera de alcance de 3b.
    fn manejar_mensaje_procesador_a_procesador(&self, msg: MsgProcesadorAProcesador) -> Option<MsgProcesadorAProcesador> {
        match msg {
            MsgProcesadorAProcesador::PrepararPago { payment_request_id, pago } => {
                let voto = if self.monto_excede_limite(monto_de(&pago)) {
                    Voto2PC::Abort
                } else {
                    Voto2PC::Commit
                };
                if let Ok(mut preparados) = self.preparados_replica.lock() {
                    preparados.insert(payment_request_id.clone(), pago);
                }
                self.persistir();
                Some(MsgProcesadorAProcesador::VotoPago {
                    payment_request_id,
                    votante_id: self.procesador_id,
                    voto,
                })
            }
            MsgProcesadorAProcesador::CommitPago { payment_request_id } => {
                self.persistir_commit_replica(&payment_request_id);
                self.persistir();
                Some(MsgProcesadorAProcesador::AckDecisionPago {
                    payment_request_id,
                    votante_id: self.procesador_id,
                })
            }
            MsgProcesadorAProcesador::AbortPago { payment_request_id } => {
                if let Ok(mut preparados) = self.preparados_replica.lock() {
                    preparados.remove(&payment_request_id);
                }
                self.persistir();
                Some(MsgProcesadorAProcesador::AckDecisionPago {
                    payment_request_id,
                    votante_id: self.procesador_id,
                })
            }
            MsgProcesadorAProcesador::ConsultarPago { payment_request_id } => {
                let pago = match self.pagos_procesados.lock() {
                    Ok(pagos) => pagos.iter().find(|p| p.payment_request_id == payment_request_id).cloned(),
                    Err(_) => None,
                };
                Some(MsgProcesadorAProcesador::RespuestaConsultaPago { payment_request_id, pago })
            }
            _ => None,
        }
    }

    /// Aplica el Commit de payment_request_id: la saca de preparados_replica
    /// y, si todavia no esta en pagos_procesados, la agrega (idempotente).
    fn persistir_commit_replica(&self, payment_request_id: &str) {
        let pendiente = match self.preparados_replica.lock() {
            Ok(mut preparados) => preparados.remove(payment_request_id),
            Err(_) => None,
        };
        if let Ok(mut pagos) = self.pagos_procesados.lock() {
            if !pagos.iter().any(|p| p.payment_request_id == payment_request_id) {
                let pago = match &pendiente {
                    Some(operacion) => self.decidir_operacion(operacion).0,
                    None => PagoProcesado {
                        payment_request_id: payment_request_id.to_string(),
                        pago_id: Uuid::new_v4().to_string(),
                        alquiler_id: None,
                        cliente_id: String::new(),
                        monto: 0.0,
                        tipo: TipoOperacionPago::CobroFinal,
                        estado: EstadoPago::Aceptado,
                    },
                };
                pagos.push(pago);
            }
        }
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

    /// Arma el PagoProcesado que se va a guardar y la respuesta para la sede,
    /// decidiendo si se acepta o rechaza segun monto_excede_limite.
    /// La usan las 3 operaciones de pago.
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
                log!("[NODO {}]: Mutex de rol_actual envenenado: {}", self.procesador_id, e);
                None
            }
        }
    }

    fn set_rol(&self, nuevo: RolActual) {
        match self.rol_actual.lock() {
            Ok(mut rol) => *rol = nuevo,
            Err(e) => log!("[NODO {}]: Mutex de rol_actual envenenado: {}", self.procesador_id, e),
        }
    }

    /// Red de seguridad contra split-brain: si un heartbeat anuncia un lider de id
    /// mayor al que reconozco, me subordino como replica. Cubre el caso del ex-lider
    /// que revive con el rol del config y se cree lider cuando ya hay otro.
    fn reconciliar_con_lider_por_heartbeat(&self, lider_emisor: u32) {
        let mut rol = match self.rol_actual.lock() {
            Ok(rol) => rol,
            Err(e) => {
                log!("[NODO {}]: Mutex de rol_actual envenenado: {}", self.procesador_id, e);
                return;
            }
        };
        let debe_ser_replica = match *rol {
            RolActual::Lider => lider_emisor > self.procesador_id,
            RolActual::Replica { lider_id } => lider_emisor > lider_id,
            RolActual::Candidato => false,
        };
        if debe_ser_replica {
            *rol = RolActual::Replica { lider_id: lider_emisor };
            drop(rol); //
            log!("[NODO {}]: heartbeat de lider {} con id mayor: me subordino como REPLICA", self.procesador_id, lider_emisor);
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
                log!("[NODO {}]: Mutex de rol_actual envenenado: {}", self.procesador_id, e);
                false
            }
        }
    }

    fn recibio_ok(&self) -> bool {
        match self.recibio_ok.lock() {
            Ok(flag) => *flag,
            Err(e) => {
                log!("[NODO {}]: Mutex de recibio_ok envenenado: {}", self.procesador_id, e);
                false
            }
        }
    }

    fn set_recibio_ok(&self, valor: bool) {
        match self.recibio_ok.lock() {
            Ok(mut flag) => *flag = valor,
            Err(e) => log!("[NODO {}]: Mutex de recibio_ok envenenado: {}", self.procesador_id, e),
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
            let es_lider = matches!(self.rol(), Some(RolActual::Lider));
            let msg = MsgProcesador::Heartbeat { procesador_id: self.procesador_id, es_lider };
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
                        log!(
                            "[{}]: Procesador en {} se considera CAIDO (sin noticias hace mas de {}s)",
                            etiqueta, addr, self.tiempos.liveness_timeout.as_secs()
                        );
                        Self::handlear_caida(&self, &etiqueta, addr);
                    }
                    EstadoNodo::Vivo => log!(
                        "[{}]: Procesador en {} volvio a estar VIVO",
                        etiqueta, addr
                    ),
                }
            }
        }
    }

    /// Replicas configuradas que el liveness del Bully considera vivas ahora mismo.
    /// Si una replica nunca aparecio en el mapa (todavia no hubo heartbeat), se la
    /// considera viva por defecto (optimista).
    fn peers_vivos(&self) -> Vec<SocketAddr> {
        let mapa = match self.liveness.lock() {
            Ok(mapa) => mapa,
            Err(_) => return self.peers.clone(),
        };
        self.peers
            .iter()
            .copied()
            .filter(|addr| mapa.get(addr).map_or(true, |info| info.estado == EstadoNodo::Vivo))
            .collect()
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
                log!("[{}]: Mutex de liveness envenenado: {}", etiqueta, e);
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
                log!("[{}]: Mutex de liveness envenenado: {}", etiqueta, e);
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
            log!("[{}]: El lider {} cayo", etiqueta, id_caido);
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
                            log!("[{}]: Mensaje invalido de {}: {}", self.etiqueta(), from, e);
                        }
                    }
                }
                Err(e) => {
                    log!("[{}]: Fallo al recibir datagrama: {}", self.etiqueta(), e);
                }
            }
        }
    }

    /// Maneja los mensajes recibidos y actua en base a ellos. Siempre que se recibe un
    /// mensaje se refresca el liveness de un nodo
    async fn handle_msg(nodo: &Arc<Self>, from: SocketAddr, msg: MsgProcesador) {
        let etiqueta = nodo.etiqueta();
        log!("[{}]: Mensaje recibido de {}: {:?}", etiqueta, from, msg);

        nodo.actualizar_liveness(&etiqueta, from);

        match msg {
            MsgProcesador::Heartbeat { procesador_id: emisor_id, es_lider } => {
                if es_lider {
                    nodo.reconciliar_con_lider_por_heartbeat(emisor_id);
                }
                nodo.enviar_heartbeat_ack(from).await;
            }
            MsgProcesador::HeartbeatAck { procesador_id: emisor_id } => {
                log!(
                    "[{}]: HeartbeatAck recibido del procesador {} ({})",
                    etiqueta, emisor_id, from
                );
            }
            MsgProcesador::Eleccion { .. } => {
                Self::handlear_eleccion(nodo, from).await;
            }
            MsgProcesador::Ok { procesador_id: emisor_id } => {
                log!("[{}]: Ok de {} ({}): hay un nodo mayor vivo", etiqueta, emisor_id, from);
                nodo.set_recibio_ok(true);
            }
            MsgProcesador::Coordinador { lider_id } => {
                log!("[{}]: Coordinador: el nuevo lider es {} ({})", etiqueta, lider_id, from);
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
            log!("[{}]: Inicio una eleccion (bully)", nodo.etiqueta());
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
            log!("[{}]: No llego Coordinador a tiempo, reinicio la eleccion", self.etiqueta());
        }
    }

    /// Se proclama líder y lo difunde con Coordinador.
    async fn proclamarse_lider(&self) {
        self.set_rol(RolActual::Lider);
        log!("[{}]: Gane la eleccion: me proclamo LIDER", self.etiqueta());
        let coordinador = MsgProcesador::Coordinador { lider_id: self.procesador_id };
        self.enviar_a(&self.peers, &coordinador).await;
        self.recuperar_transacciones_huerfanas().await;
    }

    /// Recorre las transacciones huerfanas en preparados_replica (recibidas
    /// como PrepararPago pero sin decision, porque el lider anterior murio en
    /// medio del 2PC) y las resuelve: Commit si self o algun peer vivo ya
    /// tiene el pago en pagos_procesados, Abort en caso contrario
    /// (abort-on-doubt). Difunde la decision resultante a los peers vivos.
    async fn recuperar_transacciones_huerfanas(&self) {
        let ids: Vec<String> = match self.preparados_replica.lock() {
            Ok(preparados) => preparados.keys().cloned().collect(),
            Err(_) => Vec::new(),
        };

        for payment_request_id in ids {
            let mut hay_commit = match self.pagos_procesados.lock() {
                Ok(pagos) => pagos.iter().any(|p| p.payment_request_id == payment_request_id),
                Err(_) => false,
            };

            let consulta = MsgProcesadorAProcesador::ConsultarPago { payment_request_id: payment_request_id.clone() };
            for peer in self.peers_vivos() {
                let peer_id = id_de_addr(&peer);
                let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
                if let Some(MsgProcesadorAProcesador::RespuestaConsultaPago { pago: Some(_), .. }) = self.solicitar_2pc(addr_2pc, &consulta).await {
                    hay_commit = true;
                }
            }

            if hay_commit {
                self.broadcast_decision_recuperacion(&payment_request_id, EstadoTransaccionPago::Commit).await;
                self.persistir_commit_replica(&payment_request_id);
            } else {
                self.broadcast_decision_recuperacion(&payment_request_id, EstadoTransaccionPago::Abort).await;
                if let Ok(mut preparados) = self.preparados_replica.lock() {
                    preparados.remove(&payment_request_id);
                }
            }
            self.persistir();
        }
    }

    /// Difunde la decision (CommitPago/AbortPago) de una transaccion recuperada a todos los peers vivos.
    async fn broadcast_decision_recuperacion(&self, payment_request_id: &str, estado_final: EstadoTransaccionPago) {
        let decision = match estado_final {
            EstadoTransaccionPago::Commit => MsgProcesadorAProcesador::CommitPago { payment_request_id: payment_request_id.to_string() },
            _ => MsgProcesadorAProcesador::AbortPago { payment_request_id: payment_request_id.to_string() },
        };
        for peer in self.peers_vivos() {
            let peer_id = id_de_addr(&peer);
            let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
            let _ = self.solicitar_2pc(addr_2pc, &decision).await;
        }
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
                log!("[{}]: Error serializando {:?}: {}", etiqueta, msg, e);
                return;
            }
        };
        for destino in destinos {
            match self.socket.send_to(&payload, destino).await {
                Ok(_) => log!("[{}]: {:?} enviado a {}", etiqueta, msg, destino),
                Err(e) => log!("[{}]: Error enviando {:?} a {}: {}", etiqueta, msg, destino, e),
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

/// Evento que la maquina de estados 2PC del lider puede aplicar a una TransaccionPago2PC
/// es un voto recibido de un participante o un timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventoVoto2PC {
    Voto { votante_id: u32, voto: Voto2PC },
    Timeout,
}

/// Decide el EstadoTransaccionPago resultante en funcion de los votos acumulados,
/// los participantes esperados, y si se aplico un timeout.
fn decidir_estado_2pc(tx: &TransaccionPago2PC, timeout: bool) -> EstadoTransaccionPago {
    if tx.estado == EstadoTransaccionPago::Commit || tx.estado == EstadoTransaccionPago::Abort {
        return tx.estado;
    }

    if !tx.votos_abort.is_empty() {
        return EstadoTransaccionPago::Abort;
    }

    if timeout {
        return EstadoTransaccionPago::Abort;
    }

    let todos_votaron_commit = tx.participantes.iter().all(|p| tx.votos_commit.contains(p));
    if todos_votaron_commit && !tx.participantes.is_empty() {
        return EstadoTransaccionPago::Commit;
    }

    if tx.votos_commit.is_empty() {
        EstadoTransaccionPago::Preparando
    } else {
        EstadoTransaccionPago::Preparada
    }
}

/// Guarda el voto en tx y recalcula el estado con decidir_estado_2pc.
/// Si tx ya esta en Commit o Abort no hace nada.
/// Si el voto es de alguien que no esta en tx.participantes, se ignora.
fn aplicar_evento_2pc(tx: &mut TransaccionPago2PC, evento: EventoVoto2PC) {
    if tx.estado == EstadoTransaccionPago::Commit || tx.estado == EstadoTransaccionPago::Abort {
        return;
    }

    match evento {
        EventoVoto2PC::Voto { votante_id, voto } => {
            if !tx.participantes.contains(&votante_id) {
                return;
            }
            match voto {
                Voto2PC::Commit => {
                    if !tx.votos_commit.contains(&votante_id) {
                        tx.votos_commit.push(votante_id);
                    }
                }
                Voto2PC::Abort => {
                    if !tx.votos_abort.contains(&votante_id) {
                        tx.votos_abort.push(votante_id);
                    }
                }
            }
            tx.estado = decidir_estado_2pc(tx, false);
        }
        EventoVoto2PC::Timeout => {
            tx.estado = decidir_estado_2pc(tx, true);
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
        let nodo = nodo_de_prueba(10, 0, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        }).await;

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
    async fn replica_responde_no_soy_lider_y_no_persiste_el_pago() {
        let nodo = nodo_de_prueba(11, 4, RolActual::Replica { lider_id: 1 }).await;

        let respuesta = nodo
            .procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
                payment_request_id: "pr-replica".to_string(),
                cliente_id: "c-1".to_string(),
                monto_seguridad: 100.0,
            })
            .await;

        assert_eq!(respuesta, MsgProcesadorPagosASede::NoSoyLider { lider_addr: None });
        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn candidato_responde_no_soy_lider_y_no_persiste_el_pago() {
        let nodo = nodo_de_prueba(12, 4, RolActual::Candidato).await;

        let respuesta = nodo
            .procesar_pago(MsgSedeAProcesadorPagos::CobrarPago {
                payment_request_id: "pr-candidato".to_string(),
                alquiler_id: "alq-1".to_string(),
                cliente_id: "c-1".to_string(),
                monto_final: 50.0,
            })
            .await;

        assert_eq!(respuesta, MsgProcesadorPagosASede::NoSoyLider { lider_addr: None });
        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cobrar_pago_aceptado_persiste_y_responde() {
        let nodo = nodo_de_prueba(11, 0, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "pr-3".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 200.0,
        }).await;

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
        let nodo = nodo_de_prueba(12, 0, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "pr-5".to_string(),
            alquiler_id: "alq-3".to_string(),
            cliente_id: "c-3".to_string(),
            pago_original_id: "pago-orig-1".to_string(),
            monto_reembolso: 30.0,
        }).await;

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
        let nodo = nodo_de_prueba(13, 0, RolActual::Lider).await;

        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-7".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };

        let r1 = nodo.procesar_pago(msg.clone()).await;
        let r2 = nodo.procesar_pago(msg).await;

        assert_eq!(r1, r2);
        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
    }

    #[tokio::test]
    async fn monto_que_excede_el_limite_es_rechazado() {
        let nodo = nodo_de_prueba_con_limite(14, 0, RolActual::Lider, Some(50.0)).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-10".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        }).await;

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
        let nodo = nodo_de_prueba_con_limite(15, 0, RolActual::Lider, Some(50.0)).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-11".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 50.0,
        }).await;

        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
    }

    // ---- Pagos: integracion sobre el accept loop TCP ----

    #[tokio::test(flavor = "multi_thread")]
    async fn accept_loop_responde_pre_autorizar_pago() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let nodo = nodo_de_prueba(16, 0, RolActual::Lider).await;
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

        let nodo = nodo_de_prueba(17, 0, RolActual::Lider).await;
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

        let nodo = nodo_de_prueba(18, 0, RolActual::Lider).await;
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

    fn pago_de_prueba(payment_request_id: &str) -> PagoProcesado {
        PagoProcesado {
            payment_request_id: payment_request_id.to_string(),
            pago_id: "pago-2pc".to_string(),
            alquiler_id: None,
            cliente_id: "c-2pc".to_string(),
            monto: 100.0,
            tipo: TipoOperacionPago::CobroFinal,
            estado: EstadoPago::Aceptado,
        }
    }

    #[tokio::test]
    async fn nodo_de_prueba_arranca_con_transacciones_en_curso_vacio() {
        let nodo = nodo_de_prueba(20, 0, RolActual::Lider).await;
        let transacciones = nodo.transacciones_en_curso.lock().unwrap();
        assert!(transacciones.is_empty());
    }

    #[test]
    fn voto_unico_commit_decide_commit() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-1".to_string(), pago_de_prueba("pr-sm-1"), vec![1]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });

        assert_eq!(tx.votos_commit, vec![1]);
        assert!(tx.votos_abort.is_empty());
        assert_eq!(tx.estado, EstadoTransaccionPago::Commit);
    }

    #[test]
    fn un_abort_entre_varios_decide_abort_y_es_terminal() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-2".to_string(), pago_de_prueba("pr-sm-2"), vec![1, 2, 3]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });
        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 2, voto: Voto2PC::Abort });

        assert_eq!(tx.votos_commit, vec![1]);
        assert_eq!(tx.votos_abort, vec![2]);
        assert_eq!(tx.estado, EstadoTransaccionPago::Abort);

        // El estado abort es terminal: votos posteriores no lo cambian.
        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 3, voto: Voto2PC::Commit });
        assert_eq!(tx.estado, EstadoTransaccionPago::Abort);
        assert!(!tx.votos_commit.contains(&3));
    }

    #[test]
    fn dos_participantes_unanime_commit_decide_commit() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-3".to_string(), pago_de_prueba("pr-sm-3"), vec![1, 2]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });
        assert_eq!(tx.estado, EstadoTransaccionPago::Preparada);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 2, voto: Voto2PC::Commit });
        assert_eq!(tx.votos_commit, vec![1, 2]);
        assert_eq!(tx.estado, EstadoTransaccionPago::Commit);
    }

    #[test]
    fn timeout_antes_de_recibir_todos_los_votos_decide_abort() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-4".to_string(), pago_de_prueba("pr-sm-4"), vec![1, 2]);
        tx.votos_commit.push(1);
        tx.estado = EstadoTransaccionPago::Preparada;

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Timeout);

        assert_eq!(tx.estado, EstadoTransaccionPago::Abort);
    }

    #[test]
    fn timeout_sobre_transaccion_ya_decidida_es_no_op() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-5".to_string(), pago_de_prueba("pr-sm-5"), vec![1, 2]);
        tx.votos_commit = vec![1, 2];
        tx.estado = EstadoTransaccionPago::Commit;

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Timeout);

        assert_eq!(tx.estado, EstadoTransaccionPago::Commit);
    }

    #[test]
    fn voto_duplicado_del_mismo_participante_es_idempotente() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-6".to_string(), pago_de_prueba("pr-sm-6"), vec![1, 2]);
        tx.votos_commit.push(1);
        tx.estado = EstadoTransaccionPago::Preparada;

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });

        assert_eq!(tx.votos_commit, vec![1]);
        assert_eq!(tx.estado, EstadoTransaccionPago::Preparada);
    }

    #[test]
    fn voto_de_participante_desconocido_se_ignora() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-7".to_string(), pago_de_prueba("pr-sm-7"), vec![1, 2]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 99, voto: Voto2PC::Commit });

        assert!(tx.votos_commit.is_empty());
        assert!(tx.votos_abort.is_empty());
        assert_eq!(tx.estado, EstadoTransaccionPago::Preparando);
    }

    // ---- 3b: transporte 2PC (addr_2pc_de_id, monto_de, bus_2pc) ----

    #[test]
    fn addr_2pc_de_id_aplica_offset_de_1000_sobre_puerto_base() {
        for id in 1..=10 {
            let esperado = SocketAddr::new(ip(), PUERTO_BASE + 1000 + id as u16);
            assert_eq!(addr_2pc_de_id(id, ip()), esperado);
            // No colisiona con el rango de addr_de_id para ids razonables.
            assert!(addr_2pc_de_id(id, ip()).port() > addr_de_id(id, ip()).port());
        }
    }

    #[test]
    fn monto_de_extrae_el_monto_segun_la_variante() {
        let pre = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 11.0,
        };
        let cobro = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "pr-2".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 22.0,
        };
        let reembolso = MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "pr-3".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            pago_original_id: "pago-1".to_string(),
            monto_reembolso: 33.0,
        };

        assert_eq!(monto_de(&pre), 11.0);
        assert_eq!(monto_de(&cobro), 22.0);
        assert_eq!(monto_de(&reembolso), 33.0);
    }

    #[tokio::test]
    async fn nodo_de_prueba_tiene_bus_2pc_bindeado() {
        let nodo = nodo_de_prueba(21, 0, RolActual::Lider).await;
        assert!(nodo.bus_2pc.local_addr().is_ok());
    }

    // ---- 3b: replica handler (manejar_mensaje_procesador_a_procesador) ----

    fn cobrar_msg(payment_request_id: &str, monto: f64) -> MsgSedeAProcesadorPagos {
        MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: payment_request_id.to_string(),
            alquiler_id: "alq-2pc".to_string(),
            cliente_id: "c-2pc".to_string(),
            monto_final: monto,
        }
    }

    #[tokio::test]
    async fn replica_vota_commit_si_el_monto_no_excede_su_limite() {
        let nodo = nodo_de_prueba_con_limite(30, 0, RolActual::Replica { lider_id: 1 }, Some(1000.0)).await;

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-1".to_string(),
            pago: cobrar_msg("pr-rep-1", 100.0),
        });

        match resp {
            Some(MsgProcesadorAProcesador::VotoPago { payment_request_id, votante_id, voto }) => {
                assert_eq!(payment_request_id, "pr-rep-1");
                assert_eq!(votante_id, 30);
                assert_eq!(voto, Voto2PC::Commit);
            }
            other => panic!("se esperaba VotoPago{{Commit}}, se obtuvo {:?}", other),
        }

        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn replica_vota_abort_si_el_monto_excede_su_limite() {
        let nodo = nodo_de_prueba_con_limite(31, 0, RolActual::Replica { lider_id: 1 }, Some(50.0)).await;

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-2".to_string(),
            pago: cobrar_msg("pr-rep-2", 100.0),
        });

        match resp {
            Some(MsgProcesadorAProcesador::VotoPago { payment_request_id, votante_id, voto }) => {
                assert_eq!(payment_request_id, "pr-rep-2");
                assert_eq!(votante_id, 31);
                assert_eq!(voto, Voto2PC::Abort);
            }
            other => panic!("se esperaba VotoPago{{Abort}}, se obtuvo {:?}", other),
        }

        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn replica_aplica_commit_pago_y_persiste() {
        let nodo = nodo_de_prueba(32, 0, RolActual::Replica { lider_id: 1 }).await;

        let _ = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-3".to_string(),
            pago: cobrar_msg("pr-rep-3", 100.0),
        });

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-rep-3".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::AckDecisionPago { payment_request_id, votante_id }) => {
                assert_eq!(payment_request_id, "pr-rep-3");
                assert_eq!(votante_id, 32);
            }
            other => panic!("se esperaba AckDecisionPago, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        assert_eq!(pagos[0].payment_request_id, "pr-rep-3");
    }

    #[tokio::test]
    async fn replica_aplica_commit_pago_idempotentemente() {
        let nodo = nodo_de_prueba(33, 0, RolActual::Replica { lider_id: 1 }).await;

        let _ = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-4".to_string(),
            pago: cobrar_msg("pr-rep-4", 100.0),
        });

        let r1 = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-rep-4".to_string(),
        });
        let r2 = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-rep-4".to_string(),
        });

        assert!(matches!(r1, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));
        assert!(matches!(r2, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
    }

    #[tokio::test]
    async fn replica_descarta_abort_pago_y_no_persiste() {
        let nodo = nodo_de_prueba(34, 0, RolActual::Replica { lider_id: 1 }).await;

        let _ = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-5".to_string(),
            pago: cobrar_msg("pr-rep-5", 100.0),
        });

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::AbortPago {
            payment_request_id: "pr-rep-5".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::AckDecisionPago { payment_request_id, votante_id }) => {
                assert_eq!(payment_request_id, "pr-rep-5");
                assert_eq!(votante_id, 34);
            }
            other => panic!("se esperaba AckDecisionPago, se obtuvo {:?}", other),
        }

        assert!(!nodo.pagos_procesados.lock().unwrap().iter().any(|p| p.payment_request_id == "pr-rep-5"));
    }

    // ---- 3b: integracion 2PC end-to-end (lider + replicas reales en TCP) ----

    /// El lider direcciona PrepararPago/CommitPago/AbortPago a
    /// addr_2pc_de_id(peer_id, ip), pero en los nodos de prueba el bus_2pc esta
    /// bindeado a un puerto efimero. Para el test, sobreescribimos la
    /// resolucion usando un mapa id->bus_2pc real y un lider de prueba que
    /// redirige solicitar_2pc a esas direcciones reales en lugar de
    /// addr_2pc_de_id. Como solicitar_2pc es un metodo de instancia que arma
    /// la direccion a partir de self.peers + addr_2pc_de_id, en este test
    /// hacemos que self.peers contenga direcciones cuyo id_de_addr coincide
    /// con el id real de la replica, y bindeamos el bus_2pc de la replica
    /// exactamente en addr_2pc_de_id(id, ip) (igual que from_json).
    async fn nodo_2pc_de_prueba_en_addr_canonica(id: u32, rol: RolActual, peers: Vec<u32>, limite: Option<f64>) -> Arc<ProcesadorPagos> {
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

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_dos_replicas_que_votan_commit_persiste_y_responde_ok() {
        // ids altos para no colisionar con otros tests que usan addr_2pc_de_id.
        let lider = nodo_2pc_de_prueba_en_addr_canonica(140, RolActual::Lider, vec![141, 142], None).await;
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(141, RolActual::Replica { lider_id: 140 }, vec![], None).await;
        let _r2 = nodo_2pc_de_prueba_en_addr_canonica(142, RolActual::Replica { lider_id: 140 }, vec![], None).await;

        // Deja que los accept loops esten escuchando.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-commit", 100.0)).await;
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoAceptado { .. }), "se esperaba PagoAceptado, se obtuvo {:?}", respuesta);

        assert_eq!(lider.pagos_procesados.lock().unwrap().len(), 1);
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());

        // Espera a que las replicas reciban y apliquen el CommitPago.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(_r1.pagos_procesados.lock().unwrap().len(), 1);
        assert_eq!(_r2.pagos_procesados.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_una_replica_que_vota_abort_no_persiste_y_rechaza() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(150, RolActual::Lider, vec![151, 152], None).await;
        // r1 vota Commit (sin limite); r2 tiene un limite menor al monto -> Abort.
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(151, RolActual::Replica { lider_id: 150 }, vec![], None).await;
        let _r2 = nodo_2pc_de_prueba_en_addr_canonica(152, RolActual::Replica { lider_id: 150 }, vec![], Some(10.0)).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-abort", 100.0)).await;
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoRechazado { .. }), "se esperaba PagoRechazado, se obtuvo {:?}", respuesta);

        assert!(lider.pagos_procesados.lock().unwrap().is_empty());
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(_r1.pagos_procesados.lock().unwrap().is_empty());
        assert!(_r2.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_replica_inalcanzable_decide_abort_por_timeout() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(160, RolActual::Lider, vec![161, 162], None).await;
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(161, RolActual::Replica { lider_id: 160 }, vec![], None).await;
        // 162 no existe: nadie escucha en addr_2pc_de_id(162, ip).

        tokio::time::sleep(Duration::from_millis(50)).await;

        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-timeout", 100.0)).await;
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoRechazado { .. }), "se esperaba PagoRechazado, se obtuvo {:?}", respuesta);

        assert!(lider.pagos_procesados.lock().unwrap().is_empty());
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(_r1.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_lider_propio_voto_abort_corta_sin_red() {
        // El lider tiene un limite menor al monto: vota Abort localmente y
        // NUNCA debe enviar PrepararPago a las replicas (no hay tx en curso
        // ni se pierde tiempo en timeouts).
        let lider = nodo_2pc_de_prueba_en_addr_canonica(170, RolActual::Replica { lider_id: 0 }, vec![171], Some(10.0)).await;
        lider.set_rol(RolActual::Lider);
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(171, RolActual::Replica { lider_id: 170 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let inicio = Instant::now();
        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-self-abort", 100.0)).await;
        let transcurrido = inicio.elapsed();

        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoRechazado { .. }), "se esperaba PagoRechazado, se obtuvo {:?}", respuesta);
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());
        // No debe haber esperado ningun timeout de red (espera_ok = 1s en tiempos_test).
        assert!(transcurrido < Duration::from_millis(500), "tardo demasiado: {:?}", transcurrido);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(_r1.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_payment_request_id_repetido_no_repite_la_ronda() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(180, RolActual::Lider, vec![181], None).await;
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(181, RolActual::Replica { lider_id: 180 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let msg = cobrar_msg("pr-2pc-dup", 100.0);
        let r1 = lider.procesar_pago(msg.clone()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let r2 = lider.procesar_pago(msg).await;

        assert_eq!(r1, r2);
        assert_eq!(lider.pagos_procesados.lock().unwrap().len(), 1);
        assert_eq!(_r1.pagos_procesados.lock().unwrap().len(), 1);
    }

    // ---- 3c: ConsultarPago / recuperacion de transacciones huerfanas ----

    #[tokio::test]
    async fn consultar_pago_responde_some_si_esta_committeado() {
        let nodo = nodo_de_prueba(200, 0, RolActual::Replica { lider_id: 1 }).await;
        nodo.pagos_procesados.lock().unwrap().push(pago_de_prueba("pr-cons-1"));

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::ConsultarPago {
            payment_request_id: "pr-cons-1".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::RespuestaConsultaPago { payment_request_id, pago: Some(pago) }) => {
                assert_eq!(payment_request_id, "pr-cons-1");
                assert_eq!(pago.payment_request_id, "pr-cons-1");
            }
            otro => panic!("se esperaba RespuestaConsultaPago con Some, se obtuvo {:?}", otro),
        }
    }

    #[tokio::test]
    async fn consultar_pago_responde_none_si_no_esta_committeado() {
        let nodo = nodo_de_prueba(201, 0, RolActual::Replica { lider_id: 1 }).await;

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::ConsultarPago {
            payment_request_id: "pr-cons-2".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::RespuestaConsultaPago { payment_request_id, pago: None }) => {
                assert_eq!(payment_request_id, "pr-cons-2");
            }
            otro => panic!("se esperaba RespuestaConsultaPago con None, se obtuvo {:?}", otro),
        }
    }

    #[tokio::test]
    async fn persistir_commit_replica_es_idempotente_y_remueve_de_preparados() {
        let nodo = nodo_de_prueba(202, 0, RolActual::Replica { lider_id: 1 }).await;
        nodo.preparados_replica.lock().unwrap().insert("pr-commit-1".to_string(), cobrar_msg("pr-commit-1", 100.0));

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-commit-1".to_string(),
        });

        assert!(matches!(resp, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));
        assert_eq!(nodo.pagos_procesados.lock().unwrap().len(), 1);
        assert!(!nodo.preparados_replica.lock().unwrap().contains_key("pr-commit-1"));

        // Idempotencia: un segundo CommitPago no duplica el registro.
        let resp2 = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-commit-1".to_string(),
        });
        assert!(matches!(resp2, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));
        assert_eq!(nodo.pagos_procesados.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recuperacion_huerfana_commitea_si_un_peer_tiene_evidencia() {
        // 210 se proclama lider con una tx huerfana en preparados_replica;
        // 211 (peer vivo) ya tiene esa tx en pagos_procesados.
        let nuevo_lider = nodo_2pc_de_prueba_en_addr_canonica(210, RolActual::Replica { lider_id: 0 }, vec![211], None).await;
        let _peer = nodo_2pc_de_prueba_en_addr_canonica(211, RolActual::Replica { lider_id: 0 }, vec![], None).await;

        nuevo_lider.preparados_replica.lock().unwrap().insert("pr-orphan-commit".to_string(), cobrar_msg("pr-orphan-commit", 50.0));
        _peer.pagos_procesados.lock().unwrap().push(pago_de_prueba("pr-orphan-commit"));

        tokio::time::sleep(Duration::from_millis(50)).await;

        nuevo_lider.proclamarse_lider().await;

        assert!(nuevo_lider.pagos_procesados.lock().unwrap().iter().any(|p| p.payment_request_id == "pr-orphan-commit"));
        assert!(!nuevo_lider.preparados_replica.lock().unwrap().contains_key("pr-orphan-commit"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recuperacion_huerfana_abortea_si_nadie_tiene_evidencia() {
        let nuevo_lider = nodo_2pc_de_prueba_en_addr_canonica(220, RolActual::Replica { lider_id: 0 }, vec![221], None).await;
        let _peer = nodo_2pc_de_prueba_en_addr_canonica(221, RolActual::Replica { lider_id: 0 }, vec![], None).await;

        nuevo_lider.preparados_replica.lock().unwrap().insert("pr-orphan-abort".to_string(), cobrar_msg("pr-orphan-abort", 50.0));

        tokio::time::sleep(Duration::from_millis(50)).await;

        nuevo_lider.proclamarse_lider().await;

        assert!(!nuevo_lider.pagos_procesados.lock().unwrap().iter().any(|p| p.payment_request_id == "pr-orphan-abort"));
        assert!(!nuevo_lider.preparados_replica.lock().unwrap().contains_key("pr-orphan-abort"));
        assert!(_peer.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recuperacion_huerfana_es_no_op_si_preparados_replica_esta_vacio() {
        let nuevo_lider = nodo_2pc_de_prueba_en_addr_canonica(230, RolActual::Replica { lider_id: 0 }, vec![231], None).await;
        let _peer = nodo_2pc_de_prueba_en_addr_canonica(231, RolActual::Replica { lider_id: 0 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        nuevo_lider.proclamarse_lider().await;

        assert!(nuevo_lider.pagos_procesados.lock().unwrap().is_empty());
        assert!(nuevo_lider.preparados_replica.lock().unwrap().is_empty());
        assert!(_peer.pagos_procesados.lock().unwrap().is_empty());
        assert_eq!(rol_de(&nuevo_lider), RolActual::Lider);
    }
}
