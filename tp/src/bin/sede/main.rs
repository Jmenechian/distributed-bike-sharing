use actix::{Actor, Addr, AsyncContext, Context, Handler, MessageResult};
use matenmutex::common::{calcular_distancia, DISTANCIA_MINIMA, HEARTBEAT_INTERVAL, LIVENESS_TIMEOUT, MONITOR_INTERVAL, OFFLINE, ONLINE};
use matenmutex::log;
use matenmutex::messages::sede_sede::{
    AlquilerActivo, DevolucionRecibida, EstadoAlquiler, GetAlquileresActivos, GetDevolucionesRecibidas,
    GetInfo, GetNotificacionesPendientes, GetSedesConocidas, Heartbeat, MsgSedeHeartbeat,
    NotificacionPendiente, NotificarDevolucion, SedeInfo,
};
use matenmutex::slot::{self, ActualizarSlots, Slot};
use matenmutex::token::{self, SECRET_LEN};
use matenmutex::types::{timestamp_actual, BiciAlquilada, PagoPendiente};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufReader as StdBufReader, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Notify;
use tokio::time::sleep;

mod alquileres;
mod devoluciones;
mod pagos;
mod persistencia;
mod red_sedes;
mod slots;
#[cfg(test)]
mod test_helpers;

use devoluciones::FlushNotificacionesPendientes;
use pagos::{FlushPagosPendientes, GetPagosPendientes};
use persistencia::{cargar_estado, construir_estados_con_limbo, estado_path_de_id};
use red_sedes::{loop_pub, loop_sedes, ActualizarEstado};

pub(crate) const PROCESADOR_PAGOS_ADDR_DEFAULT: &str = "127.0.0.1:9091";
const CONFIG_FILE: usize = 1;

pub(crate) const RESET: &str = "\x1b[0m";
pub(crate) const BOLD: &str = "\x1b[1m";
pub(crate) const GREEN: &str = "\x1b[32m";
pub(crate) const YELLOW: &str = "\x1b[33m";
pub(crate) const BOLD_CYAN: &str = "\x1b[1;36m";

#[derive(Deserialize)]
struct SedeConfig {
    id: u32,
    coord_x: f64,
    coord_y: f64,
}

pub(crate) struct Sede {
    pub(crate) sede_id: u32,
    pub(crate) coords: (f64, f64),
    pub(crate) slots: Vec<Addr<Slot>>,
    pub(crate) token_secret: [u8; SECRET_LEN],
    pub(crate) pending_reservas: HashMap<String, (usize, BiciAlquilada)>,
    pub(crate) sedes_conocidas: Vec<SedeInfo>,
    pub(crate) sede_info: SedeInfo,
    pub(crate) procesador_pagos_addrs: Vec<String>,
    pub(crate) lider_cacheado: Arc<Mutex<Option<String>>>,
    pub(crate) estado_path: String,
    pub(crate) alquileres_activos: Vec<AlquilerActivo>,
    pub(crate) devoluciones_recibidas: Vec<DevolucionRecibida>,
    pub(crate) pagos_pendientes: Vec<PagoPendiente>,
    pub(crate) notificaciones_pendientes: Vec<NotificacionPendiente>,
    pub(crate) offline: Arc<Mutex<bool>>,
    pub(crate) estaba_offline: bool,
    pub(crate) flush_en_curso: bool,
}

impl Actor for Sede {
    type Context = Context<Self>;

    /// Arranca el monitor de liveness: cada MONITOR_INTERVAL revisa las sedes conocidas
    /// y marca como caidas las que no mandan heartbeat hace mas de LIVENESS_TIMEOUT.
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(MONITOR_INTERVAL), |sede, _ctx| {
            sede.marcar_sedes_caidas();
        });
    }
}

impl Sede {
    pub(crate) fn cliente_en_rango(&self, coords: (f64, f64)) -> bool {
        calcular_distancia(self.coords, coords) <= DISTANCIA_MINIMA
    }

    /// Marca conectada=false a las sedes sin heartbeat reciente. Loguea solo la transicion vivo->caido.
    fn marcar_sedes_caidas(&mut self) {
        let ahora = timestamp_actual();

        // Estando offline no escuchamos heartbeats, uso este otro flag
        // para no marcar a las demás sedes como offline cuando realmente no es así.
        if *self.offline.lock().unwrap_or_else(|e| e.into_inner()) {
            self.estaba_offline = true;
            return;
        }

        if self.estaba_offline {
            self.estaba_offline = false;
            for sede in self.sedes_conocidas.iter_mut() {
                sede.ultimo_update_ts = ahora;
            }
            return;
        }

        for sede in self.sedes_conocidas.iter_mut() {
            let vencida = ahora.saturating_sub(sede.ultimo_update_ts) > LIVENESS_TIMEOUT;
            if vencida && sede.conectada {
                sede.conectada = false;
                log!("[sede {}]: la sede {} se considera CAIDA (sin heartbeat hace mas de {}s)", self.sede_id, sede.sede_id, LIVENESS_TIMEOUT);
                println!("{YELLOW}[sede {}]: la sede {} esta OFFLINE (sin heartbeat hace mas de {}s){RESET}", self.sede_id, sede.sede_id, LIVENESS_TIMEOUT);
            }
        }
    }

    pub(crate) fn alquiler_ya_procesado(&self, alquiler_id: &str) -> bool {
        self.alquileres_activos
            .iter()
            .find(|a| a.alquiler_id == alquiler_id)
            .map(|a| {
                matches!(
                a.estado,
                EstadoAlquiler::Cerrado
                    | EstadoAlquiler::RoboConfirmado
                    | EstadoAlquiler::RoboRevertido
            )
            })
            .unwrap_or(false)
    }

    pub(crate) fn cerrar_alquiler(&mut self, alquiler: NotificarDevolucion) {
        self.alquileres_activos
            .retain(|a| a.alquiler_id != alquiler.alquiler_id);
    }
}

impl Handler<GetInfo> for Sede {
    type Result = MessageResult<GetInfo>;

    fn handle(&mut self, _msg: GetInfo, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.sede_info.clone())
    }
}

impl Handler<GetSedesConocidas> for Sede {
    type Result = MessageResult<GetSedesConocidas>;

    fn handle(&mut self, _msg: GetSedesConocidas, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.sedes_conocidas.clone())
    }
}

#[actix_rt::main]
async fn main() {
    dotenvy::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    let config = match cargar_config(args) {
        Some(c) => c,
        None => return,
    };

    let token_secret = match cargar_token_secret() {
        Some(token) => token,
        None => return,
    };

    let sede_id = config.id;
    let n = config.id as u16;
    matenmutex::utils::logger::init(&format!("sede{n}"));
    let num_slots = 20;
    let coords = (config.coord_x, config.coord_y);

    let estado_path = estado_path_de_id(sede_id);
    let estado = match cargar_estado(&estado_path) {
        Ok(estado) => estado,
        Err(e) => {
            eprintln!("Error fatal cargando el estado de la sede: {e}");
            return;
        }
    };

    let snapshot_existente = !estado.slots.is_empty();
    let slots: Vec<Addr<Slot>> = if snapshot_existente {
        slot::crear_slots_con_estados(construir_estados_con_limbo(&estado))
    } else {
        slot::crear_slots(sede_id, num_slots)
    };

    let sede_info = crear_sede_info(n, sede_id, coords);

    let procesador_pagos_addrs = pagos::parsear_addrs_procesador_pagos(
        env::var("PROCESADOR_PAGOS_ADDRS").ok().as_deref(),
        env::var("PROCESADOR_PAGOS_ADDR").ok().as_deref(),
    );


    let offline = Arc::new(Mutex::new(false));
    let despertar = Arc::new(Notify::new());

    let sede = crear_sede(token_secret, sede_id, slots, coords, sede_info, procesador_pagos_addrs, estado, offline.clone());
    sede.do_send(ActualizarSlots);

    let public_addr = format!("127.0.0.1:808{}", n);
    let private_addr = format!("127.0.0.1:700{}", n);

    let public_listener = TcpListener::bind(&public_addr).await.unwrap();
    let udp = Arc::new(UdpSocket::bind(&private_addr).await.unwrap());

    println!("Sede {} — publica: {}, interna: {}", n, public_addr, private_addr);

    let other_sedes: Vec<u16> = (1..=5).filter(|&s| s != n).collect(); //pego random a un par

    tokio::spawn(loop_heartbeat_recv(sede.clone(), udp.clone(), offline.clone(), despertar.clone()));
    tokio::spawn(loop_heartbeat_send(sede.clone(), udp.clone(), offline.clone(), despertar.clone(), sede_id, other_sedes));

    let sede_sede_addr = format!("127.0.0.1:888{}", n);
    let sede_sede_listener = TcpListener::bind(&sede_sede_addr).await.unwrap();
    
    tokio::spawn(loop_cli(sede.clone(), sede_id, offline.clone(), despertar.clone()));
    tokio::spawn(loop_sedes(sede.clone(), sede_sede_listener, offline, despertar));

    loop_pub(sede, public_listener).await;
}

/// Recibe heartbeats UDP de otras sedes y los reenvia al actor Sede.
/// Offline: aislada, tampoco escucha heartbeats de otras sedes. Duerme hasta volver online.
async fn loop_heartbeat_recv(sede: Addr<Sede>, udp: Arc<UdpSocket>, offline: Arc<Mutex<bool>>, despertar: Arc<Notify>) {
    let mut buf = vec![0u8; 1024];
    loop {
        if *offline.lock().unwrap_or_else(|e| e.into_inner()) {
            despertar.notified().await;
        }

        let (len, peer) = match udp.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => { log!("UDP recv error: {}", e); continue; }
        };
        match serde_json::from_slice::<MsgSedeHeartbeat>(&buf[..len]) {
            Ok(MsgSedeHeartbeat::Heartbeat(msg)) => {
                sede.send(msg).await.ok();
            }
            Err(e) => log!("UDP parse error from {}: {}", peer, e),
        }
    }
}

/// Anuncia periodicamente el estado de esta sede al resto via heartbeat UDP.
/// Offline: no anunciamos heartbeats. Dormimos hasta que la CLI nos despierte al volver online.
async fn loop_heartbeat_send(sede: Addr<Sede>, udp: Arc<UdpSocket>, offline: Arc<Mutex<bool>>, despertar: Arc<Notify>, sede_id: u32, other_sedes: Vec<u16>) {
    let targets: Vec<String> = other_sedes
        .iter()
        .map(|s| format!("127.0.0.1:700{}", s))
        .collect();

    loop {
        if *offline.lock().unwrap_or_else(|e| e.into_inner()) {
            despertar.notified().await;
            continue;
        }

        let info = sede.send(GetInfo).await.unwrap();

        let heartbeat = serde_json::to_vec(
            &MsgSedeHeartbeat::Heartbeat(Heartbeat { sede_id, info })
        ).unwrap();

        for addr in &targets {
            udp.send_to(&heartbeat, addr.as_str()).await.ok();
        }

        sleep(Duration::from_secs(HEARTBEAT_INTERVAL)).await;
    }
}

/// CLI de la sede: muestra el estado, cambia online/offline y lista la info de la sede.
async fn loop_cli(sede: Addr<Sede>, sede_id: u32, offline: Arc<Mutex<bool>>, despertar: Arc<Notify>) {
    listar_comandos_sede(sede_id);

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        match line.trim() {
            "offline" => {
                *offline.lock().unwrap_or_else(|e| e.into_inner()) = true;
                sede.send(ActualizarEstado { estado: OFFLINE }).await.ok();
                println!("{YELLOW}[sede {sede_id}] ahora OFFLINE — aislada de las demás sedes{RESET}");
            }
            "online" => {
                *offline.lock().unwrap_or_else(|e| e.into_inner()) = false;
                sede.send(ActualizarEstado { estado: ONLINE }).await.ok();
                despertar.notify_waiters(); // despierta a loop_sedes y al receiver, ambos esperando
                // El flush no puede depender de dónde quedó parado loop_sedes (puede estar
                // bloqueado en accept y perderse el notify), así que lo disparamos acá.
                sede.do_send(FlushNotificacionesPendientes);
                sede.do_send(FlushPagosPendientes);
                println!("{GREEN}[sede {sede_id}] ahora ONLINE{RESET}");
            }
            "info" => imprimir_info(&sede, &offline, sede_id).await,
            "ver_comandos" => listar_comandos_sede(sede_id),
            "clear" => {
                limpiar_pantalla();
                listar_comandos_sede(sede_id);
            }
            "" => {}
            otro => println!("Comando invalido: '{}' (online | offline | info | ver_comandos | clear)", otro),
        }
    }
}

fn listar_comandos_sede(sede_id: u32) {
    println!(
        "\n{BOLD_CYAN}=== Sede {sede_id} — CLI ==={RESET}\n\
         {BOLD}Comandos disponibles:{RESET}\n\
         {GREEN}  online{RESET}            - Volver a estar en línea\n\
         {GREEN}  offline{RESET}           - Aislarse de las demás sedes\n\
         {GREEN}  info{RESET}              - Ver el estado de esta sede\n\
         {GREEN}  ver_comandos{RESET}      - Ver esta lista de comandos\n\
         {GREEN}  clear{RESET}             - Limpiar la pantalla\n"
    );
}

/// Limpia la pantalla de la terminal (como el comando `clear`).
fn limpiar_pantalla() {
    print!("\x1b[2J\x1b[H");
    let _ = io::stdout().flush();
}

/// Imprime un resumen del estado de la sede para la CLI.
async fn imprimir_info(sede: &Addr<Sede>, offline: &Arc<Mutex<bool>>, sede_id: u32) {
    let offline_ahora = *offline.lock().unwrap_or_else(|e| e.into_inner());
    let estado = if offline_ahora {
        format!("{YELLOW}OFFLINE{RESET}")
    } else {
        format!("{GREEN}ONLINE{RESET}")
    };

    let info = sede.send(GetInfo).await;
    let sedes = sede.send(GetSedesConocidas).await.unwrap_or_default();
    let alquileres = sede.send(GetAlquileresActivos).await.unwrap_or_default();
    let devoluciones = sede.send(GetDevolucionesRecibidas).await.unwrap_or_default();
    let pendientes = sede.send(GetNotificacionesPendientes).await.unwrap_or_default();
    let pagos_pendientes = sede.send(GetPagosPendientes).await.unwrap_or_default();

    println!("\n{BOLD_CYAN}=== Info Sede {sede_id} ==={RESET}");
    println!("  Estado:                 {estado}");
    if let Ok(info) = info {
        println!("  Coordenadas:            ({:.4}, {:.4})", info.coords.0, info.coords.1);
        println!("  Slots:                  {} disponibles | {} vacíos", info.slots_disponibles, info.slots_vacios);
    }

    let robos = alquileres.iter().filter(|a| matches!(a.estado, EstadoAlquiler::RoboConfirmado)).count();
    println!("  Alquileres activos:     {} (robos confirmados: {})", alquileres.len(), robos);
    println!("  Devoluciones recibidas: {}", devoluciones.len());

    println!("  Notificaciones pendientes: {}", pendientes.len());
    for p in &pendientes {
        println!("    - alquiler {} → sede {}", p.alquiler_id, p.sede_origen_id);
    }

    println!("  Pagos pendientes:       {}", pagos_pendientes.len());

    println!("  Otras sedes conocidas:  {}", sedes.len());
    for s in &sedes {
        let est = if s.conectada { "online " } else { "offline" };
        println!("    - sede {} | {} | ({:.4}, {:.4}) | slots disp {}", s.sede_id, est, s.coords.0, s.coords.1, s.slots_disponibles);
    }
    println!();
}

fn crear_sede_info(n: u16, sede_id: u32, coords: (f64, f64)) -> SedeInfo {
    SedeInfo {
        sede_id,
        coords,
        public_addr: format!("127.0.0.1:808{}", n),
        private_addr: format!("127.0.0.1:700{}", n),
        slots_vacios: 99,
        slots_disponibles: 99,
        conectada: true,
        ultimo_update_ts: timestamp_actual(),
    }
}

fn crear_sede(
    token_secret: [u8; 32],
    sede_id: u32,
    slots: Vec<Addr<Slot>>,
    coords: (f64, f64),
    sede_info: SedeInfo,
    procesador_pagos_addrs: Vec<String>,
    estado: persistencia::EstadoPersistenteSede,
    offline: Arc<Mutex<bool>>,
) -> Addr<Sede> {
    let pending_reservas: HashMap<String, (usize, BiciAlquilada)> = estado
        .pending_reservas
        .into_iter()
        .map(|(alquiler_id, slot_idx, bici)| (alquiler_id, (slot_idx, bici)))
        .collect();

    let sede = Sede {
        sede_id,
        coords,
        slots,
        token_secret,
        pending_reservas,
        procesador_pagos_addrs,
        lider_cacheado: Arc::new(Mutex::new(None)),
        sedes_conocidas: vec![],
        sede_info,
        estado_path: estado_path_de_id(sede_id),
        alquileres_activos: estado.alquileres_activos,
        devoluciones_recibidas: estado.devoluciones_recibidas,
        pagos_pendientes: estado.pagos_pendientes,
        notificaciones_pendientes: estado.notificaciones_pendientes,
        offline,
        estaba_offline: false,
        flush_en_curso: false,
    }
        .start();
    sede
}

fn cargar_token_secret() -> Option<[u8; 32]> {
    let token_secret_raw = match env::var("TOKEN_SECRET") {
        Ok(raw) => raw,
        Err(_) => {
            eprintln!("Falta la variable de entorno TOKEN_SECRET no se puede iniciar la sede");
            return None;
        }
    };
    let token_secret = token::cargar_secret(&token_secret_raw);
    Some(token_secret)
}

fn cargar_config(args: Vec<String>) -> Option<SedeConfig> {
    let path = match args.get(CONFIG_FILE) {
        Some(p) => p,
        None => {
            eprintln!("Uso: cargo run --bin sede <path/a/config.json>");
            return None;
        }
    };
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error al abrir la config '{path}': {e}");
            return None;
        }
    };
    match serde_json::from_reader(StdBufReader::new(file)) {
        Ok(config) => Some(config),
        Err(e) => {
            eprintln!("Error al parsear la config '{path}': {e}");
            None
        }
    }
}
