use actix::{Actor, Addr, AsyncContext, Context, Handler, MailboxError, Message, MessageResult, ResponseFuture};
use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, DevolverBici, MsgClienteASede, PedirSedes, PreguntarBicis, SolicitarBici};
use matenmutex::slot;
use matenmutex::slot::{ActualizarSlots, AsegurarBici, Confirmar, ConsultarBici, Reservar, ResultadoReserva, SetSlotCounts, Slot};
use matenmutex::token::{self, PayloadToken, SECRET_LEN};
use matenmutex::types::{timestamp_actual, Bici, BiciAlquilada};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::File;
use std::io::BufReader as StdBufReader;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UdpSocket};
use tokio::time::sleep;
use tokio::sync::Notify;
use uuid::Uuid;
use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use matenmutex::common::{calcular_distancia, DISTANCIA_MINIMA, HEARTBEAT_INTERVAL, LIVENESS_TIMEOUT, MONITOR_INTERVAL, PREMONTO, PUERTO_BASE, TASA};
use matenmutex::messages::sede_sede::{ActualizarDisponibilidadDevolver, ActualizarDisponibilidadSolicitar, AlquilerActivo, DevolucionRecibida, EstadoAlquiler, EstadoConciliacion, GetInfo, GetSedesConocidas, GuardarAlquiler, GuardarDevolucion, Heartbeat, MsgSedeHeartbeat, SedeInfo, NotificarDevolucion, NotificacionPendiente, AckDevolucion, MsgSedeASede, VerificarRoboMasTarde};
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::slot::Cancelar;
use std::io;
use std::io::Error;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;

const PROCESADOR_PAGOS_ADDR_DEFAULT: &str = "127.0.0.1:9091";
const CONFIG_FILE: usize = 1;

#[derive(Deserialize)]
struct SedeConfig {
    id: u32,
    coord_x: f64,
    coord_y: f64,
}

pub struct Sede {
    sede_id: u32,
    coords: (f64, f64),
    slots: Vec<Addr<Slot>>,
    alquileres_activos: Vec<AlquilerActivo>,
    devoluciones_recibidas: Vec<DevolucionRecibida>,
    token_secret: [u8; SECRET_LEN],
    pending_reservas: std::collections::HashMap<String, (usize, BiciAlquilada)>,
    sedes_conocidas: Vec<SedeInfo>,
    sede_info: SedeInfo,
    procesador_pagos_addrs: Vec<String>,
    // online: bool,
    lider_cacheado: Arc<Mutex<Option<String>>>,
}

/// Arma la lista de direcciones de ProcesadorPagos a partir del .env.
/// Prioridad: PROCESADOR_PAGOS_ADDRS (lista separada por comas) > PROCESADOR_PAGOS_ADDR (valor unico) > default.
fn parsear_addrs_procesador_pagos(env_addrs: Option<&str>, env_addr_legacy: Option<&str>) -> Vec<String> {
    if let Some(addrs) = env_addrs {
        let lista: Vec<String> = addrs.split(',').map(|a| a.trim().to_string()).filter(|a| !a.is_empty()).collect();
        if !lista.is_empty() {
            return lista;
        }
    }

    if let Some(addr) = env_addr_legacy {
        let addr = addr.trim();
        if !addr.is_empty() {
            return vec![addr.to_string()];
        }
    }

    vec![PROCESADOR_PAGOS_ADDR_DEFAULT.to_string()]
}

/// Envia un mensaje de pago intentando descubrir cual es el ProcesadorPagos lider.
/// Prueba primero el lider cacheado (si hay), y si falla o responde NoSoyLider invalida el cache
/// y prueba el resto de los nodos en orden, sin repetir. Si alguno responde con exito (no NoSoyLider), lo cachea como lider.
async fn enviar_con_descubrimiento_de_lider(
    nodos: &[String],
    cache: &Arc<Mutex<Option<String>>>,
    msg: MsgSedeAProcesadorPagos,
) -> Result<MsgProcesadorPagosASede, io::Error> {
    let cacheado = cache.lock().unwrap_or_else(|e| e.into_inner()).clone();

    let mut orden: Vec<String> = Vec::with_capacity(nodos.len());
    if let Some(addr) = cacheado {
        orden.push(addr);
    }
    for nodo in nodos {
        if !orden.contains(nodo) {
            orden.push(nodo.clone());
        }
    }
    orden.truncate(nodos.len());

    for addr in orden {
        match enviar_a_procesador_pagos(&addr, &msg).await {
            Ok(MsgProcesadorPagosASede::NoSoyLider { .. }) => {
                *cache.lock().unwrap_or_else(|e| e.into_inner()) = None;
                continue;
            }
            Ok(resp) => {
                *cache.lock().unwrap_or_else(|e| e.into_inner()) = Some(addr);
                return Ok(resp);
            }
            Err(_) => {
                *cache.lock().unwrap_or_else(|e| e.into_inner()) = None;
                continue;
            }
        }
    }

    Err(Error::new(io::ErrorKind::NotConnected, "no se encontro lider de pagos"))
}

/// Abre una conexion TCP nueva al ProcesadorPagos, envia un mensaje
/// JSON-lines y espera la respuesta. Es lo mismo que hace enviar_mensaje_a_sede en cliente.rs.
async fn enviar_a_procesador_pagos(
    addr: &str,
    msg: &MsgSedeAProcesadorPagos,
) -> Result<MsgProcesadorPagosASede, io::Error> {
    let stream = TcpStream::connect(addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let payload = serde_json::to_string(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        + "\n";
    writer.write_all(payload.as_bytes()).await?;

    match lines.next_line().await? {
        Some(l) => serde_json::from_str(&l).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "procesador cerro la conexion")),
    }
}

impl Sede {
    fn cliente_en_rango(&self, coords: (f64, f64)) -> bool {
        calcular_distancia(self.coords, coords) <= DISTANCIA_MINIMA
    }

    fn cliente_tiene_dinero_solicitar(&self, dinero: u32) -> bool {
        dinero > PREMONTO
    }

    fn generar_comprobante(sede_id: u32, cliente_id: u32, token_secret: &[u8; 32], bici_id: String) -> Result<BiciAlquilada, token::TokenError> {
        let alquiler_id = Uuid::new_v4().to_string();
        let inicio_ts = timestamp_actual();

        let payload = PayloadToken {
            bici_id: bici_id.clone(),
            alquiler_id: alquiler_id.clone(),
            cliente_id: cliente_id.to_string(),
            sede_origen_id: sede_id,
            inicio_ts,
        };
        let token_alquiler = token::generar_token(&payload, token_secret)?;

        Ok(BiciAlquilada::new(bici_id, alquiler_id, cliente_id.to_string(), sede_id, inicio_ts, token_alquiler))
    }

    async fn reservar_bici(slots: &Vec<Addr<Slot>>) -> Option<(usize, String)> {

        let mut bici_ganadora: Option<(usize, String)> = None;
        for (idx, slot) in slots.iter().enumerate() {
            match slot.send(Reservar).await {
                Ok(ResultadoReserva::Reservada(bici_id)) => {
                    bici_ganadora = Some((idx, bici_id));
                    break;
                }
                Ok(ResultadoReserva::Rechazada) => continue,
                Err(_) => continue,
            }
        }

        bici_ganadora
    }

    fn generar_msg_de_cobro(monto: u32, alquiler_id: String, cliente_id: String) -> MsgSedeAProcesadorPagos {
        let payment_request_id = Uuid::new_v4().to_string();
        let cobro = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id,
            alquiler_id,
            cliente_id,
            monto_final: monto as f64,
        };
        cobro
    }

    async fn procesar_respuesta_preautorizacion(slots: Vec<Addr<Slot>>, sede_id: u32, cliente_id: u32, mut dinero: u32, token_secret: &[u8; 32], sede_addr: Addr<Sede>, idx: usize, bici_id: String, preautorizacion: Result<MsgProcesadorPagosASede, Error>) -> RespuestaSolicitarBici {
        match preautorizacion {
            Ok(MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id }) => {
                println!("[sede] Recibe PagoPreAutorizado (payment_request_id {}, pago_id {})", payment_request_id, pago_id);
                match Self::generar_comprobante(sede_id, cliente_id, &token_secret, bici_id) {
                    Err(_) => {
                        println!("[sede] Envia AlquilerRechazado: error generando token");
                        let _ = slots[idx].send(Cancelar).await;
                        RespuestaSolicitarBici::AlquilerRechazado(String::from("Error interno al generar el token de alquiler"))
                    }
                    Ok(bici) => {
                        println!("[sede] Envia ComprobanteAlquilerEmitido (bici {}, alquiler {})", bici.bici_id(), bici.alquiler_id());
                        let _ = sede_addr.send(GuardarReserva { alquiler_id: bici.alquiler_id().to_string(), slot_idx: idx, bici: bici.clone() }).await;
                        let _ = sede_addr.send(ActualizarDisponibilidadSolicitar).await;
                        let alquiler = crear_alquilerActivo(&bici);
                        let alquiler_id = bici.clone().alquiler_id;
                        let _ = sede_addr.send(GuardarAlquiler {alquiler}).await;

                        sede_addr.do_send(VerificarRoboMasTarde {alquiler_id, slot_idx: idx});

                        dinero -= PREMONTO;
                        RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, dinero)
                    }
                }
            }
            Ok(MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo }) => {
                println!("[sede] Recibe PagoRechazado (payment_request_id {}, motivo {})", payment_request_id, motivo);
                let _ = slots[idx].send(Cancelar).await;
                RespuestaSolicitarBici::AlquilerRechazado(format!("El procesador de pagos rechazo la pre-autorizacion: {}", motivo))
            }
            Ok(_) => {
                println!("[sede] Respuesta inesperada del procesador de pagos");
                let _ = slots[idx].send(Cancelar).await;
                RespuestaSolicitarBici::AlquilerRechazado(String::from("Respuesta inesperada del procesador de pagos"))
            }
            Err(e) => {
                println!("[sede] Error comunicandose con el procesador de pagos: {}", e);
                let _ = slots[idx].send(Cancelar).await;
                RespuestaSolicitarBici::AlquilerRechazado(String::from("No se pudo procesar el pago de pre-autorizacion"))
            }
        }
    }
}

fn crear_alquilerActivo(bici: &BiciAlquilada) -> AlquilerActivo {
    AlquilerActivo {
        alquiler_id: bici.alquiler_id().to_string(),
        bici_id: bici.bici_id().to_string(),
        cliente_id: bici.cliente_id().to_string(),
        sede_origen_id: bici.sede_origen_id(),
        inicio_ts: bici.inicio_ts(),
        pago_preautorizacion_id: "falta".to_string(), // ?
        deadline_devolucion_ts: bici.inicio_ts(),
        estado: EstadoAlquiler::Activo,
        pago_multa_id: Some("".to_string()),
        reembolso_multa_id: Some("".to_string()),
    }
}

fn crear_devolucion(bici: BiciAlquilada, sede_destino_id: u32, fin_ts: u64, monto: f64) -> DevolucionRecibida {
    DevolucionRecibida {
        alquiler_id: bici.alquiler_id().to_string(),
        bici_id: bici.bici_id().to_string(),
        cliente_id: bici.cliente_id().to_string(),
        sede_origen_id: bici.sede_origen_id(),
        sede_destino_id,
        fin_ts,
        monto,
        pago_id: Some("".to_string()),
        estado_pago: EstadoConciliacion::Confirmado,
        estado_notificacion_origen: EstadoConciliacion::Confirmado,
    }
}

fn crear_notificacion_devolucion(d: DevolucionRecibida, token: String) -> NotificarDevolucion {
    NotificarDevolucion {
        alquiler_id: d.alquiler_id,
        bici_id: d.bici_id,
        cliente_id: d.cliente_id,
        sede_origen_id: d.sede_origen_id,
        sede_destino_id: d.sede_destino_id,
        fin_ts: d.fin_ts,
        monto: d.monto,
        pago_id: d.pago_id,
        token_alquiler: token,
    }
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
    /// Marca conectada=false a las sedes sin heartbeat reciente. Loguea solo la transicion vivo->caido.
    fn marcar_sedes_caidas(&mut self) {
        let ahora = timestamp_actual();
        for sede in self.sedes_conocidas.iter_mut() {
            let vencida = ahora.saturating_sub(sede.ultimo_update_ts) > LIVENESS_TIMEOUT;
            if vencida && sede.conectada {
                sede.conectada = false;
                println!("[sede {}]: la sede {} se considera CAIDA (sin heartbeat hace mas de {}s)", self.sede_id, sede.sede_id, LIVENESS_TIMEOUT);
            }
        }
    }

    fn alquiler_ya_procesado(&self, alquiler_id: &str) -> bool {
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

    fn cerrar_alquiler(&mut self, alquiler: NotificarDevolucion) {
        self.alquileres_activos
            .retain(|a| a.alquiler_id != alquiler.alquiler_id);
    }
}

impl Handler<ActualizarDisponibilidadSolicitar> for Sede {
    type Result = ();

    fn handle(&mut self, _msg: ActualizarDisponibilidadSolicitar, _ctx: &mut Self::Context) -> Self::Result {

        self.sede_info.slots_disponibles = self.sede_info.slots_disponibles.saturating_sub(1);
        self.sede_info.slots_vacios      = self.sede_info.slots_vacios.saturating_add(1);
        self.sede_info.ultimo_update_ts = timestamp_actual();
    }
}

impl Handler<ActualizarDisponibilidadDevolver> for Sede { //maybe algo que revise los slots? por ahora bien basicon
    type Result = ();

    fn handle(&mut self, msg: ActualizarDisponibilidadDevolver, ctx: &mut Self::Context) -> Self::Result {
        self.sede_info.slots_disponibles = self.sede_info.slots_disponibles.saturating_add(1);
        self.sede_info.slots_vacios      = self.sede_info.slots_vacios.saturating_sub(1);
        self.sede_info.ultimo_update_ts = timestamp_actual();
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

impl Handler<GuardarAlquiler> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarAlquiler, _ctx: &mut Self::Context) -> Self::Result {
        self.alquileres_activos.push(msg.alquiler);
    }
}

impl Handler<GuardarDevolucion> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarDevolucion, _ctx: &mut Self::Context) -> Self::Result {
        self.devoluciones_recibidas.push(msg.devolucion);
    }
}

impl Handler<NotificarDevolucion> for Sede {
    type Result = MessageResult<NotificarDevolucion>;

    fn handle(&mut self, msg: NotificarDevolucion, _ctx: &mut Self::Context) -> Self::Result {

        println!("[sede] Recibí notificación de devolución");

        let alquiler_id = msg.alquiler_id.clone();

        if self.alquiler_ya_procesado(&alquiler_id) { //idempotencia
            return MessageResult(AckDevolucion {alquiler_id: msg.alquiler_id});
        }

        self.cerrar_alquiler(msg);

        MessageResult(AckDevolucion {alquiler_id: alquiler_id})
    }
}

impl Handler<VerificarRoboMasTarde> for Sede {
    type Result = ();
    fn handle(&mut self, msg: VerificarRoboMasTarde, ctx: &mut Context<Self>) -> Self::Result {
        ctx.run_later(Duration::from_secs(60), move |act, _ctx| {
            if let Some(alquiler) = act.alquileres_activos
                .iter_mut()
                .find(|a| a.alquiler_id == msg.alquiler_id)
            {
                match alquiler.estado {
                    EstadoAlquiler::Activo => {
                        println!("[sede] Bici del alquiler {} no fue devuelta, marcando como robada", msg.alquiler_id);
                        alquiler.estado = EstadoAlquiler::RoboConfirmado;
                        //cobrarle al cliente ->
                    }
                    _ => {}
                }
            }
        });
    }
}
impl Handler<PedirSedes> for Sede {
    type Result = MessageResult<PedirSedes>;

    fn handle(&mut self, _msg: PedirSedes, _ctx: &mut Self::Context) -> Self::Result {
        let mut sedes_conocidas = self.sedes_conocidas.clone();
        sedes_conocidas.push(self.sede_info.clone());
         MessageResult(sedes_conocidas.clone())
    }
}
impl Handler<SolicitarBici> for Sede {
    type Result = ResponseFuture<RespuestaSolicitarBici>;

    fn handle(&mut self, msg: SolicitarBici, ctx: &mut Context<Self>) -> Self::Result {
        if !self.cliente_en_rango(msg.coords) {
            let motivo = String::from("Para poder tomar la bici debe estar mas cerca de la sede");
            println!("[sede] Envia AlquilerRechazado: {}", motivo);
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(motivo) });
        }

        if !self.cliente_tiene_dinero_solicitar(msg.dinero) {
            let motivo = format!("Para poder tomar la bici debe tener mas dinero (Minimo ${})", PREMONTO);
            println!("[sede] Envia AlquilerRechazado: {}", motivo);
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(motivo) });
        }

        println!("[AAA] {:?}", self.alquileres_activos);
        println!("[AAA] {:?}", self.devoluciones_recibidas);

        let slots = self.slots.clone();
        let sede_id = self.sede_id;
        let cliente_id = msg.cliente_id;
        let dinero = msg.dinero;
        let token_secret = self.token_secret;
        let sede_addr = ctx.address();
        let procesador_pagos_addrs = self.procesador_pagos_addrs.clone();
        let lider_cacheado = self.lider_cacheado.clone();

        Box::pin(async move {
            match Self::reservar_bici(&slots).await {
                None => {
                    println!("[sede] Envia AlquilerRechazado: no hay slots disponibles");
                    RespuestaSolicitarBici::AlquilerRechazado(String::from("No hay bicicletas disponibles en este momento"))
                }
                Some((idx, bici_id)) => {
                    let payment_request_id = Uuid::new_v4().to_string();
                    let solicitud_pre_autorizacion = MsgSedeAProcesadorPagos::PreAutorizarPago {
                        payment_request_id: payment_request_id.clone(),
                        cliente_id: cliente_id.to_string(),
                        monto_seguridad: PREMONTO as f64,
                    };
                    println!("[sede] Envia {}", solicitud_pre_autorizacion.describir());

                    let preautorizacion = enviar_con_descubrimiento_de_lider(&procesador_pagos_addrs, &lider_cacheado, solicitud_pre_autorizacion).await;
                    Self::procesar_respuesta_preautorizacion(slots, sede_id, cliente_id, dinero, &token_secret, sede_addr, idx, bici_id, preautorizacion).await
                }
            }
        })
    }
}

impl Handler<ActualizarSlots> for Sede {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: ActualizarSlots, ctx: &mut Context<Self>) -> Self::Result {
        let requests: Vec<_> = self
            .slots
            .iter()
            .map(|slot| slot.send(ConsultarBici))
            .collect();

        let addr = ctx.address(); // clonable, se puede mover al async block

        Box::pin(async move {
            let resultados = futures::future::join_all(requests).await;
            let lineas: Vec<String> = resultados.into_iter().filter_map(|r| r.ok()).collect();
            let total = lineas.len();
            let ocupados = lineas.iter().filter(|l| !l.contains("sin bici")).count();
            let vacios = total - ocupados;

            addr.do_send(SetSlotCounts { ocupados: ocupados as u8, vacios: vacios as u8 });
        })
    }
}

impl Handler<SetSlotCounts> for Sede {
    type Result = ();

    fn handle(&mut self, msg: SetSlotCounts, _ctx: &mut Context<Self>) {
        self.sede_info.slots_disponibles = msg.ocupados;
        self.sede_info.slots_vacios = msg.vacios;
    }
}

struct GuardarReserva {
    alquiler_id: String,
    slot_idx: usize,
    bici: BiciAlquilada,
}

impl actix::Message for GuardarReserva {
    type Result = ();
}

impl Handler<GuardarReserva> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarReserva, _ctx: &mut Context<Self>) {
        self.pending_reservas.insert(msg.alquiler_id, (msg.slot_idx, msg.bici));
    }
}

impl Handler<AckComprobanteAlquiler> for Sede {
    type Result = ResponseFuture<RespuestaSolicitarBici>;

    fn handle(&mut self, msg: AckComprobanteAlquiler, _ctx: &mut Context<Self>) -> Self::Result {
        match self.pending_reservas.remove(&msg.alquiler_id) {
            None => Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(String::from("No hay reserva pendiente para ese alquiler")) }),
            Some((slot_idx, bici)) => {
                let slots = self.slots.clone();
                println!("[sede] Recibe AckComprobanteAlquiler (alquiler {}) confirmando slot {}", msg.alquiler_id, slot_idx);
                Box::pin(async move {
                    let _ = slots[slot_idx].send(Confirmar).await;
                    println!("[sede] Envia AlquilerConfirmado (bici {})", bici.bici_id());
                    RespuestaSolicitarBici::AlquilerConfirmado(bici)
                })
            }
        }
    }
}

struct ConfirmarNotificacionOrigen {
    alquiler_id: String
}

impl Message for ConfirmarNotificacionOrigen {
    type Result = ();
}
impl Handler<ConfirmarNotificacionOrigen> for Sede {
    type Result = ();

    fn handle(&mut self, msg: ConfirmarNotificacionOrigen, _ctx: &mut Self::Context) {
        if let Some(dev) = self.devoluciones_recibidas
            .iter_mut()
            .find(|d| d.alquiler_id == msg.alquiler_id)
        {
            dev.estado_notificacion_origen =
                EstadoConciliacion::Confirmado;
        }
    }
}

impl Handler<DevolverBici> for Sede {
    type Result = ResponseFuture<RespuestaDevolverBici>;

    fn handle(&mut self, msg: DevolverBici, ctx: &mut Context<Self>) -> Self::Result {
        if !self.cliente_en_rango(msg.coords) {
            let motivo = String::from("Para poder devolver la bici debe estar mas cerca de la sede");
            println!("[sede] Envia DevolucionRechazada: {}", motivo);
            return Box::pin(async move { RespuestaDevolverBici::DevolucionRechazada(motivo) });
        }

        let dinero = msg.dinero;
        let monto = match preparar_monto(msg.dinero, msg.bici.inicio_ts()) {
            Ok(costo) => costo,
            Err(respuesta) => {
                if let RespuestaDevolverBici::DevolucionRechazada(ref motivo) = respuesta {
                    println!("[sede] Envia DevolucionRechazada: {}", motivo);
                }
                return Box::pin(async move { respuesta });
            }
        };

        let slots = self.slots.clone();
        let bici_id = msg.bici.bici_id().to_string();
        let sede_addr = ctx.address();
        let procesador_pagos_addrs = self.procesador_pagos_addrs.clone();
        let lider_cacheado = self.lider_cacheado.clone();
        let alquiler_id = msg.bici.alquiler_id().to_string();
        let cliente_id = msg.bici.cliente_id().to_string();
        let sede_id = self.sede_id.clone();

        Box::pin(async move {
            let cobro = Self::generar_msg_de_cobro(monto, alquiler_id, cliente_id);
            println!("[sede] Envia {}", cobro.describir());

            match enviar_con_descubrimiento_de_lider(&procesador_pagos_addrs, &lider_cacheado, cobro).await {
                Ok(MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id }) => {
                    println!("[sede] Recibe {}", MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id }.describir());
                }
                Ok(MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo }) => {
                    println!("[sede] Recibe PagoRechazado (payment_request_id {}, motivo {})", payment_request_id, motivo);
                    return RespuestaDevolverBici::DevolucionRechazada(format!("El procesador de pagos rechazo el cobro: {}", motivo));
                }
                Ok(otra) => {
                    println!("[sede] Recibe {}", otra.describir());
                    return RespuestaDevolverBici::DevolucionRechazada(String::from("Respuesta inesperada del procesador de pagos"));
                }
                Err(e) => {
                    println!("[sede] Error comunicandose con el procesador de pagos: {}", e);
                    return RespuestaDevolverBici::DevolucionRechazada(String::from("No se pudo procesar el cobro de la devolucion"));
                }
            }

            for slot in slots.iter() {
                match slot.send(AsegurarBici { bici_id: bici_id.clone() }).await {
                    Ok(true) => {
                        let _ = sede_addr.send(ActualizarDisponibilidadDevolver).await;
                        let bici = msg.bici.clone();
                        let devolucion = crear_devolucion(msg.bici, sede_id, timestamp_actual(), monto as f64);
                        let notificacion_devolucion = crear_notificacion_devolucion(devolucion.clone(), bici.token_alquiler().parse().unwrap());
                        let _ = sede_addr.send(GuardarDevolucion {devolucion}).await;

                        let sede_origen = notificacion_devolucion.sede_origen_id;

                        match enviar_notificacion(sede_origen, notificacion_devolucion).await { //do_send cuando me llega el Ack handleo el mensaje Ack
                            Ok(ack) => {
                                let _ = sede_addr.send(
                                    ConfirmarNotificacionOrigen {
                                        alquiler_id: ack.alquiler_id,
                                    }
                                ).await;
                            }
                            Err(_) => {
                                // sigue Pendiente
                            }
                        }


                        return RespuestaDevolverBici::DevolucionOk(Bici { bici_id }, dinero - monto);
                    }
                    _ => continue,
                }
            }

            RespuestaDevolverBici::DevolucionRechazada(String::from("No hay lugar disponible para devolver la bici en este momento"))
        })
    }
}



fn preparar_monto(dinero: u32, tiempo_inicio: u64) -> Result<u32, RespuestaDevolverBici> {
    let fin = timestamp_actual();
    let duracion = fin - tiempo_inicio;
    println!("Duracion del alquiler: {}", duracion);
    let costo = (duracion * TASA as u64) as u32;

    if costo > dinero {
        Err(RespuestaDevolverBici::DevolucionRechazada(String::from("Para poder devolver la bici recarga")))
    } else {
        Ok(costo)
    }
}

impl Handler<PreguntarBicis> for Sede {
    type Result = ResponseFuture<String>;

    fn handle(&mut self, _msg: PreguntarBicis, _ctx: &mut Context<Self>) -> Self::Result {

        let requests: Vec<_> = self
            .slots
            .iter()
            .map(|slot| slot.send(ConsultarBici)) // mensaje a cada slot
            .collect();

        Box::pin(async move {
            let resultados = futures::future::join_all(requests).await;
            let lineas: Vec<String> = resultados.into_iter().filter_map(|r| r.ok()).collect();

            let ocupados = lineas.iter().filter(|l| !l.contains("sin bici")).count();
            let total = lineas.len();

            let mut respuesta = lineas.join("   ");
            respuesta.push_str(&format!("--- {}/{} slots ocupados ---", ocupados, total));
            respuesta
        })
    }
}

impl Handler<Heartbeat> for Sede {
    type Result = ();

    fn handle(&mut self, msg: Heartbeat, _ctx: &mut Context<Self>) -> () {
        println!("[sede] Recibe Heartbeat de {}", msg.sede_id);
        println!("[sede] info: {:?}", msg.info); //para debug

        let mut info = msg.info;
        // Liveness con hora local: "lo escuche recien", no la auto-reportada del emisor.
        info.ultimo_update_ts = timestamp_actual();
        info.conectada = true;

        match self.sedes_conocidas.iter_mut().find(|s| s.sede_id == info.sede_id) {
            Some(sede) => {
                if !sede.conectada {
                    println!("[sede {}]: la sede {} volvio a estar VIVA", self.sede_id, info.sede_id);
                }
                *sede = info;
            }
            None => self.sedes_conocidas.push(info),
        }
    }
}

#[cfg(test)]
mod tests {
    use matenmutex::common::MAX_VALUE;
    use super::*;
    use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, DevolverBici, SolicitarBici};
    use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
    use matenmutex::messages::sede_sede::crear_info_dummy;
    use matenmutex::slot::{crear_slots_con_estados, EstadoSlot};

    fn sede_con_estados(estados: Vec<EstadoSlot>) -> Addr<Sede> {
        sede_con_estados_y_procesador(estados, PROCESADOR_PAGOS_ADDR_DEFAULT.to_string())
    }

    /// Levanta un ProcesadorPagos de prueba que responde siempre
    /// PagoPreAutorizado (o PagoRechazado si rechazar es true) a
    /// cualquier PreAutorizarPago recibido. Devuelve la direccion en la que escucha.
    async fn stub_procesador_pagos(rechazar: bool) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(c) => c,
                    Err(_) => break,
                };
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let resp = match serde_json::from_str::<MsgSedeAProcesadorPagos>(line.trim()) {
                            Ok(MsgSedeAProcesadorPagos::PreAutorizarPago { payment_request_id, .. }) => {
                                if rechazar {
                                    MsgProcesadorPagosASede::PagoRechazado {
                                        payment_request_id,
                                        motivo: "rechazo simulado en test".to_string(),
                                    }
                                } else {
                                    MsgProcesadorPagosASede::PagoPreAutorizado {
                                        payment_request_id,
                                        pago_id: "pago-test".to_string(),
                                    }
                                }
                            }
                            Ok(MsgSedeAProcesadorPagos::CobrarPago { payment_request_id, .. }) => {
                                if rechazar {
                                    MsgProcesadorPagosASede::PagoRechazado {
                                        payment_request_id,
                                        motivo: "rechazo simulado en test".to_string(),
                                    }
                                } else {
                                    MsgProcesadorPagosASede::PagoAceptado {
                                        payment_request_id,
                                        pago_id: "cobro-test".to_string(),
                                    }
                                }
                            }
                            _ => MsgProcesadorPagosASede::PagoRechazado {
                                payment_request_id: String::new(),
                                motivo: "mensaje no soportado por el stub".to_string(),
                            },
                        };
                        let payload = serde_json::to_string(&resp).unwrap() + "\n";
                        if writer.write_all(payload.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        addr
    }

    fn sede_con_estados_y_procesador(estados: Vec<EstadoSlot>, procesador_pagos_addr: String) -> Addr<Sede> {
        let dummy_info = rellenar_ocupados_vacios_testing(&estados);

        let slots = crear_slots_con_estados(estados);

        Sede {
            sede_id: 1,
            coords: (-34.6037, -58.3816),
            slots,
            alquileres_activos: vec![],
            devoluciones_recibidas: vec![],
            token_secret: token::cargar_secret("secreto-de-test"),
            pending_reservas: std::collections::HashMap::new(),
            sedes_conocidas: vec![],
            sede_info: dummy_info,
            procesador_pagos_addrs: vec![procesador_pagos_addr],
            lider_cacheado: Arc::new(Mutex::new(None)),
        }
            .start()
    }

    fn rellenar_ocupados_vacios_testing(estados: &Vec<EstadoSlot>) -> SedeInfo {
        let ocupados = estados.iter()
            .filter(|e| matches!(e, EstadoSlot::Disponible { .. }))
            .count() as u8;
        let vacios = estados.iter()
            .filter(|e| matches!(e, EstadoSlot::Vacio))
            .count() as u8;

        let mut dummy_info = crear_info_dummy();
        dummy_info.slots_disponibles = ocupados;
        dummy_info.slots_vacios = vacios;
        dummy_info
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_slot_disponible_confirma_con_bici_id_real() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Disponible {
                bici_id: "1-3".to_string(),
            },
        ], procesador_addr);

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: MAX_VALUE,
            })
            .await
            .unwrap();

        let bici_alquilada = match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => bici,
            RespuestaSolicitarBici::AlquilerRechazado(motivo) => {
                panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo AlquilerRechazado({motivo})");
            }
            RespuestaSolicitarBici::AlquilerConfirmado(_) => {
                panic!("se esperaba ComprobanteAlquilerEmitido, no AlquilerConfirmado directo");
            }
        };

        assert_eq!(bici_alquilada.bici_id(), "1-3");
        assert!(!bici_alquilada.alquiler_id().is_empty());
        assert!(bici_alquilada.inicio_ts() > 0);

        let alquiler_id = bici_alquilada.alquiler_id().to_string();
        let respuesta_final = sede
            .send(AckComprobanteAlquiler { alquiler_id })
            .await
            .unwrap();

        match respuesta_final {
            RespuestaSolicitarBici::AlquilerConfirmado(bici) => {
                assert_eq!(bici.bici_id(), "1-3");
            }
            other => panic!("se esperaba AlquilerConfirmado, se obtuvo {:?}", other),
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_sin_slots_disponibles_rechaza() {
        let sede = sede_con_estados(vec![
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-2".to_string(),
            },
        ]);

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: 9999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            RespuestaSolicitarBici::AlquilerConfirmado(_) | RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_, _) => {
                panic!("se esperaba AlquilerRechazado");
            }
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_concurrente_sobre_un_unico_slot_disponible_solo_uno_gana() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![EstadoSlot::Disponible {
            bici_id: "1-1".to_string(),
        }], procesador_addr);

        let sede_a = sede.clone();
        let sede_b = sede.clone();

        let (resp_a, resp_b) = futures::join!(
            sede_a.send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 9999999,
            }),
            sede_b.send(SolicitarBici {
                cliente_id: 2,
                coords: (-34.6037, -58.3816),
                dinero: 9999999,

            })
        );

        let ganadores = [resp_a.unwrap(), resp_b.unwrap()]
            .into_iter()
            .filter(|r| matches!(r, RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_,_)))
            .count();
        let rechazados = 2 - ganadores;

        assert_eq!(ganadores, 1, "exactamente un cliente debe recibir el comprobante");
        assert_eq!(rechazados, 1, "exactamente un cliente debe ser rechazado");
    }

    #[actix_rt::test]
    async fn devolver_bici_con_slot_vacio_asegura_la_bici_y_responde_ok() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-3".to_string(),
            },
        ], procesador_addr);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, timestamp_actual(), String::new()),
                coords: (-34.6037, -58.3816),
                dinero: MAX_VALUE,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionOk(bici, ..) => {
                assert_eq!(bici.bici_id, "9-9");
            }
            RespuestaDevolverBici::DevolucionRechazada(motivo) => {
                panic!("se esperaba DevolucionOk, se obtuvo DevolucionRechazada({motivo})");
            }
        }
    }

    #[actix_rt::test]
    async fn devolver_bici_con_cobro_rechazado_no_asegura() {
        let procesador_addr = stub_procesador_pagos(true).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-3".to_string(),
            },
        ], procesador_addr);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (-34.6037, -58.3816),
                dinero: 999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionRechazada(_) => {}
            RespuestaDevolverBici::DevolucionOk(_, ..) => {
                panic!("se esperaba DevolucionRechazada, se obtuvo DevolucionOk");
            }
        }

        // El slot vacio (idx 1) debe seguir vacio: AsegurarBici no debe
        // haberse ejecutado, por lo que Reservar (que solo aplica a slots
        // Disponible) sigue siendo Rechazada sobre el slot Vacio.
        let resultado = sede.send(ConsultarEstadoSlot { idx: 1 }).await.unwrap();
        assert_eq!(resultado, ResultadoReserva::Rechazada);
    }

    #[actix_rt::test]
    async fn devolver_bici_sin_slots_vacios_rechaza() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Reservado {
                bici_id: "1-2".to_string(),
            },
        ], procesador_addr);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (-34.6037, -58.3816),
                dinero: 999999,

            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionRechazada(_) => {}
            RespuestaDevolverBici::DevolucionOk(_, ..) => {
                panic!("se esperaba DevolucionRechazada, se obtuvo DevolucionOk");
            }
        }
    }

    #[actix_rt::test]
    async fn devolver_bici_cliente_fuera_de_rango_rechaza() {
        let sede = sede_con_estados(vec![EstadoSlot::Vacio]);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (0.0, 0.0),
                dinero: 999999,

            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionRechazada(_) => {}
            RespuestaDevolverBici::DevolucionOk(_, ..) => {
                panic!("se esperaba DevolucionRechazada por estar fuera de rango, se obtuvo DevolucionOk");
            }
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_sin_dinero_rechaza() {
        let sede = sede_con_estados(vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }]);

        let respuesta = sede.send(SolicitarBici {
            cliente_id: 1,
            coords: (-34.2, -58.2),
            dinero: PREMONTO - 1,
        }).await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            other => panic!("Se esperaba alquiler rechazaddo por falta de dinero")
        }
    }

    // #[actix_rt::test]
    // async fn solicitar_bici_con_suficiente_dinero_solo_descuenta_premonto() {
    //     let sede = sede_con_estados(vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }]);
    //
    //     let respuesta = sede.send(SolicitarBici {
    //         cliente_id: 1,
    //         coords: (-34.2, -58.2),
    //         dinero: PREMONTO + 1000,
    //     }).await
    //         .unwrap();
    //
    //     match respuesta {
    //         RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_, dinero_restante) => {
    //             assert_eq!(dinero_restante, 1000, "El saldo devuelto deberia ser 1000");
    //         }
    //         other => panic!("Se esperaba alquiler confirmado, se obtuvo rechazado")
    //     }
    // }
    // [sede] Envia PreAutorizarPago (payment_request_id e2ef2263-4689-4b71-aac4-06e6985d8efe, cliente_id 1, monto_seguridad 150)
    // [sede] Error comunicandose con el procesador de pagos: no se encontro lider de pagos



    #[actix_rt::test]
    async fn devolver_Bici_sin_dinero_suficiente_rechaza() {
        let sede = sede_con_estados(vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }]);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (0.0, 0.0),
                dinero: 0,

            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionRechazada(_) => {}
            RespuestaDevolverBici::DevolucionOk(_, ..) => {
                panic!("se esperaba DevolucionRechazada por saldo insuficiente, se obtuvo DevolucionOk");
            }
        }
    }

    #[actix_rt::test]
        async fn heartbeat_actualiza_sede_ya_conocida_sin_duplicar() {
            let sede = sede_con_estados(vec![]);

            let mut info_sede_extra1 = crear_info_dummy();
            info_sede_extra1.sede_id = 99;
            info_sede_extra1.slots_disponibles = 5;

            sede.send(Heartbeat { sede_id: info_sede_extra1.sede_id, info: info_sede_extra1 })
                .await
                .unwrap();

            let mut info_sede_extra2 = crear_info_dummy();
            info_sede_extra2.sede_id = 99;
            info_sede_extra2.slots_disponibles = 3;

            sede.send(Heartbeat { sede_id: info_sede_extra2.sede_id, info: info_sede_extra2 })
                .await
                .unwrap();

            let conocidas = sede.send(GetSedesConocidas).await.unwrap();
            assert_eq!(conocidas.len(), 1, "no debe duplicarse la sede al recibir un segundo heartbeat");
            assert_eq!(conocidas[0].slots_disponibles, 3, "debe reflejar el heartbeat más reciente");
        }

    #[actix_rt::test]
    async fn solicitar_bici_con_preautorizacion_aceptada_emite_comprobante() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => {
                assert_eq!(bici.bici_id(), "1-1");
            }
            other => panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo {:?}", other),
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_preautorizacion_rechazada_cancela_la_reserva() {
        let procesador_addr = stub_procesador_pagos(true).await;
        let sede = sede_con_estados_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 999999,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            other => panic!("se esperaba AlquilerRechazado, se obtuvo {:?}", other),
        }

        // El slot debe haber vuelto a Disponible (no quedo Reservado), por
        // lo que una nueva reserva sobre el mismo slot debe ser posible.
        let resultado = sede.send(ConsultarEstadoSlot { idx: 0 }).await.unwrap();
        assert_eq!(resultado, ResultadoReserva::Reservada("1-1".to_string()));
    }

    // ---- parsear_addrs_procesador_pagos ----

    #[test]
    fn parsear_addrs_usa_lista_separada_por_comas_con_espacios() {
        let addrs = parsear_addrs_procesador_pagos(
            Some(" 127.0.0.1:9100, 127.0.0.1:9101 ,127.0.0.1:9102"),
            None,
        );
        assert_eq!(addrs, vec!["127.0.0.1:9100", "127.0.0.1:9101", "127.0.0.1:9102"]);
    }

    #[test]
    fn parsear_addrs_lista_de_un_solo_elemento() {
        let addrs = parsear_addrs_procesador_pagos(Some("127.0.0.1:9100"), None);
        assert_eq!(addrs, vec!["127.0.0.1:9100"]);
    }

    #[test]
    fn parsear_addrs_usa_legacy_si_no_hay_lista() {
        let addrs = parsear_addrs_procesador_pagos(None, Some("127.0.0.1:9100"));
        assert_eq!(addrs, vec!["127.0.0.1:9100"]);
    }

    #[test]
    fn parsear_addrs_usa_default_si_no_hay_nada_configurado() {
        let addrs = parsear_addrs_procesador_pagos(None, None);
        assert_eq!(addrs, vec![PROCESADOR_PAGOS_ADDR_DEFAULT.to_string()]);
    }

    // ---- enviar_con_descubrimiento_de_lider ----

    /// Levanta un nodo de prueba que responde siempre el mismo mensaje
    /// (NoSoyLider o una respuesta normal) a cualquier mensaje recibido.
    async fn stub_nodo_pagos(respuesta: MsgProcesadorPagosASede) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let respuesta = respuesta.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(_)) = lines.next_line().await {
                        let payload = serde_json::to_string(&respuesta).unwrap() + "\n";
                        if writer.write_all(payload.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        addr
    }

    fn msg_de_prueba() -> MsgSedeAProcesadorPagos {
        MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-test".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        }
    }

    #[tokio::test]
    async fn descubrimiento_redirige_pasando_por_replica_hasta_el_lider() {
        let replica = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        let nodos = vec![replica, lider.clone()];
        let cache = Arc::new(Mutex::new(None));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(lider));
    }

    #[tokio::test]
    async fn descubrimiento_salta_nodo_caido_y_encuentra_al_lider() {
        // Direccion en la que no hay nada escuchando: conexion fallara.
        let nodo_caido = "127.0.0.1:1".to_string();
        let lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        let nodos = vec![nodo_caido, lider.clone()];
        let cache = Arc::new(Mutex::new(None));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(lider));
    }

    #[tokio::test]
    async fn descubrimiento_reusa_el_lider_cacheado_sin_probar_otros_nodos() {
        let lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        // Nodo que rechazaria con NoSoyLider si llegara a ser consultado.
        let replica = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let nodos = vec![replica, lider.clone()];
        let cache = Arc::new(Mutex::new(Some(lider.clone())));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(lider));
    }

    #[tokio::test]
    async fn descubrimiento_re_descubre_si_el_lider_cacheado_dejo_de_responder() {
        let viejo_lider = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let nuevo_lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        let nodos = vec![viejo_lider.clone(), nuevo_lider.clone()];
        let cache = Arc::new(Mutex::new(Some(viejo_lider)));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(nuevo_lider));
    }

    #[tokio::test]
    async fn descubrimiento_falla_si_todos_los_nodos_responden_no_soy_lider() {
        let n1 = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let n2 = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let nodos = vec![n1, n2];
        let cache = Arc::new(Mutex::new(None));

        let resultado = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await;

        assert!(resultado.is_err());
        assert_eq!(*cache.lock().unwrap(), None);
    }
}

#[cfg(test)]
struct ConsultarEstadoSlot {
    idx: usize,
}

#[cfg(test)]
impl actix::Message for ConsultarEstadoSlot {
    type Result = ResultadoReserva;
}

#[cfg(test)]
impl Handler<ConsultarEstadoSlot> for Sede {
    type Result = ResponseFuture<ResultadoReserva>;

    fn handle(&mut self, msg: ConsultarEstadoSlot, _ctx: &mut Context<Self>) -> Self::Result {
        let slot = self.slots[msg.idx].clone();
        Box::pin(async move { slot.send(Reservar).await.unwrap_or(ResultadoReserva::Rechazada) })
    }
}
    //
    //     #[actix_rt::test]
    //     async fn heartbeat_multiples_sedes_distintas_se_registran_por_separado() {
    //         let sede = sede_con_estados(vec![]);
    //
    //         let mut info_sede_extra1 = crear_info_dummy();
    //         info_sede_extra1.sede_id = 10;
    //
    //         let mut info_sede_extra2 = crear_info_dummy();
    //         info_sede_extra2.sede_id = 20;
    //
    //         sede.send(Heartbeat { sede_id: info_sede_extra1.sede_id, info: info_sede_extra1 }).await.unwrap();
    //         sede.send(Heartbeat { sede_id: info_sede_extra2.sede_id, info: info_sede_extra2 }).await.unwrap();
    //
    //         let conocidas = sede.send(GetSedesConocidas).await.unwrap();
    //         assert_eq!(conocidas.len(), 2, "deben registrarse dos sedes distintas");
    //     }
    //
    //
    // #[actix_rt::test]
    // async fn heartbeat_agrega_sede_nueva_a_sedes_conocidas() {
    //     let sede = sede_con_estados(vec![]);
    //
    //     let info_remota = crear_info_dummy();
    //     sede.send(Heartbeat { sede_id: info_remota.sede_id, info: info_remota.clone() })
    //         .await
    //         .unwrap();
    //
    //     let conocidas = sede.send(GetSedesConocidas).await.unwrap();
    //     assert_eq!(conocidas.len(), 1, "debe haber exactamente una sede conocida");
    //     assert_eq!(conocidas[0].sede_id, info_remota.sede_id);
    // }


async fn formatear_respuesta<T: Serialize>(resultado: Result<T, MailboxError>) -> String {
    match resultado {
        Ok(valor) => match serde_json::to_string(&valor) {
            Ok(json) => json + "\n",
            Err(e) => {
                eprintln!("Error serializando respuesta: {}", e);
                String::from("ERROR:no se pudo serializar la respuesta\n")
            }
        },
        Err(e) => {
            eprintln!("Error enviando mensaje al actor Sede: {}", e);
            String::from("ERROR:la sede no pudo procesar el mensaje\n")
        }
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
    let num_slots = 20;
    let slots: Vec<Addr<Slot>> = slot::crear_slots(sede_id, num_slots);
    let coords = (config.coord_x, config.coord_y);

    let sede_info = crear_sede_info(n, sede_id, coords);


    let procesador_pagos_addrs = parsear_addrs_procesador_pagos(
        env::var("PROCESADOR_PAGOS_ADDRS").ok().as_deref(),
        env::var("PROCESADOR_PAGOS_ADDR").ok().as_deref(),
    );


    let sede = crear_sede(token_secret, sede_id, slots, coords, sede_info, procesador_pagos_addrs);
    sede.do_send(ActualizarSlots);

    let public_addr = format!("127.0.0.1:808{}", n);
    let private_addr = format!("127.0.0.1:700{}", n);

    let public_listener = TcpListener::bind(&public_addr).await.unwrap();
    let udp = Arc::new(UdpSocket::bind(&private_addr).await.unwrap());

    println!("Sede {} — publica: {}, interna: {}", n, public_addr, private_addr);

    // Estado online/offline: lo mutua la CLI, lo leen el loop publico y el sender de heartbeats.
    // El loop publico se duerme en despertar.notified() cuando esta offline.
    let offline = Arc::new(Mutex::new(false));
    let despertar = Arc::new(Notify::new());

    // receiver
    let udp_rx = udp.clone();
    let sede_rx = sede.clone();
    let offline_recv = offline.clone();
    let despertar_recv = despertar.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        loop {
            // Offline: aislada, tampoco escucha heartbeats de otras sedes. Duerme hasta volver online.
            if *offline_recv.lock().unwrap_or_else(|e| e.into_inner()) {
                despertar_recv.notified().await;
            }

            let (len, peer) = match udp_rx.recv_from(&mut buf).await {
                Ok(r) => r,
                Err(e) => { eprintln!("UDP recv error: {}", e); continue; }
            };
            match serde_json::from_slice::<MsgSedeHeartbeat>(&buf[..len]) {
                Ok(MsgSedeHeartbeat::Heartbeat(msg)) => {
                    sede_rx.send(msg).await.ok();
                }
                Err(e) => eprintln!("UDP parse error from {}: {}", peer, e),
            }
        }
    });

    // sender
    let udp_tx = udp.clone();
    let sede_tx = sede.clone();
    let offline_hb = offline.clone();
    let other_sedes: Vec<u16> = (1..=5).filter(|&s| s != n).collect(); //pego random a un par

    tokio::spawn(async move {
        let mut bootstrapped = false; //para no pegarle a todos lados


        loop {
            // Offline: no anunciamos heartbeats. El propio sleep nos despierta a revisar de nuevo.
            if *offline_hb.lock().unwrap_or_else(|e| e.into_inner()) {
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL)).await;
                continue;
            }

            let info = sede_tx.send(GetInfo).await.unwrap();
            let sedes_conocidas = sede_tx.send(GetSedesConocidas).await.unwrap();

            let targets: Vec<String> = if !bootstrapped {
                bootstrapped = true;
                other_sedes.iter().map(|s| format!("127.0.0.1:700{}", s)).collect()
            } else {
                sedes_conocidas.iter().map(|s| s.private_addr.clone()).collect()
            };

            let heartbeat = serde_json::to_vec(
                &MsgSedeHeartbeat::Heartbeat(Heartbeat { sede_id, info })
            ).unwrap();

            for addr in &targets {
                udp_tx.send_to(&heartbeat, addr.as_str()).await.ok();
            }

            sleep(Duration::from_secs(HEARTBEAT_INTERVAL)).await;
        }
    });

    let sede_sede_addr = format!("127.0.0.1:888{}", n);
    let sede_sede_listener = TcpListener::bind(&sede_sede_addr).await.unwrap();
    tokio::spawn(loop_cli(offline.clone(), despertar.clone()));

    tokio::spawn(loop_sedes(sede.clone(), sede_sede_listener, offline, despertar));

    loop_pub(sede, public_listener).await;
}

/// CLI de la sede: muta el estado online/offline. Al volver online despierta al loop publico.
async fn loop_cli(offline: Arc<Mutex<bool>>, despertar: Arc<Notify>) {
    println!("Comandos de la sede: online | offline");

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        match line.trim() {
            "offline" => {
                *offline.lock().unwrap_or_else(|e| e.into_inner()) = true;
                println!("[sede] offline");
            }
            "online" => {
                *offline.lock().unwrap_or_else(|e| e.into_inner()) = false;
                despertar.notify_waiters(); // despierta a loop_sedes y al receiver, ambos esperando
                println!("[sede] online");
            }
            "" => {}
            otro => println!("Comando invalido: '{}' (online | offline)", otro),
        }
    }
}
async fn loop_sedes(sede: Addr<Sede>, listener: TcpListener, offline: Arc<Mutex<bool>>, despertar: Arc<Notify>) {
    loop {
        // Offline: Nos dormimos hasta que la CLI vuelva online
        // y dispare notify_waiters. loop_pub sigue aparte, asi que se atienden clientes locales.
        if *offline.lock().unwrap_or_else(|e| e.into_inner()) {
            println!("[sede] offline — aislada de otras sedes, esperando online");
            despertar.notified().await;
            println!("[sede] online — reanudando comunicacion entre sedes");
        }

        let (socket, _) = listener.accept().await.unwrap();

        let sede = sede.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {

                let response = match serde_json::from_str::<MsgSedeASede>(&line) {

                    Ok(MsgSedeASede::NotificarDevolucion(msg)) => {

                        match sede.send(msg).await {

                            Ok(ack) => {
                                serde_json::to_string(&ack)
                                    .unwrap() + "\n"
                            }

                            Err(_) => {
                                "ERROR\n".to_string()
                            }
                        }
                    }

                    Err(_) => {
                        "UNKNOWN\n".to_string()
                    },
                    Ok(MsgSedeASede::AckDevolucion(_)) => todo!()
                };

                writer.write_all(response.as_bytes()).await.ok();
            }
        });
    }
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

fn crear_sede(token_secret: [u8; 32], sede_id: u32, slots: Vec<Addr<Slot>>, coords: (f64, f64), sede_info: SedeInfo, procesador_pagos_addrs: Vec<String>) -> Addr<Sede> {
    let sede = Sede {
        sede_id,
        coords,
        slots,
        alquileres_activos: vec![],
        devoluciones_recibidas: vec![],
        token_secret,
        pending_reservas: std::collections::HashMap::new(),
        procesador_pagos_addrs,
        lider_cacheado: Arc::new(Mutex::new(None)),
        sedes_conocidas: vec![],
        sede_info,
    }
        .start();
    sede
}

fn cargar_token_secret() -> Option<[u8; 32]> {
    let token_secret_raw = match env::var("TOKEN_SECRET") {
        Ok(raw) => raw,
        Err(_) => {
            eprintln!("Falta la variable de entorno TOKEN_SECRET — no se puede iniciar la sede");
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

async fn loop_pub(sede: Addr<Sede>, public_listener: TcpListener) {
    loop {
        let (socket, addr) = match public_listener.accept().await {
            Ok(conexion) => conexion,
            Err(e) => {
                eprintln!("Error aceptando conexion entrante: {}", e);
                continue;
            }
        };
        println!("New connection from {}", addr);
        let sede = sede.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let response = match serde_json::from_str::<MsgClienteASede>(line.trim()) {
                    Ok(MsgClienteASede::SolicitarBici(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::AckComprobanteAlquiler(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::DevolverBici(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::PreguntarBicis(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::PedirSedes(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Err(_) => format!("UNKNOWN:{}\n", line.trim()),
                };

                if let Err(e) = writer.write_all(response.as_bytes()).await {
                    eprintln!("Error escribiendo respuesta a {}: {}", addr, e);
                    break;
                }
            }
            println!("Connection from {} closed", addr);
        });
    }
}

pub async fn enviar_notificacion( sede_origen_id: u32, info: NotificarDevolucion) -> Result<AckDevolucion, String> {
    
    let addr = format!("127.0.0.1:888{}", sede_origen_id);

    println!("[sede] Intentando notificar devolucion a sede {}", sede_origen_id);

    let mut stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("No pude conectar: {}", e))?;

    let msg = serde_json::to_string(
        &MsgSedeASede::NotificarDevolucion(info)
    )
        .map_err(|e| e.to_string())?;

    stream
        .write_all(format!("{msg}\n").as_bytes())
        .await
        .map_err(|e| e.to_string())?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();

    reader
        .read_line(&mut response)
        .await
        .map_err(|e| e.to_string())?;

    serde_json::from_str::<AckDevolucion>(response.trim())
        .map_err(|e| e.to_string())
}