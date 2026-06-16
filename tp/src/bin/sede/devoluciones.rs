use actix::{Addr, AsyncContext, Context, Handler, Message, MessageResult, ResponseFuture};
use matenmutex::common::{PREMONTO, TASA};
use matenmutex::log;
use matenmutex::messages::cliente_sede::DevolverBici;
use matenmutex::messages::sede_cliente::RespuestaDevolverBici;
use matenmutex::messages::sede_procesador::MsgProcesadorPagosASede;
use matenmutex::messages::sede_sede::{
    AckDevolucion, ActualizarDisponibilidadDevolver, DevolucionRecibida, EstadoAlquiler, EstadoConciliacion,
    GetDevolucionesRecibidas, GetNotificacionesPendientes, GuardarDevolucion, MsgSedeASede,
    NotificacionPendiente, NotificarDevolucion, RemoverAlquiler, SedeInfo,
};
use matenmutex::slot::{AsegurarBici, Slot};
use matenmutex::token;
use matenmutex::types::{timestamp_actual, Bici, BiciAlquilada, PagoPendiente};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::pagos::{enviar_con_descubrimiento_de_lider, generar_reembolso_deposito, GuardarPagoPendiente};
use crate::persistencia::PersistirEstado;
use crate::Sede;

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
        estado_notificacion_origen: EstadoConciliacion::Pendiente,
    }
}

fn crear_notificacion_pendiente(n: &NotificarDevolucion) -> NotificacionPendiente {
    NotificacionPendiente {
        alquiler_id: n.alquiler_id.clone(),
        bici_id: n.bici_id.clone(),
        cliente_id: n.cliente_id.clone(),
        sede_origen_id: n.sede_origen_id,
        sede_destino_id: n.sede_destino_id,
        fin_ts: n.fin_ts,
        monto: n.monto,
        pago_id: n.pago_id.clone(),
        token_alquiler: n.token_alquiler.clone(),
        intentos: 0,
    }
}

fn notificacion_pendiente_a_devolucion(p: &NotificacionPendiente) -> NotificarDevolucion {
    NotificarDevolucion {
        alquiler_id: p.alquiler_id.clone(),
        bici_id: p.bici_id.clone(),
        cliente_id: p.cliente_id.clone(),
        sede_origen_id: p.sede_origen_id,
        sede_destino_id: p.sede_destino_id,
        fin_ts: p.fin_ts,
        monto: p.monto,
        pago_id: p.pago_id.clone(),
        token_alquiler: p.token_alquiler.clone(),
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

fn preparar_monto(dinero: f64, tiempo_inicio: u64) -> Result<f64, RespuestaDevolverBici> {
    let fin = timestamp_actual();
    let duracion = fin - tiempo_inicio;
    log!("Duracion del alquiler: {}", duracion);
    let costo = duracion as f64 * TASA;

    if costo > dinero {
        Err(RespuestaDevolverBici::DevolucionRechazada(String::from("Para poder devolver la bici recarga")))
    } else {
        Ok(costo)
    }
}

/// Datos compartidos por las distintas etapas de una devolucion en curso.
struct ContextoDevolucion {
    sede_addr: Addr<Sede>,
    procesador_pagos_addrs: Vec<String>,
    lider_cacheado: Arc<Mutex<Option<String>>>,
    sede_id: u32,
    sedes_conocidas: Vec<SedeInfo>,
    offline: Arc<Mutex<bool>>,
}

/// Cobra el deposito al cliente. Si la sede esta offline, el cobro queda pendiente
/// (se encola mas adelante junto con el reembolso) y la devolucion sigue su curso.
async fn procesar_cobro(offline: bool, monto: f64, alquiler_id: &str, cliente_id: &str, contexto: &ContextoDevolucion) -> Result<(), RespuestaDevolverBici> {
    if offline {
        log!("[sede] offline: el cobro de la devolucion (alquiler {}) queda pendiente", alquiler_id);
        return Ok(());
    }

    let cobro = Sede::generar_msg_de_cobro(monto, alquiler_id.to_string(), cliente_id.to_string());
    log!("[sede] Envia {}", cobro.describir());

    match enviar_con_descubrimiento_de_lider(&contexto.procesador_pagos_addrs, &contexto.lider_cacheado, cobro).await {
        Ok(MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id }) => {
            log!("[sede] Recibe {}", MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id }.describir());
            Ok(())
        }
        Ok(MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo }) => {
            log!("[sede] Recibe PagoRechazado (payment_request_id {}, motivo {})", payment_request_id, motivo);
            Err(RespuestaDevolverBici::DevolucionRechazada(format!("El procesador de pagos rechazo el cobro: {}", motivo)))
        }
        Ok(otra) => {
            log!("[sede] Recibe {}", otra.describir());
            Err(RespuestaDevolverBici::DevolucionRechazada(String::from("Respuesta inesperada del procesador de pagos")))
        }
        Err(e) => {
            log!("[sede] Error comunicandose con el procesador de pagos: {}", e);
            Err(RespuestaDevolverBici::DevolucionRechazada(String::from("No se pudo procesar el cobro de la devolucion")))
        }
    }
}

/// Sede offline: encola el cobro y el reembolso del deposito para reintentarlos al volver online.
async fn encolar_pagos_pendientes_offline(sede_addr: &Addr<Sede>, monto: f64, alquiler_id: &str, cliente_id: &str) {
    let cobro_pendiente = Sede::generar_msg_de_cobro(monto, alquiler_id.to_string(), cliente_id.to_string());
    let pago_cobro = PagoPendiente {
        payment_request_id: cobro_pendiente.payment_request_id().to_string(),
        alquiler_id: Some(alquiler_id.to_string()),
        cliente_id: cliente_id.to_string(),
        mensaje: cobro_pendiente,
    };
    let _ = sede_addr.send(GuardarPagoPendiente { pago: pago_cobro }).await;

    let reembolso_pendiente = generar_reembolso_deposito(alquiler_id.to_string(), cliente_id.to_string());
    let pago_reembolso = PagoPendiente {
        payment_request_id: reembolso_pendiente.payment_request_id().to_string(),
        alquiler_id: Some(alquiler_id.to_string()),
        cliente_id: cliente_id.to_string(),
        mensaje: reembolso_pendiente,
    };
    let _ = sede_addr.send(GuardarPagoPendiente { pago: pago_reembolso }).await;
    log!("[sede] offline: cobro y reembolso de deposito (alquiler {}) quedan pendientes", alquiler_id);
}

/// Sede online: reembolsa el deposito al cliente y, si la bici era un robo confirmado, revierte el robo.
async fn procesar_reembolso_deposito(alquiler_id: &str, cliente_id: &str, era_robo_local: bool, contexto: &ContextoDevolucion) {
    let reembolso = generar_reembolso_deposito(alquiler_id.to_string(), cliente_id.to_string());
    match enviar_con_descubrimiento_de_lider(&contexto.procesador_pagos_addrs, &contexto.lider_cacheado, reembolso).await {
        Ok(MsgProcesadorPagosASede::PagoReembolsado { pago_id, .. }) => {
            log!("[sede] Depósito (${}) reintegrado al cliente por alquiler {}", PREMONTO, alquiler_id);
            if era_robo_local {
                contexto.sede_addr.do_send(RevertirRoboLocal { alquiler_id: alquiler_id.to_string(), reembolso_id: pago_id });
            }
        }
        Ok(otra) => log!("[sede] Reembolso de depósito: respuesta inesperada {}", otra.describir()),
        Err(e) => log!("[sede] Reembolso de depósito: error con el procesador de pagos: {}", e),
    }
}

/// Avisa a la sede de origen que su bici fue devuelta aca. Si la bici vuelve a su propia
/// sede no hay nada que avisar por TCP; si la sede de origen esta caida o nosotros estamos
/// offline, la notificacion queda pendiente para reintentar mas tarde.
async fn notificar_devolucion_a_origen(notificacion: NotificarDevolucion, offline: bool, contexto: &ContextoDevolucion) {
    let sede_addr = &contexto.sede_addr;
    let sede_origen_id = notificacion.sede_origen_id;

    if sede_origen_id == contexto.sede_id {
        if let Ok(ack) = sede_addr.send(notificacion).await {
            let _ = sede_addr.send(ConfirmarNotificacionOrigen { alquiler_id: ack.alquiler_id }).await;
        }
        return;
    }

    let origen_conectada = contexto.sedes_conocidas.iter()
        .any(|sede| sede.sede_id == sede_origen_id && sede.conectada);

    if offline || !origen_conectada {
        sede_addr.do_send(EncolarNotificacionPendiente { notificacion });
        return;
    }

    let addr = format!("127.0.0.1:888{}", sede_origen_id);
    match enviar_notificacion(addr, notificacion.clone()).await {
        Ok(ack) => {
            let _ = sede_addr.send(ConfirmarNotificacionOrigen { alquiler_id: ack.alquiler_id }).await;
        }
        Err(_) => {
            sede_addr.do_send(EncolarNotificacionPendiente { notificacion });
        }
    }
}

/// Busca un slot vacio para asegurar la bici devuelta y, de encontrarlo, registra la
/// devolucion, cobra/reembolsa el deposito (o los encola si estamos offline) y avisa
/// a la sede de origen. Devuelve `None` si no hay ningun slot disponible.
async fn asegurar_y_devolver(
    slots: &[Addr<Slot>],
    bici: BiciAlquilada,
    monto: f64,
    dinero: f64,
    era_robo_local: bool,
    offline_inicio: bool,
    contexto: &ContextoDevolucion,
) -> Option<RespuestaDevolverBici> {
    let bici_id = bici.bici_id().to_string();
    let alquiler_id = bici.alquiler_id().to_string();
    let cliente_id = bici.cliente_id().to_string();
    let sede_addr = &contexto.sede_addr;

    for slot in slots.iter() {
        if !matches!(slot.send(AsegurarBici { bici_id: bici_id.clone() }).await, Ok(true)) {
            continue;
        }

        let _ = sede_addr.send(ActualizarDisponibilidadDevolver).await;
        let token_alquiler = bici.token_alquiler().to_string();
        let devolucion = crear_devolucion(bici, contexto.sede_id, timestamp_actual(), monto);
        let notificacion_devolucion = crear_notificacion_devolucion(devolucion.clone(), token_alquiler);
        let _ = sede_addr.send(GuardarDevolucion { devolucion }).await;

        if !era_robo_local {
            let _ = sede_addr.send(RemoverAlquiler { alquiler_id: alquiler_id.clone() }).await;
        }

        if offline_inicio {
            encolar_pagos_pendientes_offline(sede_addr, monto, &alquiler_id, &cliente_id).await;
        } else {
            procesar_reembolso_deposito(&alquiler_id, &cliente_id, era_robo_local, contexto).await;
        }

        let _ = sede_addr.send(PersistirEstado).await;

        let offline = *contexto.offline.lock().unwrap_or_else(|e| e.into_inner());
        notificar_devolucion_a_origen(notificacion_devolucion, offline, contexto).await;

        return Some(RespuestaDevolverBici::DevolucionOk(Bici { bici_id }, dinero - monto + PREMONTO));
    }

    None
}

impl Handler<DevolverBici> for Sede {
    type Result = ResponseFuture<RespuestaDevolverBici>;

    fn handle(&mut self, msg: DevolverBici, ctx: &mut Context<Self>) -> Self::Result {
        if !self.cliente_en_rango(msg.coords) {
            let motivo = String::from("Para poder devolver la bici debe estar mas cerca de la sede");
            log!("[sede] Envia DevolucionRechazada: {}", motivo);
            return Box::pin(async move { RespuestaDevolverBici::DevolucionRechazada(motivo) });
        }

        if let Err(e) = token::validar_token(msg.bici.token_alquiler(), &self.token_secret) {
            let motivo = format!("Token de alquiler invalido: {:?}", e);
            log!("[sede] Envia DevolucionRechazada: {}", motivo);
            return Box::pin(async move { RespuestaDevolverBici::DevolucionRechazada(motivo) });
        }

        let dinero = msg.dinero;
        let monto = match preparar_monto(msg.dinero, msg.bici.inicio_ts()) {
            Ok(costo) => costo,
            Err(respuesta) => {
                if let RespuestaDevolverBici::DevolucionRechazada(ref motivo) = respuesta {
                    log!("[sede] Envia DevolucionRechazada: {}", motivo);
                }
                return Box::pin(async move { respuesta });
            }
        };

        let bici = msg.bici;
        let alquiler_id = bici.alquiler_id().to_string();
        let cliente_id = bici.cliente_id().to_string();
        let era_robo_local = self.alquileres_activos.iter()
            .any(|a| a.alquiler_id == alquiler_id && matches!(a.estado, EstadoAlquiler::RoboConfirmado));

        let contexto = ContextoDevolucion {
            sede_addr: ctx.address(),
            procesador_pagos_addrs: self.procesador_pagos_addrs.clone(),
            lider_cacheado: self.lider_cacheado.clone(),
            sede_id: self.sede_id,
            sedes_conocidas: self.sedes_conocidas.clone(),
            offline: self.offline.clone(),
        };
        let offline = *contexto.offline.lock().unwrap_or_else(|e| e.into_inner());
        let slots = self.slots.clone();

        Box::pin(async move {
            if let Err(respuesta) = procesar_cobro(offline, monto, &alquiler_id, &cliente_id, &contexto).await {
                return respuesta;
            }

            asegurar_y_devolver(&slots, bici, monto, dinero, era_robo_local, offline, &contexto).await
                .unwrap_or_else(|| RespuestaDevolverBici::DevolucionRechazada(String::from("No hay lugar disponible para devolver la bici en este momento")))
        })
    }
}

impl Handler<NotificarDevolucion> for Sede {
    type Result = MessageResult<NotificarDevolucion>;

    fn handle(&mut self, msg: NotificarDevolucion, ctx: &mut Self::Context) -> Self::Result {

        log!("[sede] Recibí notificación de devolución");

        let alquiler_id = msg.alquiler_id.clone();

        // Si el alquiler estaba marcado como robo, esta devolución tardía lo revierte
        // (el deposito se lo reintegra el cliente en la sede destino).
        if let Some(alquiler) = self.alquileres_activos.iter_mut().find(|a| a.alquiler_id == alquiler_id) {
            if matches!(alquiler.estado, EstadoAlquiler::RoboConfirmado) {
                alquiler.estado = EstadoAlquiler::RoboRevertido;
                log!("[sede] Robo revertido: el cliente devolvió la bici del alquiler {}", alquiler_id);
                println!("[sede] Robo revertido: el cliente devolvió la bici del alquiler {}", alquiler_id);
                ctx.address().do_send(PersistirEstado);
                return MessageResult(AckDevolucion { alquiler_id });
            }
        }

        if self.alquiler_ya_procesado(&alquiler_id) { //idempotencia
            return MessageResult(AckDevolucion {alquiler_id: msg.alquiler_id});
        }

        self.cerrar_alquiler(msg);

        MessageResult(AckDevolucion {alquiler_id: alquiler_id})
    }
}

struct RevertirRoboLocal {
    alquiler_id: String,
    reembolso_id: String,
}

impl Message for RevertirRoboLocal {
    type Result = ();
}

impl Handler<RevertirRoboLocal> for Sede {
    type Result = ();

    fn handle(&mut self, msg: RevertirRoboLocal, ctx: &mut Self::Context) {
        if let Some(alquiler) = self.alquileres_activos
            .iter_mut()
            .find(|a| a.alquiler_id == msg.alquiler_id)
        {
            alquiler.estado = EstadoAlquiler::RoboRevertido;
            alquiler.reembolso_multa_id = Some(msg.reembolso_id);
            log!("[sede] Robo revertido: el cliente devolvió la bici del alquiler {}", msg.alquiler_id);
            println!("[sede] Robo revertido: el cliente devolvió la bici del alquiler {}", msg.alquiler_id);
            ctx.address().do_send(PersistirEstado);
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

struct EncolarNotificacionPendiente {
    notificacion: NotificarDevolucion,
}

impl Message for EncolarNotificacionPendiente {
    type Result = ();
}

impl Handler<EncolarNotificacionPendiente> for Sede {
    type Result = ();

    fn handle(&mut self, msg: EncolarNotificacionPendiente, ctx: &mut Self::Context) {
        let alquiler_id = msg.notificacion.alquiler_id.clone();
        let origen = msg.notificacion.sede_origen_id;
        self.notificaciones_pendientes.push(crear_notificacion_pendiente(&msg.notificacion));
        ctx.address().do_send(PersistirEstado);
        log!("[sede {}]: no pude avisar la devolucion del alquiler {} a la sede {}, queda pendiente", self.sede_id, alquiler_id, origen);
        println!("[sede {}]: tengo una bici de la sede {} (alquiler {}) pero no pude avisarle, queda PENDIENTE", self.sede_id, origen, alquiler_id);
    }
}

struct RemoverNotificacionPendiente {
    alquiler_id: String,
}

impl Message for RemoverNotificacionPendiente {
    type Result = ();
}

impl Handler<RemoverNotificacionPendiente> for Sede {
    type Result = ();

    fn handle(&mut self, msg: RemoverNotificacionPendiente, ctx: &mut Self::Context) {
        self.notificaciones_pendientes.retain(|p| p.alquiler_id != msg.alquiler_id);
        ctx.address().do_send(PersistirEstado);
    }
}

pub(crate) struct FlushNotificacionesPendientes;

impl Message for FlushNotificacionesPendientes {
    type Result = ();
}

impl Handler<FlushNotificacionesPendientes> for Sede {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: FlushNotificacionesPendientes, ctx: &mut Self::Context) -> Self::Result {
        // Un solo flush en vuelo, sino los heartbeats mandan mucho mensajes innecesarios.
        if self.flush_en_curso {
            return Box::pin(async {});
        }
        self.flush_en_curso = true;

        let pendientes = self.notificaciones_pendientes.clone();
        let sede_addr = ctx.address();
        let sede_id = self.sede_id;
        let conocidas = self.sedes_conocidas.to_vec();
        Box::pin(async move {
            for pendiente in pendientes {
                let notificacion = notificacion_pendiente_a_devolucion(&pendiente);
                // Solo avisamos a la sede origen si esta online, si sigue caida la dejamos
                // pendiente y reintentamos cuando su heartbeat la marque viva de nuevo.
                if !origen_esta_online(pendiente.sede_origen_id, &conocidas) {
                    continue;
                }
                let addr = format!("127.0.0.1:888{}", pendiente.sede_origen_id);
                match enviar_notificacion(addr, notificacion).await {
                    Ok(ack) => {
                        log!("[sede {}]: reenvie la devolucion pendiente del alquiler {} a la sede {}", sede_id, pendiente.alquiler_id, pendiente.sede_origen_id);
                        println!("[sede {}]: avise a la sede {} que tengo su bici (alquiler {}) y cerre el pago", sede_id, pendiente.sede_origen_id, pendiente.alquiler_id);
                        sede_addr.do_send(ConfirmarNotificacionOrigen { alquiler_id: ack.alquiler_id.clone() });
                        sede_addr.do_send(RemoverNotificacionPendiente { alquiler_id: ack.alquiler_id });
                    }
                    Err(_) => {}
                }
            }
            // Se encola despues de los Remover, asi al limpiar la bandera la lista ya refleja
            // lo entregado y el proximo heartbeat solo reintenta lo que realmente quedo.
            sede_addr.do_send(FlushTerminado);
        })
    }
}

struct FlushTerminado;

impl Message for FlushTerminado {
    type Result = ();
}

impl Handler<FlushTerminado> for Sede {
    type Result = ();

    fn handle(&mut self, _msg: FlushTerminado, _ctx: &mut Self::Context) {
        self.flush_en_curso = false;
    }
}

fn origen_esta_online(sede_origen_id: u32, sedes_conocidas: &Vec<SedeInfo>) -> bool {
    for sede in sedes_conocidas {
        if sede.sede_id == sede_origen_id {
            return sede.conectada;
        }
    }
    false
}

impl Handler<GuardarDevolucion> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarDevolucion, _ctx: &mut Self::Context) -> Self::Result {
        self.devoluciones_recibidas.push(msg.devolucion);
    }
}

impl Handler<GetDevolucionesRecibidas> for Sede {
    type Result = MessageResult<GetDevolucionesRecibidas>;

    fn handle(&mut self, _msg: GetDevolucionesRecibidas, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.devoluciones_recibidas.clone())
    }
}

impl Handler<GetNotificacionesPendientes> for Sede {
    type Result = MessageResult<GetNotificacionesPendientes>;

    fn handle(&mut self, _msg: GetNotificacionesPendientes, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.notificaciones_pendientes.clone())
    }
}

pub(crate) async fn enviar_notificacion(addr: String, info: NotificarDevolucion) -> Result<AckDevolucion, String> {

    log!("[sede] Intentando notificar devolucion a {}", addr);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistencia::EstadoPersistenteSede;
    use crate::pagos::GetPagosPendientes;
    use crate::test_helpers::{sede_con_estados, sede_con_estados_path_y_procesador, sede_con_estados_y_procesador, sede_offline_con_estados_y_procesador, stub_procesador_pagos, token_valido_de_prueba};
    use matenmutex::common::MAX_VALUE;
    use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, SolicitarBici};
    use matenmutex::messages::sede_procesador::MsgSedeAProcesadorPagos;
    use matenmutex::messages::sede_cliente::RespuestaSolicitarBici;
    use matenmutex::slot::{EstadoSlot, ResultadoReserva};
    use crate::slots::ConsultarEstadoSlot;

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

        let inicio = timestamp_actual();
        let token = token_valido_de_prueba("9-9", "alq-test", "cliente-test", 1, inicio);
        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), "alq-test".to_string(), "cliente-test".to_string(), 1, inicio, token),
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
    async fn devolver_bici_offline_encola_cobro_y_reembolso_pendientes() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_offline_con_estados_y_procesador(vec![
            EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
        ], procesador_addr);

        let inicio = timestamp_actual();
        let token = token_valido_de_prueba("9-9", "alq-offline", "cliente-offline", 1, inicio);
        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), "alq-offline".to_string(), "cliente-offline".to_string(), 1, inicio, token),
                coords: (-34.6037, -58.3816),
                dinero: MAX_VALUE,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaDevolverBici::DevolucionOk(bici, ..) => {
                assert_eq!(bici.bici_id, "9-9");
            }
            other => panic!("se esperaba DevolucionOk, se obtuvo {:?}", other),
        }

        let pendientes = sede.send(GetPagosPendientes).await.unwrap();
        assert_eq!(pendientes.len(), 2, "deben quedar pendientes el cobro y el reembolso del deposito");
        assert!(pendientes.iter().any(|p| matches!(p.mensaje, MsgSedeAProcesadorPagos::CobrarPago { .. })));
        assert!(pendientes.iter().any(|p| matches!(p.mensaje, MsgSedeAProcesadorPagos::ReembolsarPago { .. })));
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
                dinero: 999999.0,
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
                dinero: 999999.0,

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
                dinero: 999999.0,

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
    async fn devolver_bici_sin_dinero_suficiente_rechaza() {
        let sede = sede_con_estados(vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }]);

        let respuesta = sede
            .send(DevolverBici {
                bici: BiciAlquilada::new("9-9".to_string(), String::new(), String::new(), 1, 0, String::new()),
                coords: (0.0, 0.0),
                dinero: 0.0,

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
    async fn devolver_bici_persiste_devolucion_y_remueve_alquiler_activo() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let path = crate::test_helpers::path_temporal("estado_sede_devolver");
        let sede = sede_con_estados_path_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }, EstadoSlot::Vacio],
            procesador_addr,
            path.clone(),
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: MAX_VALUE,
            })
            .await
            .unwrap();
        let bici = match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => bici,
            other => panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo {:?}", other),
        };
        let alquiler_id = bici.alquiler_id().to_string();
        sede.send(AckComprobanteAlquiler { alquiler_id: alquiler_id.clone() }).await.unwrap();

        sede.send(DevolverBici {
            bici,
            coords: (-34.6037, -58.3816),
            dinero: MAX_VALUE,
        })
            .await
            .unwrap();

        let contenido = std::fs::read_to_string(&path).expect("el snapshot debe haberse escrito");
        let snapshot: EstadoPersistenteSede = serde_json::from_str(&contenido).unwrap();

        assert!(
            !snapshot.alquileres_activos.iter().any(|a| a.alquiler_id == alquiler_id),
            "el alquiler debe haberse removido de alquileres_activos"
        );
        assert!(
            snapshot.devoluciones_recibidas.iter().any(|d| d.alquiler_id == alquiler_id),
            "debe registrarse la devolucion"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.tmp"));
    }
}
