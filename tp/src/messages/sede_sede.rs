use actix::Message;
use serde::{Deserialize, Serialize};
use crate::types::timestamp_actual;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum MsgSedeHeartbeat {
    Heartbeat(Heartbeat),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Heartbeat {
    pub sede_id: u32,
    pub info: SedeInfo,
}

impl Message for Heartbeat {
    type Result = ();
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum MsgSedeASede {
    NotificarDevolucion(NotificarDevolucion),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SedeInfo{
    pub sede_id: u32,
    pub coords: (f64, f64),
    pub public_addr: String,
    pub private_addr: String,
    pub slots_vacios: u8,
    pub slots_disponibles: u8,
    pub conectada: bool,
    pub ultimo_update_ts: u64
}

pub fn crear_info_dummy() -> SedeInfo {
    SedeInfo {
        sede_id: 0,
        coords: (90.0, 90.0),
        public_addr: String::from("127.0.0.1:7001"),
        private_addr: String::from("127.0.0.1:7001"),
        slots_vacios: 0,
        slots_disponibles: 0,
        conectada: true,
        ultimo_update_ts: timestamp_actual(),
    }
}

pub struct ActualizarDisponibilidadSolicitar;

impl Message for ActualizarDisponibilidadSolicitar {
    type Result = ();
}

pub struct ActualizarDisponibilidadDevolver;

impl Message for ActualizarDisponibilidadDevolver {
    type Result = ();
}

#[derive(Serialize, Deserialize, Clone, Debug)]

pub struct GetInfo;

impl Message for GetInfo {
    type Result = SedeInfo;
}

pub struct GetSedesConocidas;

impl Message for GetSedesConocidas {
    type Result = Vec<SedeInfo>;
}


// ##############################
// Alquileres
// ##############################

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AlquilerActivo {
    pub alquiler_id: String, //
    pub bici_id: String, //
    pub cliente_id: String, //
    pub sede_origen_id: u32, //
    pub inicio_ts: u64, //
    pub pago_preautorizacion_id: String,
    pub deadline_devolucion_ts: u64,
    pub estado: EstadoAlquiler,
    pub pago_multa_id: Option<String>,
    pub reembolso_multa_id: Option<String>,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EstadoAlquiler {
    Activo,
    Sospechoso,
    Cerrado,
    RoboConfirmado,
    RoboRevertido,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DevolucionRecibida {
    pub alquiler_id: String,
    pub bici_id: String,
    pub cliente_id: String,
    pub sede_origen_id: u32,
    pub sede_destino_id: u32,
    pub fin_ts: u64,
    pub monto: f64,
    pub pago_id: Option<String>,
    pub estado_pago: EstadoConciliacion,
    pub estado_notificacion_origen: EstadoConciliacion,
}

pub struct PagoPendiente {
    payment_request_id: String,
    alquiler_id: String,
    cliente_id: String,
    monto: f64,
    tipo: TipoPagoPendiente,
    intentos: u8,
}

pub enum TipoPagoPendiente {
    PreAutorizacion,
    CobroFinal,
    MultaRobo,
    ReembolsoPenalizacion,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificacionPendiente {
    pub alquiler_id: String,
    pub bici_id: String,
    pub cliente_id: String,
    pub sede_origen_id: u32,
    pub sede_destino_id: u32,
    pub fin_ts: u64,
    pub monto: f64,
    pub pago_id: Option<String>,
    pub token_alquiler: String,
    pub intentos: u8,
}



#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EstadoConciliacion {
    Pendiente,
    Confirmado,
    Fallido,
}

pub struct GuardarAlquiler {
    pub alquiler: AlquilerActivo,
}

impl Message for GuardarAlquiler {
    type Result = ();
}

pub struct GuardarDevolucion {
    pub devolucion: DevolucionRecibida,
}

impl Message for GuardarDevolucion {
    type Result = ();
}

pub struct RemoverAlquiler {
    pub alquiler_id: String,
}

impl Message for RemoverAlquiler {
    type Result = ();
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NotificarDevolucion {
    pub alquiler_id: String,
    pub bici_id: String,
    pub cliente_id: String,
    pub sede_origen_id: u32,
    pub sede_destino_id: u32,
    pub fin_ts: u64,
    pub monto: f64,
    pub pago_id: Option<String>,
    pub token_alquiler: String,
}

impl Message for NotificarDevolucion {
    type Result = AckDevolucion;
}

#[derive(Serialize, Deserialize)]

pub struct AckDevolucion {
    pub alquiler_id: String,
    // pub sede_origen_id: u32,
    // pub sede_destino_id: u32,
}

pub struct VerificarRoboMasTarde {
    pub alquiler_id: String,
    pub slot_idx: usize,
}

impl Message for VerificarRoboMasTarde {
    type Result = ();
}

pub struct GetAlquileresActivos;

impl Message for GetAlquileresActivos {
    type Result = Vec<AlquilerActivo>;
}

pub struct GetDevolucionesRecibidas;

impl Message for GetDevolucionesRecibidas {
    type Result = Vec<DevolucionRecibida>;
}

pub struct GetNotificacionesPendientes;

impl Message for GetNotificacionesPendientes {
    type Result = Vec<NotificacionPendiente>;
}