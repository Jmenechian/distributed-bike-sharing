use actix::Message;
use serde::{Deserialize, Serialize};
use crate::types::timestamp_actual;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum MsgSedeASede {
    Heartbeat(Heartbeat),
    // HeartbeatAck { sede_id: u32 },
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