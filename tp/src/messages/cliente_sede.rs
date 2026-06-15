use actix::Message;
use serde::{Deserialize, Serialize};
use crate::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use crate::messages::sede_sede::SedeInfo;
use crate::types::BiciAlquilada;

#[derive(Serialize, Deserialize)]
pub struct SolicitarBici {
    pub cliente_id: u32,
    pub coords: (f64, f64),
    pub dinero: f64,
}

#[derive(Serialize, Deserialize)]
pub struct AckComprobanteAlquiler {
    pub alquiler_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct DevolverBici {
    pub bici: BiciAlquilada,
    pub coords: (f64, f64),
    pub dinero: f64,
}

#[derive(Serialize, Deserialize)]

pub struct PedirSedes {
    pub coords: (f64, f64),
}
#[derive(Serialize, Deserialize)]

pub struct PreguntarBicis; // No esta en el readme

#[derive(Serialize, Deserialize)]
pub struct Handshake {
    pub coords: (f64, f64),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum RespuestaHandshake {
    Aceptado((f64, f64)),
    Rechazado(String),
}

impl Message for Handshake {
    type Result = RespuestaHandshake;
}

#[derive(Serialize, Deserialize)]
pub enum MsgClienteASede {
    Handshake(Handshake),
    SolicitarBici(SolicitarBici),
    AckComprobanteAlquiler(AckComprobanteAlquiler),
    DevolverBici(DevolverBici),
    PreguntarBicis(PreguntarBicis),
    PedirSedes(PedirSedes),
}

impl Message for SolicitarBici {
    type Result = RespuestaSolicitarBici;
}

impl Message for AckComprobanteAlquiler {
    type Result = RespuestaSolicitarBici;
}

impl Message for DevolverBici {
    type Result = RespuestaDevolverBici;
}

impl Message for PreguntarBicis {
    type Result = String;
}

impl Message for PedirSedes {
    type Result = Vec<SedeInfo>;
}
