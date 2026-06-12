use actix::Message;
use serde::{Deserialize, Serialize};
use crate::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use crate::messages::sedes::SedeInfo;
use crate::types::BiciAlquilada;

#[derive(Serialize, Deserialize)]
pub struct SolicitarBici {
    pub cliente_id: u32,
    pub coords: (f64, f64),
    pub dinero: u32,
}

#[derive(Serialize, Deserialize)]
pub struct AckComprobanteAlquiler {
    pub alquiler_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct DevolverBici {
    pub bici: BiciAlquilada,
    pub coords: (f64, f64),
    pub dinero: u32,
}

#[derive(Serialize, Deserialize)]

pub struct PedirSedes {
    pub coords: (f64, f64),
}
#[derive(Serialize, Deserialize)]

pub struct PreguntarBicis; // No esta en el readme

#[derive(Serialize, Deserialize)]
pub enum MsgClienteASede {
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
