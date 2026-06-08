use crate::messages::sede_cliente::{RespuestaDevolucionBici, RespuestaSolicitarBici};
use crate::types::{Bici, BiciAlquilada};
use actix::Message;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SolicitarBici {
    pub cliente_id: u32,
    pub coords: (f64, f64),
}

#[derive(Serialize, Deserialize)]
pub struct DevolverBici {
    pub bici: BiciAlquilada,
    pub coords: (f64, f64),
}

#[derive(Serialize, Deserialize)]

pub struct PreguntarBicis; // No esta en el readme

#[derive(Serialize, Deserialize)]
pub enum MsgClienteASede {
    SolicitarBici(SolicitarBici),
    DevolverBici(DevolverBici),
    PreguntarBicis(PreguntarBicis),
}

impl Message for SolicitarBici {
    type Result = RespuestaSolicitarBici;
}

impl Message for DevolverBici {
    type Result = RespuestaDevolucionBici;
}

impl Message for PreguntarBicis {
    type Result = String;
}
