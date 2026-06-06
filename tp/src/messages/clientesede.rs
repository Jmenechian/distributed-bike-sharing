use crate::messages::sede_cliente::RespuestaSolicitarBici;
use crate::types::Bici;
use actix::Message;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SolicitarBici {
    pub cliente_id: u32,
    pub coords: (f64, f64),
}

#[derive(Serialize, Deserialize)]
pub struct DevolverBici {
    pub bici: Bici,
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
    type Result = Option<Bici>;
}

impl Message for PreguntarBicis {
    type Result = String;
}
