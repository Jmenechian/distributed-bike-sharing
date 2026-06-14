use serde::{Deserialize, Serialize};
use crate::types::{Bici, BiciAlquilada};
#[derive(Serialize, Deserialize, Debug)]
pub enum RespuestaSolicitarBici {
    ComprobanteAlquilerEmitido(BiciAlquilada, f64),
    AlquilerConfirmado(BiciAlquilada),
    AlquilerRechazado(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RespuestaDevolverBici {
    DevolucionOk(Bici, f64),
    DevolucionRechazada(String),
}