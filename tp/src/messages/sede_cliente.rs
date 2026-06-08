use serde::{Deserialize, Serialize};
use crate::types::{Bici, BiciAlquilada};
#[derive(Serialize, Deserialize, Debug)]
pub enum RespuestaSolicitarBici {
    ComprobanteAlquilerEmitido(BiciAlquilada),
    AlquilerConfirmado(BiciAlquilada),
    AlquilerRechazado(String),
}

#[derive(Serialize, Deserialize)]
pub enum RespuestaDevolverBici {
    DevolucionOk(Bici),
    DevolucionRechazada(String),
}