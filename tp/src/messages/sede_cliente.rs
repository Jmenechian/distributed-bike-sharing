use serde::{Deserialize, Serialize};
use crate::types::{Bici, BiciAlquilada};
#[derive(Serialize, Deserialize)]
pub enum RespuestaSolicitarBici {
    AlquilerConfirmado(BiciAlquilada),
    AlquilerRechazado(String)
}

#[derive(Serialize, Deserialize)]
pub enum RespuestaDevolverBici {
    DevolucionOk(Bici),
    DevolucionRechazada(String),
}