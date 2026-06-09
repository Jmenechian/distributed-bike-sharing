use serde::{Deserialize, Serialize};
use crate::types::{Bici, BiciAlquilada};
#[derive(Serialize, Deserialize, Debug)]
pub enum RespuestaSolicitarBici {
    ComprobanteAlquilerEmitido(BiciAlquilada, u32),
    AlquilerConfirmado(BiciAlquilada),
    AlquilerRechazado(String),
}

#[derive(Serialize, Deserialize)]
pub enum RespuestaDevolverBici {
    DevolucionOk(Bici, u32),
    DevolucionRechazada(String),
}