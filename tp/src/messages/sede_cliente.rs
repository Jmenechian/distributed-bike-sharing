use crate::types::BiciAlquilada;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub enum RespuestaSolicitarBici {
    AlquilerConfirmado(BiciAlquilada),
    AlquilerRechazado(String),
}
