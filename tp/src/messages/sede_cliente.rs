use serde::{Deserialize, Serialize};
use crate::types::BiciAlquilada;
#[derive(Serialize, Deserialize)]
pub enum RespuestaSolicitarBici {
    AlquilerConfirmado(BiciAlquilada),
    AlquilerRechazado(String)
}