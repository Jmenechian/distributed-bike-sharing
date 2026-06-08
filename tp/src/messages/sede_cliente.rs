use crate::types::BiciAlquilada;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub enum RespuestaSolicitarBici {
    AlquilerConfirmado(BiciAlquilada),
    AlquilerRechazado(String),
}

#[derive(Serialize, Deserialize)]
pub enum RespuestaDevolucionBici {
    DevolucionOk{monto: f64, duracion: u64},
    DevolucionRechazada(String),
}