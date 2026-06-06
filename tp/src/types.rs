use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Bici {
    pub bici_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct BiciAlquilada {
    bici_id: String,
    alquiler_id: String,
    cliente_id: String,
    sede_origen_id: u32,
    inicio_ts: u64,
    token_alquiler: String,
}

impl BiciAlquilada {
    pub fn new(
        bici_id: String,
        alquiler_id: String,
        cliente_id: String,
        sede_origen_id: u32,
        inicio_ts: u64,
        token_alquiler: String,
    ) -> Self {
        BiciAlquilada {
            bici_id,
            alquiler_id,
            cliente_id,
            sede_origen_id,
            inicio_ts,
            token_alquiler,
        }
    }
}
