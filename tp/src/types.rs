use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn timestamp_actual() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

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
    pub fn new(bici_id: String, alquiler_id: String, cliente_id: String, sede_origen_id: u32, inicio_ts: u64, token_alquiler: String) -> Self {
        BiciAlquilada{
            bici_id,
            alquiler_id,
            cliente_id,
            sede_origen_id,
            inicio_ts,
            token_alquiler,
        }
    }

    pub fn bici_id(&self) -> &str {
        &self.bici_id
    }

    pub fn alquiler_id(&self) -> &str {
        &self.alquiler_id
    }

    pub fn cliente_id(&self) -> &str {
        &self.cliente_id
    }

    pub fn sede_origen_id(&self) -> u32 {
        self.sede_origen_id
    }

    pub fn inicio_ts(&self) -> u64 {
        self.inicio_ts
    }

    pub fn token_alquiler(&self) -> &str {
        &self.token_alquiler
    }
}