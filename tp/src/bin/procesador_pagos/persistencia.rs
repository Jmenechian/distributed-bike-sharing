use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use matenmutex::log;
use matenmutex::messages::sede_procesador::MsgSedeAProcesadorPagos;
use matenmutex::types::{PagoProcesado, TransaccionPago2PC};
use serde::{Deserialize, Serialize};

use super::ProcesadorPagos;

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct EstadoPersistente {
    pub(crate) pagos_procesados: Vec<PagoProcesado>,
    pub(crate) transacciones_en_curso: Vec<TransaccionPago2PC>,
    pub(crate) preparados_replica: HashMap<String, MsgSedeAProcesadorPagos>,
}

pub(crate) fn estado_path_de_id(procesador_id: u32) -> String {
    format!("estado_procesador_{}.json", procesador_id)
}

pub(crate) fn cargar_estado(path: &str) -> Result<EstadoPersistente, Box<dyn std::error::Error>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(EstadoPersistente::default()),
        Err(e) => return Err(format!("no se pudo abrir el estado en {path}: {e}").into()),
    };
    serde_json::from_reader(BufReader::new(file))
        .map_err(|e| format!("estado corrupto en {path}: {e}").into())
}

impl ProcesadorPagos {
    pub(crate) fn persistir(&self) {
        if self.estado_path.is_empty() {
            return;
        }
        let snapshot = EstadoPersistente {
            pagos_procesados: self.pagos_procesados.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            transacciones_en_curso: self.transacciones_en_curso.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            preparados_replica: self.preparados_replica.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        };
        let json = match serde_json::to_vec_pretty(&snapshot) {
            Ok(bytes) => bytes,
            Err(e) => {
                log!("[persistencia] no se pudo serializar el estado: {e}");
                return;
            }
        };
        let tmp = format!("{}.tmp", self.estado_path);
        if let Err(e) = std::fs::write(&tmp, &json).and_then(|_| std::fs::rename(&tmp, &self.estado_path)) {
            log!("[persistencia] no se pudo escribir {}: {e}", self.estado_path);
        }
    }
}
