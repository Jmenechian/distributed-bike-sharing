use matenmutex::messages::sede_sede::SedeInfo;
use matenmutex::types::BiciAlquilada;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader as StdBufReader;

use super::Cliente;

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct EstadoPersistenteCliente {
    pub(crate) coords: (f64, f64),
    pub(crate) bici_activa: Option<BiciAlquilada>,
    pub(crate) dinero: f64,
    pub(crate) sedes_conocidas: Vec<SedeInfo>,
}

pub(crate) fn estado_path_de_id(cliente_id: u32) -> String {
    format!("estado_cliente_{}.json", cliente_id)
}

pub(crate) fn cargar_estado_cliente(path: &str) -> Option<EstadoPersistenteCliente> {
    let file = File::open(path).ok()?;
    serde_json::from_reader(StdBufReader::new(file)).ok()
}

fn escribir_snapshot_a(estado_path: &str, snapshot: &EstadoPersistenteCliente) {
    if estado_path.is_empty() {
        return;
    }

    let json = match serde_json::to_vec_pretty(snapshot) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("[persistencia] no se pudo serializar el estado: {e}");
            return;
        }
    };
    let tmp = format!("{}.tmp", estado_path);
    if let Err(e) = std::fs::write(&tmp, &json).and_then(|_| std::fs::rename(&tmp, estado_path)) {
        println!("[persistencia] no se pudo escribir {}: {e}", estado_path);
    }
}

impl Cliente {
    pub(crate) fn persistir_estado(&self) {
        let path = estado_path_de_id(self.cliente_id);
        let snapshot = EstadoPersistenteCliente {
            coords: self.coords,
            bici_activa: self.bici_activa.clone(),
            dinero: self.dinero,
            sedes_conocidas: self.sedes_conocidas.clone(),
        };
        escribir_snapshot_a(&path, &snapshot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estado_path_de_id_usa_el_id_del_cliente() {
        assert_eq!(estado_path_de_id(42), "estado_cliente_42.json");
    }

    #[test]
    fn cargar_estado_de_archivo_inexistente_devuelve_none() {
        assert!(cargar_estado_cliente("no_existe_este_archivo.json").is_none());
    }
}
