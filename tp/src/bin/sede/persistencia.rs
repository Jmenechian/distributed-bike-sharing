use actix::{Addr, Context, Handler, Message, ResponseFuture};
use matenmutex::log;
use matenmutex::messages::sede_sede::{AlquilerActivo, DevolucionRecibida, NotificacionPendiente};
use matenmutex::slot::{EstadoSlot, EstadoSlotPersistido, ExportarEstado, Slot};
use matenmutex::types::{BiciAlquilada, PagoPendiente};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader as StdBufReader;

use crate::Sede;

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct EstadoPersistenteSede {
    pub(crate) slots: Vec<EstadoSlotPersistido>,
    pub(crate) pending_reservas: Vec<(String, usize, BiciAlquilada)>,
    pub(crate) alquileres_activos: Vec<AlquilerActivo>,
    pub(crate) devoluciones_recibidas: Vec<DevolucionRecibida>,
    pub(crate) pagos_pendientes: Vec<PagoPendiente>,
    pub(crate) notificaciones_pendientes: Vec<NotificacionPendiente>,
}

pub(crate) fn estado_path_de_id(sede_id: u32) -> String {
    format!("estado_sede_{}.json", sede_id)
}

/// Lee el snapshot de disco. Si el archivo no existe, arranca con estado vacio (primer boot).
pub(crate) fn cargar_estado(path: &str) -> Result<EstadoPersistenteSede, Box<dyn std::error::Error>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(EstadoPersistenteSede::default()),
        Err(e) => return Err(format!("no se pudo abrir el estado en {path}: {e}").into()),
    };
    serde_json::from_reader(StdBufReader::new(file))
        .map_err(|e| format!("estado corrupto en {path}: {e}").into())
}

fn escribir_snapshot_a(estado_path: &str, snapshot: &EstadoPersistenteSede) {
    if estado_path.is_empty() {
        return;
    }
    let json = match serde_json::to_vec_pretty(snapshot) {
        Ok(bytes) => bytes,
        Err(e) => {
            log!("[persistencia] no se pudo serializar el estado: {e}");
            return;
        }
    };
    let tmp = format!("{}.tmp", estado_path);
    if let Err(e) = std::fs::write(&tmp, &json).and_then(|_| std::fs::rename(&tmp, estado_path)) {
        log!("[persistencia] no se pudo escribir {}: {e}", estado_path);
    }
}

pub(crate) struct PersistirEstado;

impl Message for PersistirEstado {
    type Result = ();
}

impl Handler<PersistirEstado> for Sede {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: PersistirEstado, _ctx: &mut Context<Self>) -> Self::Result {
        let slots = self.slots.clone();
        let pending_reservas: Vec<(String, usize, BiciAlquilada)> = self
            .pending_reservas
            .iter()
            .map(|(alquiler_id, (slot_idx, bici))| (alquiler_id.clone(), *slot_idx, bici.clone()))
            .collect();
        let alquileres_activos = self.alquileres_activos.clone();
        let devoluciones_recibidas = self.devoluciones_recibidas.clone();
        let pagos_pendientes = self.pagos_pendientes.clone();
        let notificaciones_pendientes = self.notificaciones_pendientes.clone();
        let estado_path = self.estado_path.clone();

        Box::pin(async move {
            let slot_states = match Sede::snapshot_slots(&slots).await {
                Some(estados) => estados,
                None => return,
            };

            let snapshot = EstadoPersistenteSede {
                slots: slot_states,
                pending_reservas,
                alquileres_activos,
                devoluciones_recibidas,
                pagos_pendientes,
                notificaciones_pendientes,
            };

            escribir_snapshot_a(&estado_path, &snapshot);
        })
    }
}

impl Sede {
    pub(crate) async fn snapshot_slots(slots: &[Addr<Slot>]) -> Option<Vec<EstadoSlotPersistido>> {
        let mut estados = Vec::with_capacity(slots.len());
        for slot in slots {
            match slot.send(ExportarEstado).await {
                Ok(estado) => estados.push(estado),
                Err(e) => {
                    log!("[persistencia] no se pudo exportar el estado de un slot: {e}");
                    return None;
                }
            }
        }
        Some(estados)
    }
}

pub(crate) fn construir_estados_con_limbo(estado: &EstadoPersistenteSede) -> Vec<EstadoSlot> {
    estado
        .slots
        .iter()
        .cloned()
        .map(|slot_persistido| match slot_persistido {
            EstadoSlotPersistido::Reservado { bici_id } => {
                let respaldado = estado.alquileres_activos.iter().any(|a| a.bici_id == bici_id);
                if respaldado {
                    EstadoSlot::Reservado { bici_id }
                } else {
                    EstadoSlot::Disponible { bici_id }
                }
            }
            otro => EstadoSlot::from(otro),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alquileres::crear_alquiler_activo;
    use crate::test_helpers::path_temporal;
    use matenmutex::types::BiciAlquilada;

    #[test]
    fn cargar_estado_sin_archivo_devuelve_default() {
        let path = path_temporal("estado_sede_inexistente");
        let estado = cargar_estado(&path).unwrap();
        assert!(estado.slots.is_empty());
        assert!(estado.alquileres_activos.is_empty());
    }

    #[test]
    fn cargar_estado_con_archivo_corrupto_aborta() {
        let path = path_temporal("estado_sede_corrupto");
        std::fs::write(&path, "esto no es json valido").unwrap();

        let resultado = cargar_estado(&path);
        assert!(resultado.is_err());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cargar_estado_con_archivo_valido_rehidrata_colecciones() {

        let path = path_temporal("estado_sede_valido");
        let bici = BiciAlquilada::new("1-2".to_string(), "alq-1".to_string(), "c-1".to_string(), 1, 1000, "tok".to_string());
        let alquiler = crear_alquiler_activo(&bici, "hola".parse().unwrap());
        let snapshot = EstadoPersistenteSede {
            slots: vec![EstadoSlotPersistido::Vacio, EstadoSlotPersistido::Disponible { bici_id: "1-1".to_string() }],
            pending_reservas: vec![],
            alquileres_activos: vec![alquiler],
            devoluciones_recibidas: vec![],
            pagos_pendientes: vec![],
            notificaciones_pendientes: vec![],
        };
        std::fs::write(&path, serde_json::to_vec_pretty(&snapshot).unwrap()).unwrap();

        let estado = cargar_estado(&path).unwrap();
        assert_eq!(estado.slots.len(), 2);
        assert_eq!(estado.alquileres_activos.len(), 1);
        assert_eq!(estado.alquileres_activos[0].alquiler_id, "alq-1");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn construir_estados_con_limbo_downgradea_reservado_sin_alquiler_activo() {
        let estado = EstadoPersistenteSede {
            slots: vec![EstadoSlotPersistido::Reservado { bici_id: "1-1".to_string() }],
            pending_reservas: vec![],
            alquileres_activos: vec![],
            devoluciones_recibidas: vec![],
            pagos_pendientes: vec![],
            notificaciones_pendientes: vec![],
        };

        let estados = construir_estados_con_limbo(&estado);

        match &estados[0] {
            EstadoSlot::Disponible { bici_id } => assert_eq!(bici_id, "1-1"),
            _ => panic!("se esperaba Disponible (limbo downgrade)"),
        }
    }

    #[test]
    fn construir_estados_con_limbo_mantiene_reservado_con_alquiler_activo() {
        let bici = BiciAlquilada::new("1-1".to_string(), "alq-1".to_string(), "c-1".to_string(), 1, 1000, "tok".to_string());
        let alquiler = crear_alquiler_activo(&bici, "hola".parse().unwrap());
        let estado = EstadoPersistenteSede {
            slots: vec![EstadoSlotPersistido::Reservado { bici_id: "1-1".to_string() }],
            pending_reservas: vec![],
            alquileres_activos: vec![alquiler],
            devoluciones_recibidas: vec![],
            pagos_pendientes: vec![],
            notificaciones_pendientes: vec![],
        };

        let estados = construir_estados_con_limbo(&estado);

        match &estados[0] {
            EstadoSlot::Reservado { bici_id } => assert_eq!(bici_id, "1-1"),
            _ => panic!("se esperaba Reservado (respaldado por AlquilerActivo)"),
        }
    }

    #[test]
    fn construir_estados_con_limbo_mapea_vacio_y_disponible_sin_cambios() {
        let estado = EstadoPersistenteSede {
            slots: vec![EstadoSlotPersistido::Vacio, EstadoSlotPersistido::Disponible { bici_id: "1-2".to_string() }],
            pending_reservas: vec![],
            alquileres_activos: vec![],
            devoluciones_recibidas: vec![],
            pagos_pendientes: vec![],
            notificaciones_pendientes: vec![],
        };

        let estados = construir_estados_con_limbo(&estado);

        match &estados[0] {
            EstadoSlot::Vacio => {}
            _ => panic!("se esperaba Vacio"),
        }
        match &estados[1] {
            EstadoSlot::Disponible { bici_id } => assert_eq!(bici_id, "1-2"),
            _ => panic!("se esperaba Disponible"),
        }
    }
}
