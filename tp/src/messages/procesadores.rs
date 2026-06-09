use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum MsgProcesador {
    Hola { procesador_id: u32 },
    Heartbeat { procesador_id: u32 },
    HeartbeatAck { procesador_id: u32 },
    /// Bully: inicia/propaga una elección. Se envía a los de id mayor.
    Eleccion { procesador_id: u32 },
    /// Bully: respuesta de un id mayor que toma la posta ("callate, sigo yo").
    Ok { procesador_id: u32 },
    /// Bully: el ganador se proclama nuevo líder ante todos.
    Coordinador { lider_id: u32 },
}
