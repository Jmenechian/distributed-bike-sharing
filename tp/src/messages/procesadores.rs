use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum MsgProcesador {
    Hola { procesador_id: u32 },
    Heartbeat { procesador_id: u32 },
    HeartbeatAck { procesador_id: u32 },
}
