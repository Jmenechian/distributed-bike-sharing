use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum MsgProcesador {
    Heartbeat { procesador_id: u32, es_lider: bool },
    HeartbeatAck { procesador_id: u32 },
    Eleccion { procesador_id: u32 },
    Ok { procesador_id: u32 },
    Coordinador { lider_id: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_serializaion_y_deserializacion_de_cada_variante() {
        let mensajes = [
            MsgProcesador::Heartbeat { procesador_id: 2, es_lider: false },
            MsgProcesador::HeartbeatAck { procesador_id: 3 },
            MsgProcesador::Eleccion { procesador_id: 4 },
            MsgProcesador::Ok { procesador_id: 5 },
            MsgProcesador::Coordinador { lider_id: 6 },
        ];
        for msg in mensajes {
            let bytes = serde_json::to_vec(&msg).expect("serializa");
            let recuperado: MsgProcesador = serde_json::from_slice(&bytes).expect("deserializa");
            assert_eq!(msg, recuperado);
        }
    }
}
