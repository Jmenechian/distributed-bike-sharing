use serde::{Deserialize, Serialize};

use crate::messages::sede_procesador::MsgSedeAProcesadorPagos;
use crate::types::{PagoProcesado, Voto2PC};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MsgProcesadorAProcesador {
    PrepararPago {
        payment_request_id: String,
        pago: MsgSedeAProcesadorPagos,
    },
    VotoPago {
        payment_request_id: String,
        votante_id: u32,
        voto: Voto2PC,
    },
    CommitPago {
        payment_request_id: String,
    },
    AbortPago {
        payment_request_id: String,
    },
    AckDecisionPago {
        payment_request_id: String,
        votante_id: u32,
    },
    ConsultarPago {
        payment_request_id: String,
    },
    RespuestaConsultaPago {
        payment_request_id: String,
        pago: Option<PagoProcesado>,
    },
}

impl MsgProcesadorAProcesador {
    /// Texto para los logs, mismo formato que usamos para Sede<->Procesador:
    /// Variante (campo valor, campo valor).
    pub fn describir(&self) -> String {
        match self {
            MsgProcesadorAProcesador::PrepararPago { payment_request_id, pago } => {
                format!(
                    "PrepararPago (payment_request_id {}, pago {})",
                    payment_request_id,
                    pago.describir()
                )
            }
            MsgProcesadorAProcesador::VotoPago { payment_request_id, votante_id, voto } => {
                format!(
                    "VotoPago (payment_request_id {}, votante_id {}, voto {:?})",
                    payment_request_id, votante_id, voto
                )
            }
            MsgProcesadorAProcesador::CommitPago { payment_request_id } => {
                format!("CommitPago (payment_request_id {})", payment_request_id)
            }
            MsgProcesadorAProcesador::AbortPago { payment_request_id } => {
                format!("AbortPago (payment_request_id {})", payment_request_id)
            }
            MsgProcesadorAProcesador::AckDecisionPago { payment_request_id, votante_id } => {
                format!(
                    "AckDecisionPago (payment_request_id {}, votante_id {})",
                    payment_request_id, votante_id
                )
            }
            MsgProcesadorAProcesador::ConsultarPago { payment_request_id } => {
                format!("ConsultarPago (payment_request_id {})", payment_request_id)
            }
            MsgProcesadorAProcesador::RespuestaConsultaPago { payment_request_id, pago } => {
                let pago_str = match pago {
                    None => "pago None".to_string(),
                    Some(p) => format!("pago Some({:?})", p),
                };
                format!(
                    "RespuestaConsultaPago (payment_request_id {}, {})",
                    payment_request_id, pago_str
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EstadoPago, TipoOperacionPago};

    #[test]
    fn preparar_pago_round_trip_y_describir_incluye_pago_anidado() {
        let pago = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "pr-1".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 100.0,
        };
        let msg = MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-1".to_string(),
            pago: pago.clone(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);

        let descripcion = msg.describir();
        assert!(descripcion.contains("pr-1"));
        assert!(descripcion.contains(&pago.describir()));
    }

    #[test]
    fn voto_pago_round_trip_y_describir() {
        let msg = MsgProcesadorAProcesador::VotoPago {
            payment_request_id: "pr-2".to_string(),
            votante_id: 2,
            voto: Voto2PC::Commit,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
        assert!(!msg.describir().is_empty());
    }

    #[test]
    fn voto_pago_abort_round_trip() {
        let msg = MsgProcesadorAProcesador::VotoPago {
            payment_request_id: "pr-2b".to_string(),
            votante_id: 3,
            voto: Voto2PC::Abort,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }

    #[test]
    fn commit_pago_round_trip_y_describir() {
        let msg = MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-3".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
        assert!(!msg.describir().is_empty());
    }

    #[test]
    fn abort_pago_round_trip_y_describir() {
        let msg = MsgProcesadorAProcesador::AbortPago {
            payment_request_id: "pr-4".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
        assert!(!msg.describir().is_empty());
    }

    #[test]
    fn ack_decision_pago_round_trip_y_describir() {
        let msg = MsgProcesadorAProcesador::AckDecisionPago {
            payment_request_id: "pr-5".to_string(),
            votante_id: 4,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
        assert!(!msg.describir().is_empty());
    }

    #[test]
    fn consultar_pago_round_trip_y_describir() {
        let msg = MsgProcesadorAProcesador::ConsultarPago {
            payment_request_id: "pr-6".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
        assert!(!msg.describir().is_empty());
    }

    #[test]
    fn respuesta_consulta_pago_round_trip_con_y_sin_pago() {
        let msg_none = MsgProcesadorAProcesador::RespuestaConsultaPago {
            payment_request_id: "pr-7".to_string(),
            pago: None,
        };
        let json = serde_json::to_string(&msg_none).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg_none);
        assert!(msg_none.describir().contains("pago None"));

        let pago = PagoProcesado {
            payment_request_id: "pr-7".to_string(),
            pago_id: "pago-7".to_string(),
            alquiler_id: None,
            cliente_id: "c-7".to_string(),
            monto: 75.0,
            tipo: TipoOperacionPago::PreAutorizacion,
            estado: EstadoPago::PreAutorizado,
        };
        let msg_some = MsgProcesadorAProcesador::RespuestaConsultaPago {
            payment_request_id: "pr-7".to_string(),
            pago: Some(pago),
        };
        let json = serde_json::to_string(&msg_some).unwrap();
        let de: MsgProcesadorAProcesador = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg_some);
        assert!(msg_some.describir().contains("pago Some("));
    }
}
