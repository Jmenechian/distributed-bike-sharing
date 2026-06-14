use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MsgSedeAProcesadorPagos {
    PreAutorizarPago {
        payment_request_id: String,
        cliente_id: String,
        monto_seguridad: f64,
    },
    CobrarPago {
        payment_request_id: String,
        alquiler_id: String,
        cliente_id: String,
        monto_final: f64,
    },
    ReembolsarPago {
        payment_request_id: String,
        alquiler_id: String,
        cliente_id: String,
        pago_original_id: String,
        monto_reembolso: f64,
    },
}

impl MsgSedeAProcesadorPagos {
    pub fn payment_request_id(&self) -> &str {
        match self {
            MsgSedeAProcesadorPagos::PreAutorizarPago { payment_request_id, .. } => payment_request_id,
            MsgSedeAProcesadorPagos::CobrarPago { payment_request_id, .. } => payment_request_id,
            MsgSedeAProcesadorPagos::ReembolsarPago { payment_request_id, .. } => payment_request_id,
        }
    }

    pub fn describir(&self) -> String {
        match self {
            MsgSedeAProcesadorPagos::PreAutorizarPago { payment_request_id, cliente_id, monto_seguridad } => {
                format!(
                    "PreAutorizarPago (payment_request_id {}, cliente_id {}, monto_seguridad {})",
                    payment_request_id, cliente_id, monto_seguridad
                )
            }
            MsgSedeAProcesadorPagos::CobrarPago { payment_request_id, alquiler_id, cliente_id, monto_final } => {
                format!(
                    "CobrarPago (payment_request_id {}, alquiler_id {}, cliente_id {}, monto_final {})",
                    payment_request_id, alquiler_id, cliente_id, monto_final
                )
            }
            MsgSedeAProcesadorPagos::ReembolsarPago { payment_request_id, alquiler_id, cliente_id, pago_original_id, monto_reembolso } => {
                format!(
                    "ReembolsarPago (payment_request_id {}, alquiler_id {}, cliente_id {}, pago_original_id {}, monto_reembolso {})",
                    payment_request_id, alquiler_id, cliente_id, pago_original_id, monto_reembolso
                )
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MsgProcesadorPagosASede {
    PagoPreAutorizado { payment_request_id: String, pago_id: String },
    PagoAceptado { payment_request_id: String, pago_id: String },
    PagoReembolsado { payment_request_id: String, pago_id: String },
    PagoRechazado { payment_request_id: String, motivo: String },
    NoSoyLider { lider_addr: Option<SocketAddr> },
}

impl MsgProcesadorPagosASede {
    pub fn describir(&self) -> String {
        match self {
            MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id } => {
                format!("PagoPreAutorizado (payment_request_id {}, pago_id {})", payment_request_id, pago_id)
            }
            MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id } => {
                format!("PagoAceptado (payment_request_id {}, pago_id {})", payment_request_id, pago_id)
            }
            MsgProcesadorPagosASede::PagoReembolsado { payment_request_id, pago_id } => {
                format!("PagoReembolsado (payment_request_id {}, pago_id {})", payment_request_id, pago_id)
            }
            MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo } => {
                format!("PagoRechazado (payment_request_id {}, motivo {})", payment_request_id, motivo)
            }
            MsgProcesadorPagosASede::NoSoyLider { lider_addr } => {
                format!("NoSoyLider (lider_addr {:?})", lider_addr)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_autorizar_pago_round_trip_y_keys() {
        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgSedeAProcesadorPagos = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let inner = &value["PreAutorizarPago"];
        assert!(inner.get("payment_request_id").is_some());
        assert!(inner.get("cliente_id").is_some());
        assert!(inner.get("monto_seguridad").is_some());
    }

    #[test]
    fn cobrar_pago_round_trip() {
        let msg = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "pr-2".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 200.0,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgSedeAProcesadorPagos = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }

    #[test]
    fn reembolsar_pago_round_trip() {
        let msg = MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "pr-3".to_string(),
            alquiler_id: "alq-2".to_string(),
            cliente_id: "c-2".to_string(),
            pago_original_id: "pago-orig-1".to_string(),
            monto_reembolso: 30.0,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgSedeAProcesadorPagos = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }

    #[test]
    fn pago_pre_autorizado_round_trip() {
        let msg = MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-1".to_string(),
            pago_id: "pago-1".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorPagosASede = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }

    #[test]
    fn pago_aceptado_round_trip() {
        let msg = MsgProcesadorPagosASede::PagoAceptado {
            payment_request_id: "pr-2".to_string(),
            pago_id: "pago-2".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorPagosASede = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }

    #[test]
    fn pago_reembolsado_round_trip() {
        let msg = MsgProcesadorPagosASede::PagoReembolsado {
            payment_request_id: "pr-3".to_string(),
            pago_id: "pago-3".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorPagosASede = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }

    #[test]
    fn pago_rechazado_round_trip() {
        let msg = MsgProcesadorPagosASede::PagoRechazado {
            payment_request_id: "pr-4".to_string(),
            motivo: "monto excede el limite".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorPagosASede = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let inner = &value["PagoRechazado"];
        assert!(inner.get("payment_request_id").is_some());
        assert!(inner.get("motivo").is_some());
    }

    #[test]
    fn no_soy_lider_round_trip_con_addr_none() {
        let msg = MsgProcesadorPagosASede::NoSoyLider { lider_addr: None };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorPagosASede = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }

    #[test]
    fn no_soy_lider_round_trip_con_addr_some() {
        let addr: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let msg = MsgProcesadorPagosASede::NoSoyLider { lider_addr: Some(addr) };
        let json = serde_json::to_string(&msg).unwrap();
        let de: MsgProcesadorPagosASede = serde_json::from_str(&json).unwrap();
        assert_eq!(de, msg);
    }
}
