use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};



pub fn timestamp_actual() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Bici {
    pub bici_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BiciAlquilada {
    pub bici_id: String,
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PagoProcesado {
    pub payment_request_id: String,
    pub pago_id: String,
    pub alquiler_id: Option<String>,
    pub cliente_id: String,
    pub monto: f64,
    pub tipo: TipoOperacionPago,
    pub estado: EstadoPago,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TipoOperacionPago {
    PreAutorizacion,
    CobroFinal,
    MultaRobo,
    ReembolsoPenalizacion,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum EstadoPago {
    PreAutorizado,
    Aceptado,
    Reembolsado,
    Rechazado,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Voto2PC {
    Commit,
    Abort,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum EstadoTransaccionPago {
    Preparando,
    Preparada,
    Commit,
    Abort,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TransaccionPago2PC {
    pub payment_request_id: String,
    pub operacion: PagoProcesado,
    pub participantes: Vec<u32>,
    pub votos_commit: Vec<u32>,
    pub votos_abort: Vec<u32>,
    pub estado: EstadoTransaccionPago,
}

impl TransaccionPago2PC {
    pub fn nueva(payment_request_id: String, operacion: PagoProcesado, participantes: Vec<u32>) -> Self {
        TransaccionPago2PC {
            payment_request_id,
            operacion,
            participantes,
            votos_commit: Vec::new(),
            votos_abort: Vec::new(),
            estado: EstadoTransaccionPago::Preparando,
        }
    }
}

#[cfg(test)]
mod tests_pagos {
    use super::*;

    #[test]
    fn pago_procesado_round_trip_y_keys() {
        let pago = PagoProcesado {
            payment_request_id: "pr-1".to_string(),
            pago_id: "pago-1".to_string(),
            alquiler_id: None,
            cliente_id: "c-1".to_string(),
            monto: 100.0,
            tipo: TipoOperacionPago::PreAutorizacion,
            estado: EstadoPago::PreAutorizado,
        };
        let json = serde_json::to_string(&pago).unwrap();
        let de: PagoProcesado = serde_json::from_str(&json).unwrap();
        assert_eq!(de, pago);

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        for key in [
            "payment_request_id",
            "pago_id",
            "alquiler_id",
            "cliente_id",
            "monto",
            "tipo",
            "estado",
        ] {
            assert!(value.get(key).is_some(), "falta la key {}", key);
        }
    }

    #[test]
    fn pago_procesado_con_alquiler_id_round_trip() {
        let pago = PagoProcesado {
            payment_request_id: "pr-2".to_string(),
            pago_id: "pago-2".to_string(),
            alquiler_id: Some("alq-1".to_string()),
            cliente_id: "c-1".to_string(),
            monto: 200.0,
            tipo: TipoOperacionPago::CobroFinal,
            estado: EstadoPago::Aceptado,
        };
        let json = serde_json::to_string(&pago).unwrap();
        let de: PagoProcesado = serde_json::from_str(&json).unwrap();
        assert_eq!(de, pago);
    }

    #[test]
    fn tipo_operacion_pago_round_trip_todas_las_variantes() {
        for tipo in [
            TipoOperacionPago::PreAutorizacion,
            TipoOperacionPago::CobroFinal,
            TipoOperacionPago::MultaRobo,
            TipoOperacionPago::ReembolsoPenalizacion,
        ] {
            let json = serde_json::to_string(&tipo).unwrap();
            let de: TipoOperacionPago = serde_json::from_str(&json).unwrap();
            assert_eq!(de, tipo);
        }
    }

    #[test]
    fn estado_pago_round_trip_todas_las_variantes() {
        for estado in [
            EstadoPago::PreAutorizado,
            EstadoPago::Aceptado,
            EstadoPago::Reembolsado,
            EstadoPago::Rechazado,
        ] {
            let json = serde_json::to_string(&estado).unwrap();
            let de: EstadoPago = serde_json::from_str(&json).unwrap();
            assert_eq!(de, estado);
        }
    }

    #[test]
    fn voto_2pc_round_trip_todas_las_variantes() {
        for voto in [Voto2PC::Commit, Voto2PC::Abort] {
            let json = serde_json::to_string(&voto).unwrap();
            let de: Voto2PC = serde_json::from_str(&json).unwrap();
            assert_eq!(de, voto);
        }
    }

    #[test]
    fn estado_transaccion_pago_round_trip_todas_las_variantes() {
        for estado in [
            EstadoTransaccionPago::Preparando,
            EstadoTransaccionPago::Preparada,
            EstadoTransaccionPago::Commit,
            EstadoTransaccionPago::Abort,
        ] {
            let json = serde_json::to_string(&estado).unwrap();
            let de: EstadoTransaccionPago = serde_json::from_str(&json).unwrap();
            assert_eq!(de, estado);
        }
    }

    #[test]
    fn transaccion_pago_2pc_round_trip() {
        let operacion = PagoProcesado {
            payment_request_id: "pr-2pc".to_string(),
            pago_id: "pago-2pc".to_string(),
            alquiler_id: Some("alq-2pc".to_string()),
            cliente_id: "c-2pc".to_string(),
            monto: 150.0,
            tipo: TipoOperacionPago::CobroFinal,
            estado: EstadoPago::Aceptado,
        };
        let tx = TransaccionPago2PC {
            payment_request_id: "pr-2pc".to_string(),
            operacion,
            participantes: vec![1, 2, 3],
            votos_commit: vec![1, 2],
            votos_abort: vec![3],
            estado: EstadoTransaccionPago::Preparada,
        };
        let json = serde_json::to_string(&tx).unwrap();
        let de: TransaccionPago2PC = serde_json::from_str(&json).unwrap();
        assert_eq!(de, tx);
    }

    #[test]
    fn transaccion_pago_2pc_nueva_arranca_en_preparando_sin_votos() {
        let operacion = PagoProcesado {
            payment_request_id: "pr-3".to_string(),
            pago_id: "pago-3".to_string(),
            alquiler_id: None,
            cliente_id: "c-3".to_string(),
            monto: 50.0,
            tipo: TipoOperacionPago::PreAutorizacion,
            estado: EstadoPago::PreAutorizado,
        };
        let tx = TransaccionPago2PC::nueva("pr-3".to_string(), operacion, vec![1, 2]);
        assert_eq!(tx.estado, EstadoTransaccionPago::Preparando);
        assert!(tx.votos_commit.is_empty());
        assert!(tx.votos_abort.is_empty());
        assert_eq!(tx.participantes, vec![1, 2]);
    }
}