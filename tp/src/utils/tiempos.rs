use std::time::Duration;

const HEARTBEAT_INTERVAL: u64 = 5;
const MONITOR_INTERVAL: u64 = 5;
const LIVENESS_TIMEOUT: u64 = 15;
const TIMEOUT_OK: u64 = 3;
const TIMEOUT_COORDINADOR: u64 = 8;
/// Tiempos del nodo. Se construyen con `TiemposBuilder` que valida que
/// los tiempos sean consistentes
#[derive(Clone, Copy)]
pub struct Tiempos {
    pub heartbeat: Duration,
    pub monitor: Duration,
    pub liveness_timeout: Duration,
    pub espera_ok: Duration,
    pub espera_coordinador: Duration,
}

/// Builder de `Tiempos`. Arranca con valores default (las constantes de
/// arriba). Se pueden pisar los valores y llamar a `build()` para validar
/// que los tiempos sean correctos.
pub struct TiemposBuilder {
    heartbeat: Duration,
    monitor: Duration,
    liveness_timeout: Duration,
    espera_ok: Duration,
    espera_coordinador: Duration,
}

impl TiemposBuilder {
    pub fn new() -> Self {
        TiemposBuilder {
            heartbeat: Duration::from_secs(HEARTBEAT_INTERVAL),
            monitor: Duration::from_secs(MONITOR_INTERVAL),
            liveness_timeout: Duration::from_secs(LIVENESS_TIMEOUT),
            espera_ok: Duration::from_secs(TIMEOUT_OK),
            espera_coordinador: Duration::from_secs(TIMEOUT_COORDINADOR),
        }
    }

    pub fn heartbeat(mut self, d: Duration) -> Self { self.heartbeat = d; self }
    pub fn monitor(mut self, d: Duration) -> Self { self.monitor = d; self }
    pub fn liveness_timeout(mut self, d: Duration) -> Self { self.liveness_timeout = d; self }
    pub fn espera_ok(mut self, d: Duration) -> Self { self.espera_ok = d; self }
    pub fn espera_coordinador(mut self, d: Duration) -> Self { self.espera_coordinador = d; self }

    pub fn build(self) -> Result<Tiempos, String> {
        let timers = [
            ("heartbeat", self.heartbeat),
            ("monitor", self.monitor),
            ("liveness_timeout", self.liveness_timeout),
            ("espera_ok", self.espera_ok),
            ("espera_coordinador", self.espera_coordinador),
        ];
        for (nombre, d) in timers {
            if d.is_zero() {
                return Err(format!("el timer '{}' no puede ser cero", nombre));
            }
        }
        // Sin esto un nodo se consideraría caído entre un heartbeat y el siguiente.
        if self.heartbeat >= self.liveness_timeout {
            return Err(format!(
                "heartbeat ({:?}) debe ser menor que liveness_timeout ({:?}) para evitar flapping",
                self.heartbeat, self.liveness_timeout
            ));
        }
        Ok(Tiempos {
            heartbeat: self.heartbeat,
            monitor: self.monitor,
            liveness_timeout: self.liveness_timeout,
            espera_ok: self.espera_ok,
            espera_coordinador: self.espera_coordinador,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tiempos_builder_default_es_valido() {
        assert!(TiemposBuilder::new().build().is_ok());
    }

    #[test]
    fn test_tiempos_builder_rechaza_heartbeat_mayor_o_igual_que_liveness() {
        let r = TiemposBuilder::new()
            .heartbeat(Duration::from_secs(10))
            .liveness_timeout(Duration::from_secs(5))
            .build();
        assert!(r.is_err());
    }

    #[test]
    fn test_tiempos_builder_rechaza_timer_en_cero() {
        let r = TiemposBuilder::new().monitor(Duration::ZERO).build();
        assert!(r.is_err());
    }
}