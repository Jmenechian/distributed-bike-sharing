pub const DISTANCIA_MINIMA: f64 = 2.0;
pub const PREMONTO: u32 = 150;
pub const TASA: u32 = 20;

// const CONFIG_FILE: usize = 1;
// const MINIMAL_LENGTH: usize = 2;
pub const HEARTBEAT_INTERVAL: u64 = 5;
pub const MONITOR_INTERVAL: u64 = 5;
pub const LIVENESS_TIMEOUT: u64 = 15;
// const TIMEOUT_OK: u64 = 3;
// const TIMEOUT_COORDINADOR: u64 = 8;
pub const PUERTO_BASE: u16 = 6000;
pub const MAX_VALUE: u32 = 4294967295;

pub fn calcular_distancia(coords_a: (f64, f64), coords_b: (f64, f64)) -> f64 {
    let (coords_sede_x, coords_sede_y) = coords_a;
    let (coords_cliente_x, coords_cliente_y) = coords_b;

    ((coords_sede_x - coords_cliente_x).powi(2) + (coords_sede_y - coords_cliente_y).powi(2)).sqrt()
}