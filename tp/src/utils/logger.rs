use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

static LOG_FILE: OnceLock<Mutex<File>> = OnceLock::new();

/// Inicializa el log del nodo en `logs/<nombre>.log` (trunca el previo).
/// Es idempotente, solo el primer llamado tiene efecto.
pub fn init(nombre: &str) {
    let _ = create_dir_all("logs");
    if let Ok(file) = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(format!("logs/{nombre}.log"))
    {
        let _ = LOG_FILE.set(Mutex::new(file));
    }
}

/// Escribe una línea con timestamp. No hace nada si no se inicializó (ej. en los tests)
pub fn write_log(args: std::fmt::Arguments) {
    if let Some(lock) = LOG_FILE.get() {
        let mut file = lock.lock().unwrap_or_else(|e| e.into_inner());
        let _ = writeln!(file, "{} {}", timestamp(), args);
    }
}

fn timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{:02}:{:02}:{:02}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60)
}

/// Escribe un log de un nodo.
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::utils::logger::write_log(format_args!($($arg)*))
    };
}
