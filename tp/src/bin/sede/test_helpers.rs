use actix::{Actor, Addr};
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::messages::sede_sede::{crear_info_dummy, SedeInfo};
use matenmutex::slot::{crear_slots_con_estados, EstadoSlot};
use matenmutex::token;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

use crate::{Sede, PROCESADOR_PAGOS_ADDR_DEFAULT};

pub(crate) fn sede_con_estados(estados: Vec<EstadoSlot>) -> Addr<Sede> {
    sede_con_estados_y_procesador(estados, PROCESADOR_PAGOS_ADDR_DEFAULT.to_string())
}

pub(crate) fn rellenar_ocupados_vacios_testing(estados: &Vec<EstadoSlot>) -> SedeInfo {
    let ocupados = estados.iter()
        .filter(|e| matches!(e, EstadoSlot::Disponible { .. }))
        .count() as u8;
    let vacios = estados.iter()
        .filter(|e| matches!(e, EstadoSlot::Vacio))
        .count() as u8;

    let mut dummy_info = crear_info_dummy();
    dummy_info.slots_disponibles = ocupados;
    dummy_info.slots_vacios = vacios;
    dummy_info
}

pub(crate) fn token_valido_de_prueba(bici_id: &str, alquiler_id: &str, cliente_id: &str, sede_origen_id: u32, inicio_ts: u64) -> String {
    let secreto = token::cargar_secret("secreto-de-test");
    let payload = token::PayloadToken {
        bici_id: bici_id.to_string(),
        alquiler_id: alquiler_id.to_string(),
        cliente_id: cliente_id.to_string(),
        sede_origen_id,
        inicio_ts,
    };
    token::generar_token(&payload, &secreto).expect("generar_token no debe fallar en tests")
}

/// Levanta un ProcesadorPagos de prueba que responde siempre
/// PagoPreAutorizado (o PagoRechazado si rechazar es true) a
/// cualquier PreAutorizarPago recibido. Devuelve la direccion en la que escucha.
pub(crate) async fn stub_procesador_pagos(rechazar: bool) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let (reader, mut writer) = socket.into_split();
                let mut lines = BufReader::new(reader).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let resp = match serde_json::from_str::<MsgSedeAProcesadorPagos>(line.trim()) {
                        Ok(MsgSedeAProcesadorPagos::PreAutorizarPago { payment_request_id, .. }) => {
                            if rechazar {
                                MsgProcesadorPagosASede::PagoRechazado {
                                    payment_request_id,
                                    motivo: "rechazo simulado en test".to_string(),
                                }
                            } else {
                                MsgProcesadorPagosASede::PagoPreAutorizado {
                                    payment_request_id,
                                    pago_id: "pago-test".to_string(),
                                }
                            }
                        }
                        Ok(MsgSedeAProcesadorPagos::CobrarPago { payment_request_id, .. }) => {
                            if rechazar {
                                MsgProcesadorPagosASede::PagoRechazado {
                                    payment_request_id,
                                    motivo: "rechazo simulado en test".to_string(),
                                }
                            } else {
                                MsgProcesadorPagosASede::PagoAceptado {
                                    payment_request_id,
                                    pago_id: "cobro-test".to_string(),
                                }
                            }
                        }
                        _ => MsgProcesadorPagosASede::PagoRechazado {
                            payment_request_id: String::new(),
                            motivo: "mensaje no soportado por el stub".to_string(),
                        },
                    };
                    let payload = serde_json::to_string(&resp).unwrap() + "\n";
                    if writer.write_all(payload.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    addr
}

pub(crate) fn sede_con_estados_y_procesador(estados: Vec<EstadoSlot>, procesador_pagos_addr: String) -> Addr<Sede> {
    sede_con_estados_path_y_procesador(estados, procesador_pagos_addr, String::new())
}

pub(crate) fn sede_con_estados_path_y_procesador(estados: Vec<EstadoSlot>, procesador_pagos_addr: String, estado_path: String) -> Addr<Sede> {
    let dummy_info = rellenar_ocupados_vacios_testing(&estados);
    let slots = crear_slots_con_estados(estados);

    Sede {
        sede_id: 1,
        coords: (-34.6037, -58.3816),
        slots,
        token_secret: token::cargar_secret("secreto-de-test"),
        pending_reservas: std::collections::HashMap::new(),
        sedes_conocidas: vec![],
        sede_info: dummy_info,
        procesador_pagos_addrs: vec![procesador_pagos_addr],
        lider_cacheado: Arc::new(Mutex::new(None)),
        estado_path,
        alquileres_activos: Vec::new(),
        devoluciones_recibidas: Vec::new(),
        pagos_pendientes: Vec::new(),
        notificaciones_pendientes: Vec::new(),
        offline: Arc::new(Mutex::new(false)),
        estaba_offline: false,
    }
        .start()
}

pub(crate) fn sede_offline_con_estados_y_procesador(estados: Vec<EstadoSlot>, procesador_pagos_addr: String) -> Addr<Sede> {
    let dummy_info = rellenar_ocupados_vacios_testing(&estados);
    let slots = crear_slots_con_estados(estados);

    Sede {
        sede_id: 1,
        coords: (-34.6037, -58.3816),
        slots,
        token_secret: token::cargar_secret("secreto-de-test"),
        pending_reservas: std::collections::HashMap::new(),
        sedes_conocidas: vec![],
        sede_info: dummy_info,
        procesador_pagos_addrs: vec![procesador_pagos_addr],
        lider_cacheado: Arc::new(Mutex::new(None)),
        estado_path: String::new(),
        alquileres_activos: Vec::new(),
        devoluciones_recibidas: Vec::new(),
        pagos_pendientes: Vec::new(),
        notificaciones_pendientes: Vec::new(),
        offline: Arc::new(Mutex::new(true)),
        estaba_offline: false,
    }
        .start()
}

pub(crate) fn path_temporal(nombre: &str) -> String {
    let mut path = std::env::temp_dir();
    path.push(format!("{nombre}-{}.json", Uuid::new_v4()));
    path.to_string_lossy().to_string()
}
