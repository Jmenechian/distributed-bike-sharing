use actix::{AsyncContext, Handler, Message, MessageResult, ResponseFuture};
use matenmutex::common::PREMONTO;
use matenmutex::log;
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::types::PagoPendiente;
use std::io;
use std::io::Error;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::alquileres::ActualizarPagoPreautorizacion;
use crate::persistencia::PersistirEstado;
use crate::Sede;
use crate::PROCESADOR_PAGOS_ADDR_DEFAULT;

pub(crate) fn parsear_addrs_procesador_pagos(env_addrs: Option<&str>, env_addr_legacy: Option<&str>) -> Vec<String> {
    if let Some(addrs) = env_addrs {
        let lista: Vec<String> = addrs.split(',').map(|a| a.trim().to_string()).filter(|a| !a.is_empty()).collect();
        if !lista.is_empty() {
            return lista;
        }
    }

    if let Some(addr) = env_addr_legacy {
        let addr = addr.trim();
        if !addr.is_empty() {
            return vec![addr.to_string()];
        }
    }

    vec![PROCESADOR_PAGOS_ADDR_DEFAULT.to_string()]
}

/// Envia un mensaje de pago intentando descubrir cual es el ProcesadorPagos lider.
/// Prueba primero el lider cacheado (si hay), y si falla o responde NoSoyLider invalida el cache
/// y prueba el resto de los nodos en orden, sin repetir. Si alguno responde con exito (no NoSoyLider), lo cachea como lider.
pub(crate) async fn enviar_con_descubrimiento_de_lider(
    nodos: &[String],
    cache: &Arc<Mutex<Option<String>>>,
    msg: MsgSedeAProcesadorPagos,
) -> Result<MsgProcesadorPagosASede, io::Error> {
    let cacheado = cache.lock().unwrap_or_else(|e| e.into_inner()).clone();

    let mut orden: Vec<String> = Vec::with_capacity(nodos.len());
    if let Some(addr) = cacheado {
        orden.push(addr);
    }
    for nodo in nodos {
        if !orden.contains(nodo) {
            orden.push(nodo.clone());
        }
    }
    orden.truncate(nodos.len());

    for addr in orden {
        match enviar_a_procesador_pagos(&addr, &msg).await {
            Ok(MsgProcesadorPagosASede::NoSoyLider { .. }) => {
                *cache.lock().unwrap_or_else(|e| e.into_inner()) = None;
                continue;
            }
            Ok(resp) => {
                *cache.lock().unwrap_or_else(|e| e.into_inner()) = Some(addr);
                return Ok(resp);
            }
            Err(_) => {
                *cache.lock().unwrap_or_else(|e| e.into_inner()) = None;
                continue;
            }
        }
    }

    Err(Error::new(io::ErrorKind::NotConnected, "no se encontro lider de pagos"))
}

/// Abre una conexion TCP nueva al ProcesadorPagos, envia un mensaje
/// JSON-lines y espera la respuesta. Es lo mismo que hace enviar_mensaje_a_sede en cliente.rs.
async fn enviar_a_procesador_pagos(
    addr: &str,
    msg: &MsgSedeAProcesadorPagos,
) -> Result<MsgProcesadorPagosASede, io::Error> {
    let stream = TcpStream::connect(addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let payload = serde_json::to_string(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        + "\n";
    writer.write_all(payload.as_bytes()).await?;

    match lines.next_line().await? {
        Some(l) => serde_json::from_str(&l).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "procesador cerro la conexion")),
    }
}

pub(crate) fn generar_captura_deposito(alquiler_id: String, cliente_id: String) -> MsgSedeAProcesadorPagos {
    MsgSedeAProcesadorPagos::CobrarPago {
        payment_request_id: Uuid::new_v4().to_string(),
        alquiler_id,
        cliente_id,
        monto_final: PREMONTO,
    }
}

pub(crate) fn generar_reembolso_deposito(alquiler_id: String, cliente_id: String) -> MsgSedeAProcesadorPagos {
    MsgSedeAProcesadorPagos::ReembolsarPago {
        payment_request_id: Uuid::new_v4().to_string(),
        alquiler_id,
        cliente_id,
        pago_original_id: String::new(),
        monto_reembolso: PREMONTO,
    }
}

pub(crate) struct GuardarPagoPendiente {
    pub(crate) pago: PagoPendiente,
}

impl Message for GuardarPagoPendiente {
    type Result = ();
}

impl Handler<GuardarPagoPendiente> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarPagoPendiente, ctx: &mut Self::Context) -> Self::Result {
        log!("[sede {}]: pago pendiente encolado ({})", self.sede_id, msg.pago.mensaje.describir());
        self.pagos_pendientes.push(msg.pago);
        ctx.address().do_send(PersistirEstado);
    }
}

pub(crate) struct RemoverPagoPendiente {
    pub(crate) payment_request_id: String,
}

impl Message for RemoverPagoPendiente {
    type Result = ();
}

impl Handler<RemoverPagoPendiente> for Sede {
    type Result = ();

    fn handle(&mut self, msg: RemoverPagoPendiente, ctx: &mut Self::Context) -> Self::Result {
        self.pagos_pendientes.retain(|p| p.payment_request_id != msg.payment_request_id);
        ctx.address().do_send(PersistirEstado);
    }
}

pub(crate) struct GetPagosPendientes;

impl Message for GetPagosPendientes {
    type Result = Vec<PagoPendiente>;
}

impl Handler<GetPagosPendientes> for Sede {
    type Result = MessageResult<GetPagosPendientes>;

    fn handle(&mut self, _msg: GetPagosPendientes, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.pagos_pendientes.clone())
    }
}

pub(crate) struct FlushPagosPendientes;

impl Message for FlushPagosPendientes {
    type Result = ();
}

impl Handler<FlushPagosPendientes> for Sede {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: FlushPagosPendientes, ctx: &mut Self::Context) -> Self::Result {
        let pendientes = self.pagos_pendientes.clone();
        let procesador_pagos_addrs = self.procesador_pagos_addrs.clone();
        let lider_cacheado = self.lider_cacheado.clone();
        let sede_addr = ctx.address();
        let sede_id = self.sede_id;

        Box::pin(async move {
            for pendiente in pendientes {
                let payment_request_id = pendiente.payment_request_id.clone();

                match enviar_con_descubrimiento_de_lider(&procesador_pagos_addrs, &lider_cacheado, pendiente.mensaje.clone()).await {
                    Ok(MsgProcesadorPagosASede::PagoPreAutorizado { pago_id, .. }) => {
                        log!("[sede {}]: pago pendiente {} preautorizado (pago_id {})", sede_id, payment_request_id, pago_id);
                        if let Some(alquiler_id) = pendiente.alquiler_id.clone() {
                            sede_addr.do_send(ActualizarPagoPreautorizacion { alquiler_id, pago_id });
                        }
                        sede_addr.do_send(RemoverPagoPendiente { payment_request_id });
                    }
                    Ok(MsgProcesadorPagosASede::PagoRechazado { motivo, .. }) => {
                        log!("[sede {}]: pago pendiente {} rechazado al reintentar ({}), se descarta", sede_id, payment_request_id, motivo);
                        sede_addr.do_send(RemoverPagoPendiente { payment_request_id });
                    }
                    Ok(otra) => {
                        log!("[sede {}]: pago pendiente {} procesado ({})", sede_id, payment_request_id, otra.describir());
                        sede_addr.do_send(RemoverPagoPendiente { payment_request_id });
                    }
                    Err(e) => {
                        log!("[sede {}]: pago pendiente {} sigue fallando, reintento luego: {}", sede_id, payment_request_id, e);
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsear_addrs_usa_lista_separada_por_comas_con_espacios() {
        let addrs = parsear_addrs_procesador_pagos(
            Some(" 127.0.0.1:9100, 127.0.0.1:9101 ,127.0.0.1:9102"),
            None,
        );
        assert_eq!(addrs, vec!["127.0.0.1:9100", "127.0.0.1:9101", "127.0.0.1:9102"]);
    }

    #[test]
    fn parsear_addrs_lista_de_un_solo_elemento() {
        let addrs = parsear_addrs_procesador_pagos(Some("127.0.0.1:9100"), None);
        assert_eq!(addrs, vec!["127.0.0.1:9100"]);
    }

    #[test]
    fn parsear_addrs_usa_legacy_si_no_hay_lista() {
        let addrs = parsear_addrs_procesador_pagos(None, Some("127.0.0.1:9100"));
        assert_eq!(addrs, vec!["127.0.0.1:9100"]);
    }

    #[test]
    fn parsear_addrs_usa_default_si_no_hay_nada_configurado() {
        let addrs = parsear_addrs_procesador_pagos(None, None);
        assert_eq!(addrs, vec![PROCESADOR_PAGOS_ADDR_DEFAULT.to_string()]);
    }

    async fn stub_nodo_pagos(respuesta: MsgProcesadorPagosASede) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let respuesta = respuesta.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(_)) = lines.next_line().await {
                        let payload = serde_json::to_string(&respuesta).unwrap() + "\n";
                        if writer.write_all(payload.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        addr
    }

    fn msg_de_prueba() -> MsgSedeAProcesadorPagos {
        MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-test".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        }
    }

    #[tokio::test]
    async fn descubrimiento_redirige_pasando_por_replica_hasta_el_lider() {
        let replica = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        let nodos = vec![replica, lider.clone()];
        let cache = Arc::new(Mutex::new(None));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(lider));
    }

    #[tokio::test]
    async fn descubrimiento_salta_nodo_caido_y_encuentra_al_lider() {
        let nodo_caido = "127.0.0.1:1".to_string();
        let lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        let nodos = vec![nodo_caido, lider.clone()];
        let cache = Arc::new(Mutex::new(None));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(lider));
    }

    #[tokio::test]
    async fn descubrimiento_reusa_el_lider_cacheado_sin_probar_otros_nodos() {
        let lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        let replica = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let nodos = vec![replica, lider.clone()];
        let cache = Arc::new(Mutex::new(Some(lider.clone())));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(lider));
    }

    #[tokio::test]
    async fn descubrimiento_re_descubre_si_el_lider_cacheado_dejo_de_responder() {
        let viejo_lider = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let nuevo_lider = stub_nodo_pagos(MsgProcesadorPagosASede::PagoPreAutorizado {
            payment_request_id: "pr-test".to_string(),
            pago_id: "pago-1".to_string(),
        })
        .await;
        let nodos = vec![viejo_lider.clone(), nuevo_lider.clone()];
        let cache = Arc::new(Mutex::new(Some(viejo_lider)));

        let resp = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await.unwrap();

        assert!(matches!(resp, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
        assert_eq!(*cache.lock().unwrap(), Some(nuevo_lider));
    }

    #[tokio::test]
    async fn descubrimiento_falla_si_todos_los_nodos_responden_no_soy_lider() {
        let n1 = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let n2 = stub_nodo_pagos(MsgProcesadorPagosASede::NoSoyLider { lider_addr: None }).await;
        let nodos = vec![n1, n2];
        let cache = Arc::new(Mutex::new(None));

        let resultado = enviar_con_descubrimiento_de_lider(&nodos, &cache, msg_de_prueba()).await;

        assert!(resultado.is_err());
        assert_eq!(*cache.lock().unwrap(), None);
    }
}
