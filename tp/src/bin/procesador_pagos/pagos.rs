use std::sync::Arc;
use matenmutex::log;
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::types::{EstadoPago, PagoProcesado, TipoOperacionPago};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::TcpStream;
use uuid::Uuid;

use super::bully::RolActual;
use super::ProcesadorPagos;

pub(crate) fn monto_de(msg: &MsgSedeAProcesadorPagos) -> f64 {
    match msg {
        MsgSedeAProcesadorPagos::PreAutorizarPago { monto_seguridad, .. } => *monto_seguridad,
        MsgSedeAProcesadorPagos::CobrarPago { monto_final, .. } => *monto_final,
        MsgSedeAProcesadorPagos::ReembolsarPago { monto_reembolso, .. } => *monto_reembolso,
    }
}

fn respuesta_desde_pago(pago: &PagoProcesado) -> MsgProcesadorPagosASede {
    let payment_request_id = pago.payment_request_id.clone();
    match pago.estado {
        EstadoPago::PreAutorizado => MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id: pago.pago_id.clone() },
        EstadoPago::Aceptado => MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id: pago.pago_id.clone() },
        EstadoPago::Reembolsado => MsgProcesadorPagosASede::PagoReembolsado { payment_request_id, pago_id: pago.pago_id.clone() },
        EstadoPago::Rechazado => MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo: "operacion previamente rechazada".to_string() },
    }
}

impl ProcesadorPagos {
    pub(crate) async fn accept_loop_sedes(self: Arc<Self>) {
        loop {
            match self.public_listener.accept().await {
                Ok((socket, addr)) => {
                    log!("[NODO {}]: Nueva conexion de Sede desde {}", self.procesador_id, addr);
                    tokio::spawn(self.clone().atender_sede(socket));
                }
                Err(e) => log!("[{}]: error aceptando sede: {}", self.etiqueta(), e),
            }
        }
    }

    async fn atender_sede(self: Arc<Self>, socket: TcpStream) {
        let (reader, mut writer) = socket.into_split();
        let mut lines = TokioBufReader::new(reader).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let resp = match serde_json::from_str::<MsgSedeAProcesadorPagos>(line.trim()) {
                Ok(msg) => {
                    log!("[procesador_pagos] Recibe {}", msg.describir());
                    self.procesar_pago(msg).await
                }
                Err(_) => {
                    log!("[procesador_pagos] Recibe mensaje invalido: {}", line.trim());
                    MsgProcesadorPagosASede::PagoRechazado {
                        payment_request_id: String::new(),
                        motivo: format!("mensaje invalido: {}", line.trim()),
                    }
                }
            };
            log!("[procesador_pagos] Envia {}", resp.describir());
            let payload = match serde_json::to_string(&resp) {
                Ok(json) => json + "\n",
                Err(_) => continue,
            };
            if writer.write_all(payload.as_bytes()).await.is_err() {
                break;
            }
        }
    }

    pub(crate) async fn procesar_pago(self: &Arc<Self>, msg: MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
        let payment_request_id = msg.payment_request_id().to_string();
        {
            let pagos = match self.pagos_procesados.lock() {
                Ok(guard) => guard,
                Err(_) => {
                    return MsgProcesadorPagosASede::PagoRechazado {
                        payment_request_id,
                        motivo: "estado de pagos no disponible".into(),
                    }
                }
            };
            if let Some(pago) = pagos.iter().find(|p| p.payment_request_id == payment_request_id) {
                return respuesta_desde_pago(pago);
            }
        }

        match self.rol() {
            Some(RolActual::Lider) if !self.peers.is_empty() => self.coordinar_2pc_como_lider(msg).await,
            Some(RolActual::Lider) => self.procesar_pago_local(&msg),
            _ => MsgProcesadorPagosASede::NoSoyLider { lider_addr: None },
        }
    }

    pub(crate) fn procesar_pago_local(&self, msg: &MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
        let mut pagos = match self.pagos_procesados.lock() {
            Ok(g) => g,
            Err(_) => {
                return MsgProcesadorPagosASede::PagoRechazado {
                    payment_request_id: msg.payment_request_id().to_string(),
                    motivo: "estado de pagos no disponible".into(),
                }
            }
        };

        if let Some(pago) = pagos.iter().find(|p| p.payment_request_id == msg.payment_request_id()) {
            return respuesta_desde_pago(pago);
        }

        let (pago, respuesta) = self.decidir_operacion(msg);
        pagos.push(pago);
        drop(pagos);
        self.persistir();
        respuesta
    }

    pub(crate) fn persistir_commit_replica(&self, payment_request_id: &str) {
        let pendiente = match self.preparados_replica.lock() {
            Ok(mut preparados) => preparados.remove(payment_request_id),
            Err(_) => None,
        };
        if let Ok(mut pagos) = self.pagos_procesados.lock()
            && !pagos.iter().any(|p| p.payment_request_id == payment_request_id) {
                let pago = match &pendiente {
                    Some(operacion) => self.decidir_operacion(operacion).0,
                    None => PagoProcesado {
                        payment_request_id: payment_request_id.to_string(),
                        pago_id: Uuid::new_v4().to_string(),
                        alquiler_id: None,
                        cliente_id: String::new(),
                        monto: 0.0,
                        tipo: TipoOperacionPago::CobroFinal,
                        estado: EstadoPago::Aceptado,
                    },
                };
                pagos.push(pago);
            }
    }

    pub(crate) fn decidir_operacion(&self, msg: &MsgSedeAProcesadorPagos) -> (PagoProcesado, MsgProcesadorPagosASede) {
        match msg {
            MsgSedeAProcesadorPagos::PreAutorizarPago { payment_request_id, cliente_id, monto_seguridad } => {
                self.resolver_operacion(
                    payment_request_id,
                    None,
                    cliente_id,
                    *monto_seguridad,
                    TipoOperacionPago::PreAutorizacion,
                    EstadoPago::PreAutorizado,
                    "monto_seguridad excede el limite configurado",
                    |payment_request_id, pago_id| MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id },
                )
            }
            MsgSedeAProcesadorPagos::CobrarPago { payment_request_id, alquiler_id, cliente_id, monto_final } => {
                self.resolver_operacion(
                    payment_request_id,
                    Some(alquiler_id.clone()),
                    cliente_id,
                    *monto_final,
                    TipoOperacionPago::CobroFinal,
                    EstadoPago::Aceptado,
                    "monto_final excede el limite configurado",
                    |payment_request_id, pago_id| MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id },
                )
            }
            MsgSedeAProcesadorPagos::ReembolsarPago { payment_request_id, alquiler_id, cliente_id, pago_original_id: _, monto_reembolso } => {
                self.resolver_operacion(
                    payment_request_id,
                    Some(alquiler_id.clone()),
                    cliente_id,
                    *monto_reembolso,
                    TipoOperacionPago::ReembolsoPenalizacion,
                    EstadoPago::Reembolsado,
                    "monto_reembolso excede el limite configurado",
                    |payment_request_id, pago_id| MsgProcesadorPagosASede::PagoReembolsado { payment_request_id, pago_id },
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn resolver_operacion(
        &self,
        payment_request_id: &str,
        alquiler_id: Option<String>,
        cliente_id: &str,
        monto: f64,
        tipo: TipoOperacionPago,
        estado_aceptado: EstadoPago,
        motivo_rechazo: &str,
        respuesta_aceptada: impl FnOnce(String, String) -> MsgProcesadorPagosASede,
    ) -> (PagoProcesado, MsgProcesadorPagosASede) {
        if self.monto_excede_limite(monto) {
            let pago = PagoProcesado {
                payment_request_id: payment_request_id.to_string(),
                pago_id: String::new(),
                alquiler_id,
                cliente_id: cliente_id.to_string(),
                monto,
                tipo,
                estado: EstadoPago::Rechazado,
            };
            let respuesta = MsgProcesadorPagosASede::PagoRechazado {
                payment_request_id: payment_request_id.to_string(),
                motivo: motivo_rechazo.to_string(),
            };
            (pago, respuesta)
        } else {
            let pago_id = Uuid::new_v4().to_string();
            let pago = PagoProcesado {
                payment_request_id: payment_request_id.to_string(),
                pago_id: pago_id.clone(),
                alquiler_id,
                cliente_id: cliente_id.to_string(),
                monto,
                tipo,
                estado: estado_aceptado,
            };
            let respuesta = respuesta_aceptada(payment_request_id.to_string(), pago_id);
            (pago, respuesta)
        }
    }

    pub(crate) fn monto_excede_limite(&self, monto: f64) -> bool {
        match self.rechazar_si_monto_supera {
            Some(limite) => monto > limite,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use matenmutex::types::{EstadoPago, TipoOperacionPago};
    use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn pre_autorizar_pago_aceptado_persiste_y_responde() {
        let nodo = nodo_de_prueba(10, 0, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        }).await;

        match respuesta {
            MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "pr-1");
                assert!(!pago_id.is_empty());
            }
            other => panic!("se esperaba PagoPreAutorizado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        let pago = &pagos[0];
        assert_eq!(pago.payment_request_id, "pr-1");
        assert_eq!(pago.alquiler_id, None);
        assert_eq!(pago.tipo, TipoOperacionPago::PreAutorizacion);
        assert_eq!(pago.estado, EstadoPago::PreAutorizado);
        assert_eq!(pago.monto, 100.0);
    }

    #[tokio::test]
    async fn replica_responde_no_soy_lider_y_no_persiste_el_pago() {
        let nodo = nodo_de_prueba(11, 4, RolActual::Replica { lider_id: 1 }).await;

        let respuesta = nodo
            .procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
                payment_request_id: "pr-replica".to_string(),
                cliente_id: "c-1".to_string(),
                monto_seguridad: 100.0,
            })
            .await;

        assert_eq!(respuesta, MsgProcesadorPagosASede::NoSoyLider { lider_addr: None });
        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn candidato_responde_no_soy_lider_y_no_persiste_el_pago() {
        let nodo = nodo_de_prueba(12, 4, RolActual::Candidato).await;

        let respuesta = nodo
            .procesar_pago(MsgSedeAProcesadorPagos::CobrarPago {
                payment_request_id: "pr-candidato".to_string(),
                alquiler_id: "alq-1".to_string(),
                cliente_id: "c-1".to_string(),
                monto_final: 50.0,
            })
            .await;

        assert_eq!(respuesta, MsgProcesadorPagosASede::NoSoyLider { lider_addr: None });
        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cobrar_pago_aceptado_persiste_y_responde() {
        let nodo = nodo_de_prueba(11, 0, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "pr-3".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 200.0,
        }).await;

        match respuesta {
            MsgProcesadorPagosASede::PagoAceptado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "pr-3");
                assert!(!pago_id.is_empty());
            }
            other => panic!("se esperaba PagoAceptado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        let pago = &pagos[0];
        assert_eq!(pago.alquiler_id, Some("alq-1".to_string()));
        assert_eq!(pago.tipo, TipoOperacionPago::CobroFinal);
        assert_eq!(pago.estado, EstadoPago::Aceptado);
    }

    #[tokio::test]
    async fn reembolsar_pago_aceptado_persiste_y_responde() {
        let nodo = nodo_de_prueba(12, 0, RolActual::Lider).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "pr-5".to_string(),
            alquiler_id: "alq-3".to_string(),
            cliente_id: "c-3".to_string(),
            pago_original_id: "pago-orig-1".to_string(),
            monto_reembolso: 30.0,
        }).await;

        match respuesta {
            MsgProcesadorPagosASede::PagoReembolsado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "pr-5");
                assert!(!pago_id.is_empty());
            }
            other => panic!("se esperaba PagoReembolsado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        let pago = &pagos[0];
        assert_eq!(pago.tipo, TipoOperacionPago::ReembolsoPenalizacion);
        assert_eq!(pago.estado, EstadoPago::Reembolsado);
        assert_eq!(pago.monto, 30.0);
    }

    #[tokio::test]
    async fn payment_request_id_repetido_es_idempotente() {
        let nodo = nodo_de_prueba(13, 0, RolActual::Lider).await;

        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-7".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };

        let r1 = nodo.procesar_pago(msg.clone()).await;
        let r2 = nodo.procesar_pago(msg).await;

        assert_eq!(r1, r2);
        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
    }

    #[tokio::test]
    async fn monto_que_excede_el_limite_es_rechazado() {
        let nodo = nodo_de_prueba_con_limite(14, 0, RolActual::Lider, Some(50.0)).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-10".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        }).await;

        match respuesta {
            MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo } => {
                assert_eq!(payment_request_id, "pr-10");
                assert!(!motivo.is_empty());
            }
            other => panic!("se esperaba PagoRechazado, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        assert_eq!(pagos[0].estado, EstadoPago::Rechazado);
        assert_eq!(pagos[0].tipo, TipoOperacionPago::PreAutorizacion);
    }

    #[tokio::test]
    async fn monto_dentro_del_limite_es_aceptado() {
        let nodo = nodo_de_prueba_con_limite(15, 0, RolActual::Lider, Some(50.0)).await;

        let respuesta = nodo.procesar_pago(MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-11".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 50.0,
        }).await;

        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn accept_loop_responde_pre_autorizar_pago() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let nodo = nodo_de_prueba(16, 0, RolActual::Lider).await;
        let addr = nodo.public_listener.local_addr().unwrap();
        tokio::spawn(nodo.clone().accept_loop_sedes());

        let stream = TcpStream::connect(addr).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-acc-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };
        let payload = serde_json::to_string(&msg).unwrap() + "\n";
        writer.write_all(payload.as_bytes()).await.unwrap();

        let line = lines.next_line().await.unwrap().unwrap();
        let respuesta: MsgProcesadorPagosASede = serde_json::from_str(&line).unwrap();
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn accept_loop_no_crashea_con_json_invalido_y_sigue_respondiendo() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let nodo = nodo_de_prueba(17, 0, RolActual::Lider).await;
        let addr = nodo.public_listener.local_addr().unwrap();
        tokio::spawn(nodo.clone().accept_loop_sedes());

        let stream1 = TcpStream::connect(addr).await.unwrap();
        let (reader1, mut writer1) = stream1.into_split();
        let mut lines1 = BufReader::new(reader1).lines();
        writer1.write_all(b"not json\n").await.unwrap();
        let line1 = lines1.next_line().await.unwrap().unwrap();
        let respuesta1: MsgProcesadorPagosASede = serde_json::from_str(&line1).unwrap();
        assert!(matches!(respuesta1, MsgProcesadorPagosASede::PagoRechazado { .. }));

        let stream2 = TcpStream::connect(addr).await.unwrap();
        let (reader2, mut writer2) = stream2.into_split();
        let mut lines2 = BufReader::new(reader2).lines();
        let msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-acc-2".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };
        let payload = serde_json::to_string(&msg).unwrap() + "\n";
        writer2.write_all(payload.as_bytes()).await.unwrap();
        let line2 = lines2.next_line().await.unwrap().unwrap();
        let respuesta2: MsgProcesadorPagosASede = serde_json::from_str(&line2).unwrap();
        assert!(matches!(respuesta2, MsgProcesadorPagosASede::PagoPreAutorizado { .. }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_preautorizar_cobrar_reembolsar_e_idempotencia() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use std::net::SocketAddr;

        let nodo = nodo_de_prueba(18, 0, RolActual::Lider).await;
        let addr = nodo.public_listener.local_addr().unwrap();
        tokio::spawn(nodo.clone().accept_loop_sedes());

        async fn enviar(addr: SocketAddr, msg: &MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            let payload = serde_json::to_string(msg).unwrap() + "\n";
            writer.write_all(payload.as_bytes()).await.unwrap();
            let line = lines.next_line().await.unwrap().unwrap();
            serde_json::from_str(&line).unwrap()
        }

        let preauth_msg = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "e2e-pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 100.0,
        };
        let r1 = enviar(addr, &preauth_msg).await;
        let pago_id_1 = match &r1 {
            MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "e2e-pr-1");
                pago_id.clone()
            }
            other => panic!("se esperaba PagoPreAutorizado, se obtuvo {:?}", other),
        };

        let cobrar_msg = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "e2e-pr-2".to_string(),
            alquiler_id: "alq-e2e-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 250.0,
        };
        let r2 = enviar(addr, &cobrar_msg).await;
        assert!(matches!(r2, MsgProcesadorPagosASede::PagoAceptado { .. }));

        let reembolsar_msg = MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "e2e-pr-3".to_string(),
            alquiler_id: "alq-e2e-1".to_string(),
            cliente_id: "c-1".to_string(),
            pago_original_id: "e2e-pr-2".to_string(),
            monto_reembolso: 20.0,
        };
        let r3 = enviar(addr, &reembolsar_msg).await;
        assert!(matches!(r3, MsgProcesadorPagosASede::PagoReembolsado { .. }));

        let r1_retry = enviar(addr, &preauth_msg).await;
        match r1_retry {
            MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id } => {
                assert_eq!(payment_request_id, "e2e-pr-1");
                assert_eq!(pago_id, pago_id_1);
            }
            other => panic!("se esperaba PagoPreAutorizado identico, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 3);
    }

    #[test]
    fn monto_de_extrae_el_monto_segun_la_variante() {
        let pre = MsgSedeAProcesadorPagos::PreAutorizarPago {
            payment_request_id: "pr-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_seguridad: 11.0,
        };
        let cobro = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id: "pr-2".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            monto_final: 22.0,
        };
        let reembolso = MsgSedeAProcesadorPagos::ReembolsarPago {
            payment_request_id: "pr-3".to_string(),
            alquiler_id: "alq-1".to_string(),
            cliente_id: "c-1".to_string(),
            pago_original_id: "pago-1".to_string(),
            monto_reembolso: 33.0,
        };

        assert_eq!(monto_de(&pre), 11.0);
        assert_eq!(monto_de(&cobro), 22.0);
        assert_eq!(monto_de(&reembolso), 33.0);
    }
}
