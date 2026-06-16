use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use matenmutex::log;
use matenmutex::messages::procesador_procesador_pagos::MsgProcesadorAProcesador;
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::types::{EstadoTransaccionPago, TransaccionPago2PC, Voto2PC};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::TcpStream;

use super::pagos::monto_de;
use crate::{id_de_addr, ProcesadorPagos, PUERTO_BASE_2PC};

pub(crate) fn addr_2pc_de_id(id: u32, ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, PUERTO_BASE_2PC + id as u16)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventoVoto2PC {
    Voto { votante_id: u32, voto: Voto2PC },
    Timeout,
}

pub(crate) fn decidir_estado_2pc(tx: &TransaccionPago2PC, timeout: bool) -> EstadoTransaccionPago {
    if tx.estado == EstadoTransaccionPago::Commit || tx.estado == EstadoTransaccionPago::Abort {
        return tx.estado;
    }

    if !tx.votos_abort.is_empty() {
        return EstadoTransaccionPago::Abort;
    }

    if timeout {
        return EstadoTransaccionPago::Abort;
    }

    let todos_votaron_commit = tx.participantes.iter().all(|p| tx.votos_commit.contains(p));
    if todos_votaron_commit && !tx.participantes.is_empty() {
        return EstadoTransaccionPago::Commit;
    }

    if tx.votos_commit.is_empty() {
        EstadoTransaccionPago::Preparando
    } else {
        EstadoTransaccionPago::Preparada
    }
}

pub(crate) fn aplicar_evento_2pc(tx: &mut TransaccionPago2PC, evento: EventoVoto2PC) {
    if tx.estado == EstadoTransaccionPago::Commit || tx.estado == EstadoTransaccionPago::Abort {
        return;
    }

    match evento {
        EventoVoto2PC::Voto { votante_id, voto } => {
            if !tx.participantes.contains(&votante_id) {
                return;
            }
            match voto {
                Voto2PC::Commit => {
                    if !tx.votos_commit.contains(&votante_id) {
                        tx.votos_commit.push(votante_id);
                    }
                }
                Voto2PC::Abort => {
                    if !tx.votos_abort.contains(&votante_id) {
                        tx.votos_abort.push(votante_id);
                    }
                }
            }
            tx.estado = decidir_estado_2pc(tx, false);
        }
        EventoVoto2PC::Timeout => {
            tx.estado = decidir_estado_2pc(tx, true);
        }
    }
}

impl ProcesadorPagos {
    pub(crate) async fn coordinar_2pc_como_lider(self: &Arc<Self>, msg: MsgSedeAProcesadorPagos) -> MsgProcesadorPagosASede {
        let payment_request_id = msg.payment_request_id().to_string();

        if self.monto_excede_limite(monto_de(&msg)) {
            let (pago, respuesta) = self.decidir_operacion(&msg);
            if let Ok(mut pagos) = self.pagos_procesados.lock() {
                pagos.push(pago);
            }
            self.persistir();
            return respuesta;
        }

        let replicas = self.peers_vivos();
        if replicas.is_empty() {
            return self.procesar_pago_local(&msg);
        }

        let participantes: Vec<u32> = replicas.iter().map(id_de_addr).collect();
        let (pago_provisorio, _) = self.decidir_operacion(&msg);
        {
            let mut transacciones = match self.transacciones_en_curso.lock() {
                Ok(g) => g,
                Err(_) => {
                    return MsgProcesadorPagosASede::PagoRechazado {
                        payment_request_id,
                        motivo: "estado de transacciones no disponible".into(),
                    }
                }
            };
            transacciones.push(TransaccionPago2PC::nueva(payment_request_id.clone(), pago_provisorio, participantes.clone()));
        }

        self.enviar_preparar_pago_a_replicas(&replicas, &msg, &payment_request_id).await;

        let estado_final = self.obtener_estado_final(&payment_request_id);

        self.broadcast_de_decision(&replicas, &payment_request_id, estado_final).await;

        let respuesta = if estado_final == EstadoTransaccionPago::Commit {
            let (pago, respuesta) = self.decidir_operacion(&msg);
            if let Ok(mut pagos) = self.pagos_procesados.lock() {
                pagos.push(pago);
            }
            respuesta
        } else {
            MsgProcesadorPagosASede::PagoRechazado {
                payment_request_id: payment_request_id.clone(),
                motivo: "transaccion 2PC abortada por las replicas".to_string(),
            }
        };

        if let Ok(mut transacciones) = self.transacciones_en_curso.lock() {
            transacciones.retain(|t| t.payment_request_id != payment_request_id);
        }

        self.persistir();
        respuesta
    }

    async fn broadcast_de_decision(self: &Arc<Self>, replicas: &[SocketAddr], payment_request_id: &String, estado_final: EstadoTransaccionPago) {
        let decision = match estado_final {
            EstadoTransaccionPago::Commit => MsgProcesadorAProcesador::CommitPago { payment_request_id: payment_request_id.clone() },
            _ => MsgProcesadorAProcesador::AbortPago { payment_request_id: payment_request_id.clone() },
        };
        for peer in replicas {
            let peer_id = id_de_addr(peer);
            let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
            let _ = self.solicitar_2pc(addr_2pc, &decision).await;
        }
    }

    fn obtener_estado_final(self: &Arc<Self>, payment_request_id: &String) -> EstadoTransaccionPago {
        let estado_final = match self.transacciones_en_curso.lock() {
            Ok(transacciones) => transacciones
                .iter()
                .find(|t| t.payment_request_id == *payment_request_id)
                .map(|t| t.estado),
            Err(_) => None,
        };
        estado_final.unwrap_or(EstadoTransaccionPago::Abort)
    }

    async fn enviar_preparar_pago_a_replicas(self: &Arc<Self>, replicas: &[SocketAddr], msg: &MsgSedeAProcesadorPagos, payment_request_id: &String) {
        for peer in replicas {
            let peer_id = id_de_addr(peer);
            let preparar = MsgProcesadorAProcesador::PrepararPago {
                payment_request_id: payment_request_id.clone(),
                pago: msg.clone(),
            };
            let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
            let respuesta = self.solicitar_2pc(addr_2pc, &preparar).await;

            let evento = match respuesta {
                Some(MsgProcesadorAProcesador::VotoPago { votante_id, voto, .. }) => {
                    EventoVoto2PC::Voto { votante_id, voto }
                }
                _ => EventoVoto2PC::Timeout,
            };

            if let Ok(mut transacciones) = self.transacciones_en_curso.lock() {
                if let Some(tx) = transacciones.iter_mut().find(|t| t.payment_request_id == *payment_request_id) {
                    aplicar_evento_2pc(tx, evento);
                }
            }
        }
    }

    async fn solicitar_2pc(&self, addr: SocketAddr, msg: &MsgProcesadorAProcesador) -> Option<MsgProcesadorAProcesador> {
        log!("[{}]: 2PC envia {} a {}", self.etiqueta(), msg.describir(), addr);

        let respuesta: Option<MsgProcesadorAProcesador> = tokio::time::timeout(self.tiempos.espera_ok, async {
            let stream = TcpStream::connect(addr).await.ok()?;
            let (reader, mut writer) = stream.into_split();
            let payload = serde_json::to_string(msg).ok()? + "\n";
            writer.write_all(payload.as_bytes()).await.ok()?;

            let mut lines = TokioBufReader::new(reader).lines();
            let line = lines.next_line().await.ok()??;
            serde_json::from_str(line.trim()).ok()
        })
        .await
        .ok()
        .flatten();

        match &respuesta {
            Some(resp) => log!("[{}]: 2PC recibe {} de {}", self.etiqueta(), resp.describir(), addr),
            None => log!("[{}]: 2PC sin respuesta de {} (timeout o conexion fallida)", self.etiqueta(), addr),
        }

        respuesta
    }

    pub(crate) async fn accept_loop_2pc(self: Arc<Self>) {
        loop {
            match self.bus_2pc.accept().await {
                Ok((socket, addr)) => {
                    log!("[{}]: Nueva conexion 2PC desde {}", self.etiqueta(), addr);
                    tokio::spawn(self.clone().atender_procesador(socket));
                }
                Err(e) => log!("[{}]: error aceptando conexion 2PC: {}", self.etiqueta(), e),
            }
        }
    }

    async fn atender_procesador(self: Arc<Self>, socket: TcpStream) {
        let (reader, mut writer) = socket.into_split();
        let mut lines = TokioBufReader::new(reader).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let resp = match serde_json::from_str::<MsgProcesadorAProcesador>(line.trim()) {
                Ok(msg) => {
                    log!("[{}]: 2PC recibe {}", self.etiqueta(), msg.describir());
                    self.manejar_mensaje_procesador_a_procesador(msg)
                }
                Err(_) => {
                    log!("[{}]: 2PC recibe mensaje invalido: {}", self.etiqueta(), line.trim());
                    None
                }
            };

            let Some(resp) = resp else { continue };
            log!("[{}]: 2PC envia {}", self.etiqueta(), resp.describir());
            let payload = match serde_json::to_string(&resp) {
                Ok(json) => json + "\n",
                Err(_) => continue,
            };
            if writer.write_all(payload.as_bytes()).await.is_err() {
                break;
            }
        }
    }

    fn manejar_mensaje_procesador_a_procesador(&self, msg: MsgProcesadorAProcesador) -> Option<MsgProcesadorAProcesador> {
        match msg {
            MsgProcesadorAProcesador::PrepararPago { payment_request_id, pago } => {
                let voto = if self.monto_excede_limite(monto_de(&pago)) {
                    Voto2PC::Abort
                } else {
                    Voto2PC::Commit
                };
                if let Ok(mut preparados) = self.preparados_replica.lock() {
                    preparados.insert(payment_request_id.clone(), pago);
                }
                self.persistir();
                Some(MsgProcesadorAProcesador::VotoPago {
                    payment_request_id,
                    votante_id: self.procesador_id,
                    voto,
                })
            }
            MsgProcesadorAProcesador::CommitPago { payment_request_id } => {
                self.persistir_commit_replica(&payment_request_id);
                self.persistir();
                Some(MsgProcesadorAProcesador::AckDecisionPago {
                    payment_request_id,
                    votante_id: self.procesador_id,
                })
            }
            MsgProcesadorAProcesador::AbortPago { payment_request_id } => {
                if let Ok(mut preparados) = self.preparados_replica.lock() {
                    preparados.remove(&payment_request_id);
                }
                self.persistir();
                Some(MsgProcesadorAProcesador::AckDecisionPago {
                    payment_request_id,
                    votante_id: self.procesador_id,
                })
            }
            MsgProcesadorAProcesador::ConsultarPago { payment_request_id } => {
                let pago = match self.pagos_procesados.lock() {
                    Ok(pagos) => pagos.iter().find(|p| p.payment_request_id == payment_request_id).cloned(),
                    Err(_) => None,
                };
                Some(MsgProcesadorAProcesador::RespuestaConsultaPago { payment_request_id, pago })
            }
            _ => None,
        }
    }

    pub(crate) async fn recuperar_transacciones_huerfanas(&self) {
        let ids: Vec<String> = match self.preparados_replica.lock() {
            Ok(preparados) => preparados.keys().cloned().collect(),
            Err(_) => Vec::new(),
        };

        for payment_request_id in ids {
            let mut hay_commit = match self.pagos_procesados.lock() {
                Ok(pagos) => pagos.iter().any(|p| p.payment_request_id == payment_request_id),
                Err(_) => false,
            };

            let consulta = MsgProcesadorAProcesador::ConsultarPago { payment_request_id: payment_request_id.clone() };
            for peer in self.peers_vivos() {
                let peer_id = id_de_addr(&peer);
                let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
                if let Some(MsgProcesadorAProcesador::RespuestaConsultaPago { pago: Some(_), .. }) = self.solicitar_2pc(addr_2pc, &consulta).await {
                    hay_commit = true;
                }
            }

            if hay_commit {
                self.broadcast_decision_recuperacion(&payment_request_id, EstadoTransaccionPago::Commit).await;
                self.persistir_commit_replica(&payment_request_id);
            } else {
                self.broadcast_decision_recuperacion(&payment_request_id, EstadoTransaccionPago::Abort).await;
                if let Ok(mut preparados) = self.preparados_replica.lock() {
                    preparados.remove(&payment_request_id);
                }
            }
            self.persistir();
        }
    }

    async fn broadcast_decision_recuperacion(&self, payment_request_id: &str, estado_final: EstadoTransaccionPago) {
        let decision = match estado_final {
            EstadoTransaccionPago::Commit => MsgProcesadorAProcesador::CommitPago { payment_request_id: payment_request_id.to_string() },
            _ => MsgProcesadorAProcesador::AbortPago { payment_request_id: payment_request_id.to_string() },
        };
        for peer in self.peers_vivos() {
            let peer_id = id_de_addr(&peer);
            let addr_2pc = addr_2pc_de_id(peer_id, peer.ip());
            let _ = self.solicitar_2pc(addr_2pc, &decision).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use crate::bully::RolActual;
    use crate::{addr_de_id, PUERTO_BASE};
    use matenmutex::types::{EstadoPago, PagoProcesado, TipoOperacionPago};
    use std::time::{Duration, Instant};

    fn pago_de_prueba(payment_request_id: &str) -> PagoProcesado {
        PagoProcesado {
            payment_request_id: payment_request_id.to_string(),
            pago_id: "pago-2pc".to_string(),
            alquiler_id: None,
            cliente_id: "c-2pc".to_string(),
            monto: 100.0,
            tipo: TipoOperacionPago::CobroFinal,
            estado: EstadoPago::Aceptado,
        }
    }

    #[tokio::test]
    async fn nodo_de_prueba_arranca_con_transacciones_en_curso_vacio() {
        let nodo = nodo_de_prueba(20, 0, RolActual::Lider).await;
        let transacciones = nodo.transacciones_en_curso.lock().unwrap();
        assert!(transacciones.is_empty());
    }

    #[test]
    fn voto_unico_commit_decide_commit() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-1".to_string(), pago_de_prueba("pr-sm-1"), vec![1]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });

        assert_eq!(tx.votos_commit, vec![1]);
        assert!(tx.votos_abort.is_empty());
        assert_eq!(tx.estado, EstadoTransaccionPago::Commit);
    }

    #[test]
    fn un_abort_entre_varios_decide_abort_y_es_terminal() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-2".to_string(), pago_de_prueba("pr-sm-2"), vec![1, 2, 3]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });
        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 2, voto: Voto2PC::Abort });

        assert_eq!(tx.votos_commit, vec![1]);
        assert_eq!(tx.votos_abort, vec![2]);
        assert_eq!(tx.estado, EstadoTransaccionPago::Abort);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 3, voto: Voto2PC::Commit });
        assert_eq!(tx.estado, EstadoTransaccionPago::Abort);
        assert!(!tx.votos_commit.contains(&3));
    }

    #[test]
    fn dos_participantes_unanime_commit_decide_commit() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-3".to_string(), pago_de_prueba("pr-sm-3"), vec![1, 2]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });
        assert_eq!(tx.estado, EstadoTransaccionPago::Preparada);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 2, voto: Voto2PC::Commit });
        assert_eq!(tx.votos_commit, vec![1, 2]);
        assert_eq!(tx.estado, EstadoTransaccionPago::Commit);
    }

    #[test]
    fn timeout_antes_de_recibir_todos_los_votos_decide_abort() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-4".to_string(), pago_de_prueba("pr-sm-4"), vec![1, 2]);
        tx.votos_commit.push(1);
        tx.estado = EstadoTransaccionPago::Preparada;

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Timeout);

        assert_eq!(tx.estado, EstadoTransaccionPago::Abort);
    }

    #[test]
    fn timeout_sobre_transaccion_ya_decidida_es_no_op() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-5".to_string(), pago_de_prueba("pr-sm-5"), vec![1, 2]);
        tx.votos_commit = vec![1, 2];
        tx.estado = EstadoTransaccionPago::Commit;

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Timeout);

        assert_eq!(tx.estado, EstadoTransaccionPago::Commit);
    }

    #[test]
    fn voto_duplicado_del_mismo_participante_es_idempotente() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-6".to_string(), pago_de_prueba("pr-sm-6"), vec![1, 2]);
        tx.votos_commit.push(1);
        tx.estado = EstadoTransaccionPago::Preparada;

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 1, voto: Voto2PC::Commit });

        assert_eq!(tx.votos_commit, vec![1]);
        assert_eq!(tx.estado, EstadoTransaccionPago::Preparada);
    }

    #[test]
    fn voto_de_participante_desconocido_se_ignora() {
        let mut tx = TransaccionPago2PC::nueva("pr-sm-7".to_string(), pago_de_prueba("pr-sm-7"), vec![1, 2]);

        aplicar_evento_2pc(&mut tx, EventoVoto2PC::Voto { votante_id: 99, voto: Voto2PC::Commit });

        assert!(tx.votos_commit.is_empty());
        assert!(tx.votos_abort.is_empty());
        assert_eq!(tx.estado, EstadoTransaccionPago::Preparando);
    }

    #[test]
    fn addr_2pc_de_id_aplica_offset_de_1000_sobre_puerto_base() {
        for id in 1..=10 {
            let esperado = SocketAddr::new(ip(), PUERTO_BASE + 1000 + id as u16);
            assert_eq!(addr_2pc_de_id(id, ip()), esperado);
            assert!(addr_2pc_de_id(id, ip()).port() > addr_de_id(id, ip()).port());
        }
    }

    #[tokio::test]
    async fn nodo_de_prueba_tiene_bus_2pc_bindeado() {
        let nodo = nodo_de_prueba(21, 0, RolActual::Lider).await;
        assert!(nodo.bus_2pc.local_addr().is_ok());
    }

    #[tokio::test]
    async fn replica_vota_commit_si_el_monto_no_excede_su_limite() {
        let nodo = nodo_de_prueba_con_limite(30, 0, RolActual::Replica { lider_id: 1 }, Some(1000.0)).await;

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-1".to_string(),
            pago: cobrar_msg("pr-rep-1", 100.0),
        });

        match resp {
            Some(MsgProcesadorAProcesador::VotoPago { payment_request_id, votante_id, voto }) => {
                assert_eq!(payment_request_id, "pr-rep-1");
                assert_eq!(votante_id, 30);
                assert_eq!(voto, Voto2PC::Commit);
            }
            other => panic!("se esperaba VotoPago{{Commit}}, se obtuvo {:?}", other),
        }

        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn replica_vota_abort_si_el_monto_excede_su_limite() {
        let nodo = nodo_de_prueba_con_limite(31, 0, RolActual::Replica { lider_id: 1 }, Some(50.0)).await;

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-2".to_string(),
            pago: cobrar_msg("pr-rep-2", 100.0),
        });

        match resp {
            Some(MsgProcesadorAProcesador::VotoPago { payment_request_id, votante_id, voto }) => {
                assert_eq!(payment_request_id, "pr-rep-2");
                assert_eq!(votante_id, 31);
                assert_eq!(voto, Voto2PC::Abort);
            }
            other => panic!("se esperaba VotoPago{{Abort}}, se obtuvo {:?}", other),
        }

        assert!(nodo.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn replica_aplica_commit_pago_y_persiste() {
        let nodo = nodo_de_prueba(32, 0, RolActual::Replica { lider_id: 1 }).await;

        let _ = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-3".to_string(),
            pago: cobrar_msg("pr-rep-3", 100.0),
        });

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-rep-3".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::AckDecisionPago { payment_request_id, votante_id }) => {
                assert_eq!(payment_request_id, "pr-rep-3");
                assert_eq!(votante_id, 32);
            }
            other => panic!("se esperaba AckDecisionPago, se obtuvo {:?}", other),
        }

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
        assert_eq!(pagos[0].payment_request_id, "pr-rep-3");
    }

    #[tokio::test]
    async fn replica_aplica_commit_pago_idempotentemente() {
        let nodo = nodo_de_prueba(33, 0, RolActual::Replica { lider_id: 1 }).await;

        let _ = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-4".to_string(),
            pago: cobrar_msg("pr-rep-4", 100.0),
        });

        let r1 = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-rep-4".to_string(),
        });
        let r2 = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-rep-4".to_string(),
        });

        assert!(matches!(r1, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));
        assert!(matches!(r2, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));

        let pagos = nodo.pagos_procesados.lock().unwrap();
        assert_eq!(pagos.len(), 1);
    }

    #[tokio::test]
    async fn replica_descarta_abort_pago_y_no_persiste() {
        let nodo = nodo_de_prueba(34, 0, RolActual::Replica { lider_id: 1 }).await;

        let _ = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::PrepararPago {
            payment_request_id: "pr-rep-5".to_string(),
            pago: cobrar_msg("pr-rep-5", 100.0),
        });

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::AbortPago {
            payment_request_id: "pr-rep-5".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::AckDecisionPago { payment_request_id, votante_id }) => {
                assert_eq!(payment_request_id, "pr-rep-5");
                assert_eq!(votante_id, 34);
            }
            other => panic!("se esperaba AckDecisionPago, se obtuvo {:?}", other),
        }

        assert!(!nodo.pagos_procesados.lock().unwrap().iter().any(|p| p.payment_request_id == "pr-rep-5"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_dos_replicas_que_votan_commit_persiste_y_responde_ok() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(140, RolActual::Lider, vec![141, 142], None).await;
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(141, RolActual::Replica { lider_id: 140 }, vec![], None).await;
        let _r2 = nodo_2pc_de_prueba_en_addr_canonica(142, RolActual::Replica { lider_id: 140 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-commit", 100.0)).await;
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoAceptado { .. }), "se esperaba PagoAceptado, se obtuvo {:?}", respuesta);

        assert_eq!(lider.pagos_procesados.lock().unwrap().len(), 1);
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(_r1.pagos_procesados.lock().unwrap().len(), 1);
        assert_eq!(_r2.pagos_procesados.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_una_replica_que_vota_abort_no_persiste_y_rechaza() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(150, RolActual::Lider, vec![151, 152], None).await;
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(151, RolActual::Replica { lider_id: 150 }, vec![], None).await;
        let _r2 = nodo_2pc_de_prueba_en_addr_canonica(152, RolActual::Replica { lider_id: 150 }, vec![], Some(10.0)).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-abort", 100.0)).await;
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoRechazado { .. }), "se esperaba PagoRechazado, se obtuvo {:?}", respuesta);

        assert!(lider.pagos_procesados.lock().unwrap().is_empty());
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(_r1.pagos_procesados.lock().unwrap().is_empty());
        assert!(_r2.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_replica_inalcanzable_decide_abort_por_timeout() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(160, RolActual::Lider, vec![161, 162], None).await;
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(161, RolActual::Replica { lider_id: 160 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-timeout", 100.0)).await;
        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoRechazado { .. }), "se esperaba PagoRechazado, se obtuvo {:?}", respuesta);

        assert!(lider.pagos_procesados.lock().unwrap().is_empty());
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(_r1.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_con_lider_propio_voto_abort_corta_sin_red() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(170, RolActual::Replica { lider_id: 0 }, vec![171], Some(10.0)).await;
        lider.set_rol(RolActual::Lider);
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(171, RolActual::Replica { lider_id: 170 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let inicio = Instant::now();
        let respuesta = lider.procesar_pago(cobrar_msg("pr-2pc-self-abort", 100.0)).await;
        let transcurrido = inicio.elapsed();

        assert!(matches!(respuesta, MsgProcesadorPagosASede::PagoRechazado { .. }), "se esperaba PagoRechazado, se obtuvo {:?}", respuesta);
        assert!(lider.transacciones_en_curso.lock().unwrap().is_empty());
        assert!(transcurrido < Duration::from_millis(500), "tardo demasiado: {:?}", transcurrido);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(_r1.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lider_2pc_payment_request_id_repetido_no_repite_la_ronda() {
        let lider = nodo_2pc_de_prueba_en_addr_canonica(180, RolActual::Lider, vec![181], None).await;
        let _r1 = nodo_2pc_de_prueba_en_addr_canonica(181, RolActual::Replica { lider_id: 180 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let msg = cobrar_msg("pr-2pc-dup", 100.0);
        let r1 = lider.procesar_pago(msg.clone()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let r2 = lider.procesar_pago(msg).await;

        assert_eq!(r1, r2);
        assert_eq!(lider.pagos_procesados.lock().unwrap().len(), 1);
        assert_eq!(_r1.pagos_procesados.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn consultar_pago_responde_some_si_esta_committeado() {
        let nodo = nodo_de_prueba(200, 0, RolActual::Replica { lider_id: 1 }).await;
        nodo.pagos_procesados.lock().unwrap().push(pago_de_prueba("pr-cons-1"));

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::ConsultarPago {
            payment_request_id: "pr-cons-1".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::RespuestaConsultaPago { payment_request_id, pago: Some(pago) }) => {
                assert_eq!(payment_request_id, "pr-cons-1");
                assert_eq!(pago.payment_request_id, "pr-cons-1");
            }
            otro => panic!("se esperaba RespuestaConsultaPago con Some, se obtuvo {:?}", otro),
        }
    }

    #[tokio::test]
    async fn consultar_pago_responde_none_si_no_esta_committeado() {
        let nodo = nodo_de_prueba(201, 0, RolActual::Replica { lider_id: 1 }).await;

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::ConsultarPago {
            payment_request_id: "pr-cons-2".to_string(),
        });

        match resp {
            Some(MsgProcesadorAProcesador::RespuestaConsultaPago { payment_request_id, pago: None }) => {
                assert_eq!(payment_request_id, "pr-cons-2");
            }
            otro => panic!("se esperaba RespuestaConsultaPago con None, se obtuvo {:?}", otro),
        }
    }

    #[tokio::test]
    async fn persistir_commit_replica_es_idempotente_y_remueve_de_preparados() {
        let nodo = nodo_de_prueba(202, 0, RolActual::Replica { lider_id: 1 }).await;
        nodo.preparados_replica.lock().unwrap().insert("pr-commit-1".to_string(), cobrar_msg("pr-commit-1", 100.0));

        let resp = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-commit-1".to_string(),
        });

        assert!(matches!(resp, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));
        assert_eq!(nodo.pagos_procesados.lock().unwrap().len(), 1);
        assert!(!nodo.preparados_replica.lock().unwrap().contains_key("pr-commit-1"));

        let resp2 = nodo.manejar_mensaje_procesador_a_procesador(MsgProcesadorAProcesador::CommitPago {
            payment_request_id: "pr-commit-1".to_string(),
        });
        assert!(matches!(resp2, Some(MsgProcesadorAProcesador::AckDecisionPago { .. })));
        assert_eq!(nodo.pagos_procesados.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recuperacion_huerfana_commitea_si_un_peer_tiene_evidencia() {
        let nuevo_lider = nodo_2pc_de_prueba_en_addr_canonica(210, RolActual::Replica { lider_id: 0 }, vec![211], None).await;
        let _peer = nodo_2pc_de_prueba_en_addr_canonica(211, RolActual::Replica { lider_id: 0 }, vec![], None).await;

        nuevo_lider.preparados_replica.lock().unwrap().insert("pr-orphan-commit".to_string(), cobrar_msg("pr-orphan-commit", 50.0));
        _peer.pagos_procesados.lock().unwrap().push(pago_de_prueba("pr-orphan-commit"));

        tokio::time::sleep(Duration::from_millis(50)).await;

        nuevo_lider.proclamarse_lider().await;

        assert!(nuevo_lider.pagos_procesados.lock().unwrap().iter().any(|p| p.payment_request_id == "pr-orphan-commit"));
        assert!(!nuevo_lider.preparados_replica.lock().unwrap().contains_key("pr-orphan-commit"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recuperacion_huerfana_abortea_si_nadie_tiene_evidencia() {
        let nuevo_lider = nodo_2pc_de_prueba_en_addr_canonica(220, RolActual::Replica { lider_id: 0 }, vec![221], None).await;
        let _peer = nodo_2pc_de_prueba_en_addr_canonica(221, RolActual::Replica { lider_id: 0 }, vec![], None).await;

        nuevo_lider.preparados_replica.lock().unwrap().insert("pr-orphan-abort".to_string(), cobrar_msg("pr-orphan-abort", 50.0));

        tokio::time::sleep(Duration::from_millis(50)).await;

        nuevo_lider.proclamarse_lider().await;

        assert!(!nuevo_lider.pagos_procesados.lock().unwrap().iter().any(|p| p.payment_request_id == "pr-orphan-abort"));
        assert!(!nuevo_lider.preparados_replica.lock().unwrap().contains_key("pr-orphan-abort"));
        assert!(_peer.pagos_procesados.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recuperacion_huerfana_es_no_op_si_preparados_replica_esta_vacio() {
        let nuevo_lider = nodo_2pc_de_prueba_en_addr_canonica(230, RolActual::Replica { lider_id: 0 }, vec![231], None).await;
        let _peer = nodo_2pc_de_prueba_en_addr_canonica(231, RolActual::Replica { lider_id: 0 }, vec![], None).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        nuevo_lider.proclamarse_lider().await;

        assert!(nuevo_lider.pagos_procesados.lock().unwrap().is_empty());
        assert!(nuevo_lider.preparados_replica.lock().unwrap().is_empty());
        assert!(_peer.pagos_procesados.lock().unwrap().is_empty());
        assert_eq!(rol_de(&nuevo_lider), RolActual::Lider);
    }
}
