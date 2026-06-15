use actix::{Addr, AsyncContext, Context, Handler, Message, MessageResult, ResponseFuture};
use matenmutex::common::PREMONTO;
use matenmutex::log;
use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, SolicitarBici};
use matenmutex::messages::sede_cliente::RespuestaSolicitarBici;
use matenmutex::messages::sede_procesador::{MsgProcesadorPagosASede, MsgSedeAProcesadorPagos};
use matenmutex::messages::sede_sede::{
    ActualizarDisponibilidadSolicitar, AlquilerActivo, EstadoAlquiler, GetAlquileresActivos,
    GuardarAlquiler, RemoverAlquiler, VerificarRoboMasTarde,
};
use matenmutex::slot::{Cancelar, Confirmar, Reservar, ResultadoReserva, Slot};
use matenmutex::token::{self, PayloadToken};
use matenmutex::types::{timestamp_actual, BiciAlquilada, PagoPendiente};
use std::io::Error;
use std::time::Duration;
use uuid::Uuid;

use crate::pagos::{enviar_con_descubrimiento_de_lider, generar_captura_deposito, GuardarPagoPendiente};
use crate::persistencia::PersistirEstado;
use crate::Sede;

pub(crate) fn crear_alquiler_activo(bici: &BiciAlquilada, pago_preautorizado: String) -> AlquilerActivo {
    AlquilerActivo {
        alquiler_id: bici.alquiler_id().to_string(),
        bici_id: bici.bici_id().to_string(),
        cliente_id: bici.cliente_id().to_string(),
        sede_origen_id: bici.sede_origen_id(),
        inicio_ts: bici.inicio_ts(),
        pago_preautorizacion_id: pago_preautorizado,
        deadline_devolucion_ts: bici.inicio_ts(),
        estado: EstadoAlquiler::Activo,
        pago_multa_id: Some("".to_string()),
        reembolso_multa_id: Some("".to_string()),
    }
}

impl Sede {
    fn cliente_tiene_dinero_solicitar(&self, dinero: f64) -> bool {
        dinero > PREMONTO
    }

    fn generar_comprobante(sede_id: u32, cliente_id: u32, token_secret: &[u8; 32], bici_id: String) -> Result<BiciAlquilada, token::TokenError> {
        let alquiler_id = Uuid::new_v4().to_string();
        let inicio_ts = timestamp_actual();

        let payload = PayloadToken {
            bici_id: bici_id.clone(),
            alquiler_id: alquiler_id.clone(),
            cliente_id: cliente_id.to_string(),
            sede_origen_id: sede_id,
            inicio_ts,
        };
        let token_alquiler = token::generar_token(&payload, token_secret)?;

        Ok(BiciAlquilada::new(bici_id, alquiler_id, cliente_id.to_string(), sede_id, inicio_ts, token_alquiler))
    }

    async fn reservar_bici(slots: &Vec<Addr<Slot>>) -> Option<(usize, String)> {

        let mut bici_ganadora: Option<(usize, String)> = None;
        for (idx, slot) in slots.iter().enumerate() {
            match slot.send(Reservar).await {
                Ok(ResultadoReserva::Reservada(bici_id)) => {
                    bici_ganadora = Some((idx, bici_id));
                    break;
                }
                Ok(ResultadoReserva::Rechazada) => continue,
                Err(_) => continue,
            }
        }

        bici_ganadora
    }

    pub(crate) fn generar_msg_de_cobro(monto: f64, alquiler_id: String, cliente_id: String) -> MsgSedeAProcesadorPagos {
        let payment_request_id = Uuid::new_v4().to_string();
        let cobro = MsgSedeAProcesadorPagos::CobrarPago {
            payment_request_id,
            alquiler_id,
            cliente_id,
            monto_final: monto,
        };
        cobro
    }

    async fn finalizar_reserva(
        slots: &[Addr<Slot>],
        sede_id: u32,
        cliente_id: u32,
        mut dinero: f64,
        token_secret: &[u8; 32],
        sede_addr: Addr<Sede>,
        idx: usize,
        bici_id: String,
        pago_preautorizacion_id: String,
        pago_pendiente: Option<MsgSedeAProcesadorPagos>,
    ) -> RespuestaSolicitarBici {
        match Self::generar_comprobante(sede_id, cliente_id, token_secret, bici_id) {
            Err(_) => {
                log!("[sede] Envia AlquilerRechazado: error generando token");
                let _ = slots[idx].send(Cancelar).await;
                RespuestaSolicitarBici::AlquilerRechazado(String::from("Error interno al generar el token de alquiler"))
            }
            Ok(bici) => {
                log!("[sede] Envia ComprobanteAlquilerEmitido (bici {}, alquiler {})", bici.bici_id(), bici.alquiler_id());
                let _ = sede_addr.send(GuardarReserva { alquiler_id: bici.alquiler_id().to_string(), slot_idx: idx, bici: bici.clone() }).await;
                let _ = sede_addr.send(ActualizarDisponibilidadSolicitar).await;
                let alquiler = crear_alquiler_activo(&bici, pago_preautorizacion_id);
                let alquiler_id = bici.clone().alquiler_id;
                let _ = sede_addr.send(GuardarAlquiler {alquiler}).await;

                if let Some(mensaje) = pago_pendiente {
                    let pago = PagoPendiente {
                        payment_request_id: mensaje.payment_request_id().to_string(),
                        alquiler_id: Some(alquiler_id.clone()),
                        cliente_id: cliente_id.to_string(),
                        mensaje,
                    };
                    let _ = sede_addr.send(GuardarPagoPendiente { pago }).await;
                }

                let _ = sede_addr.send(PersistirEstado).await;

                sede_addr.do_send(VerificarRoboMasTarde {alquiler_id, slot_idx: idx});

                dinero -= PREMONTO;
                RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, dinero)
            }
        }
    }

    async fn procesar_respuesta_preautorizacion(slots: Vec<Addr<Slot>>, sede_id: u32, cliente_id: u32, dinero: f64, token_secret: &[u8; 32], sede_addr: Addr<Sede>, idx: usize, bici_id: String, preautorizacion: Result<MsgProcesadorPagosASede, Error>) -> RespuestaSolicitarBici {
        match preautorizacion {
            Ok(MsgProcesadorPagosASede::PagoPreAutorizado { payment_request_id, pago_id }) => {
                log!("[sede] Recibe PagoPreAutorizado (payment_request_id {}, pago_id {})", payment_request_id, pago_id);
                Self::finalizar_reserva(&slots, sede_id, cliente_id, dinero, token_secret, sede_addr, idx, bici_id, pago_id, None).await
            }
            Ok(MsgProcesadorPagosASede::PagoRechazado { payment_request_id, motivo }) => {
                log!("[sede] Recibe PagoRechazado (payment_request_id {}, motivo {})", payment_request_id, motivo);
                let _ = slots[idx].send(Cancelar).await;
                RespuestaSolicitarBici::AlquilerRechazado(format!("El procesador de pagos rechazo la pre-autorizacion: {}", motivo))
            }
            Ok(_) => {
                log!("[sede] Respuesta inesperada del procesador de pagos");
                let _ = slots[idx].send(Cancelar).await;
                RespuestaSolicitarBici::AlquilerRechazado(String::from("Respuesta inesperada del procesador de pagos"))
            }
            Err(e) => {
                log!("[sede] Error comunicandose con el procesador de pagos: {}", e);
                let _ = slots[idx].send(Cancelar).await;
                RespuestaSolicitarBici::AlquilerRechazado(String::from("No se pudo procesar el pago de pre-autorizacion"))
            }
        }
    }
}

impl Handler<SolicitarBici> for Sede {
    type Result = ResponseFuture<RespuestaSolicitarBici>;

    fn handle(&mut self, msg: SolicitarBici, ctx: &mut Context<Self>) -> Self::Result {
        if !self.cliente_en_rango(msg.coords) {
            let motivo = String::from("Para poder tomar la bici debe estar mas cerca de la sede");
            log!("[sede] Envia AlquilerRechazado: {}", motivo);
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(motivo) });
        }

        if !self.cliente_tiene_dinero_solicitar(msg.dinero) {
            let motivo = format!("Para poder tomar la bici debe tener mas dinero (Minimo ${:.2})", PREMONTO);
            log!("[sede] Envia AlquilerRechazado: {}", motivo);
            return Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(motivo) });
        }

        let slots = self.slots.clone();
        let sede_id = self.sede_id;
        let cliente_id = msg.cliente_id;
        let dinero = msg.dinero;
        let token_secret = self.token_secret;
        let sede_addr = ctx.address();
        let procesador_pagos_addrs = self.procesador_pagos_addrs.clone();
        let lider_cacheado = self.lider_cacheado.clone();
        let offline = *self.offline.lock().unwrap_or_else(|e| e.into_inner());

        Box::pin(async move {
            match Self::reservar_bici(&slots).await {
                None => {
                    log!("[sede] Envia AlquilerRechazado: no hay slots disponibles");
                    RespuestaSolicitarBici::AlquilerRechazado(String::from("No hay bicicletas disponibles en este momento"))
                }
                Some((idx, bici_id)) => {
                    let solicitud_pre_autorizacion = MsgSedeAProcesadorPagos::PreAutorizarPago {
                        payment_request_id: Uuid::new_v4().to_string(),
                        cliente_id: cliente_id.to_string(),
                        monto_seguridad: PREMONTO as f64,
                    };

                    if offline {
                        log!("[sede] offline: {} queda pendiente, emito el comprobante igual", solicitud_pre_autorizacion.describir());
                        Self::finalizar_reserva(&slots, sede_id, cliente_id, dinero, &token_secret, sede_addr, idx, bici_id, String::new(), Some(solicitud_pre_autorizacion)).await
                    } else {
                        log!("[sede] Envia {}", solicitud_pre_autorizacion.describir());
                        let preautorizacion = enviar_con_descubrimiento_de_lider(&procesador_pagos_addrs, &lider_cacheado, solicitud_pre_autorizacion).await;
                        Self::procesar_respuesta_preautorizacion(slots, sede_id, cliente_id, dinero, &token_secret, sede_addr, idx, bici_id, preautorizacion).await
                    }
                }
            }
        })
    }
}

struct GuardarReserva {
    alquiler_id: String,
    slot_idx: usize,
    bici: BiciAlquilada,
}

impl Message for GuardarReserva {
    type Result = ();
}

impl Handler<GuardarReserva> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarReserva, _ctx: &mut Context<Self>) {
        self.pending_reservas.insert(msg.alquiler_id, (msg.slot_idx, msg.bici));
    }
}

impl Handler<AckComprobanteAlquiler> for Sede {
    type Result = ResponseFuture<RespuestaSolicitarBici>;

    fn handle(&mut self, msg: AckComprobanteAlquiler, ctx: &mut Context<Self>) -> Self::Result {
        match self.pending_reservas.remove(&msg.alquiler_id) {
            None => Box::pin(async move { RespuestaSolicitarBici::AlquilerRechazado(String::from("No hay reserva pendiente para ese alquiler")) }),
            Some((slot_idx, bici)) => {
                let slots = self.slots.clone();
                let sede_addr = ctx.address();

                log!("[sede] Recibe AckComprobanteAlquiler (alquiler {}) confirmando slot {}", msg.alquiler_id, slot_idx);
                Box::pin(async move {
                    let _ = slots[slot_idx].send(Confirmar).await;
                    let _ = sede_addr.send(PersistirEstado).await; // Persistir DEBE completarse antes de responder AlquilerConfirmado
                    log!("[sede] Envia AlquilerConfirmado (bici {})", bici.bici_id());
                    RespuestaSolicitarBici::AlquilerConfirmado(bici)
                })
            }
        }
    }
}

impl Handler<GuardarAlquiler> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarAlquiler, _ctx: &mut Self::Context) -> Self::Result {
        self.alquileres_activos.push(msg.alquiler);
    }
}

impl Handler<RemoverAlquiler> for Sede {
    type Result = ();

    fn handle(&mut self, msg: RemoverAlquiler, _ctx: &mut Self::Context) -> Self::Result {
        self.alquileres_activos.retain(|a| a.alquiler_id != msg.alquiler_id);
    }
}

impl Handler<GetAlquileresActivos> for Sede {
    type Result = MessageResult<GetAlquileresActivos>;

    fn handle(&mut self, _msg: GetAlquileresActivos, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.alquileres_activos.clone())
    }
}

pub(crate) struct GuardarPagoMulta {
    pub(crate) alquiler_id: String,
    pub(crate) pago_id: String,
}

impl Message for GuardarPagoMulta {
    type Result = ();
}

impl Handler<GuardarPagoMulta> for Sede {
    type Result = ();

    fn handle(&mut self, msg: GuardarPagoMulta, ctx: &mut Self::Context) {
        if let Some(alquiler) = self.alquileres_activos
            .iter_mut()
            .find(|a| a.alquiler_id == msg.alquiler_id)
        {
            alquiler.pago_multa_id = Some(msg.pago_id);
            ctx.address().do_send(PersistirEstado);
        }
    }
}

pub(crate) struct ActualizarPagoPreautorizacion {
    pub(crate) alquiler_id: String,
    pub(crate) pago_id: String,
}

impl Message for ActualizarPagoPreautorizacion {
    type Result = ();
}

impl Handler<ActualizarPagoPreautorizacion> for Sede {
    type Result = ();

    fn handle(&mut self, msg: ActualizarPagoPreautorizacion, ctx: &mut Self::Context) {
        if let Some(alquiler) = self.alquileres_activos.iter_mut().find(|a| a.alquiler_id == msg.alquiler_id) {
            alquiler.pago_preautorizacion_id = msg.pago_id;
            ctx.address().do_send(PersistirEstado);
        }
    }
}

impl Handler<VerificarRoboMasTarde> for Sede {
    type Result = ();
    fn handle(&mut self, msg: VerificarRoboMasTarde, ctx: &mut Context<Self>) -> Self::Result {
        ctx.run_later(Duration::from_secs(60), move |act, ctx| {
            let cliente_id = match act.alquileres_activos
                .iter_mut()
                .find(|a| a.alquiler_id == msg.alquiler_id)
            {
                Some(alquiler) if matches!(alquiler.estado, EstadoAlquiler::Activo) => {
                    log!("[sede] Bici del alquiler {} no fue devuelta, marcando como robada", msg.alquiler_id);
                    println!("[sede] ¡Bici robada! El alquiler {} no fue devuelto", msg.alquiler_id);
                    alquiler.estado = EstadoAlquiler::RoboConfirmado;
                    alquiler.cliente_id.clone()
                }
                _ => return,
            };

            let alquiler_id = msg.alquiler_id.clone();
            let cobro = generar_captura_deposito(alquiler_id.clone(), cliente_id);
            let addrs = act.procesador_pagos_addrs.clone();
            let cache = act.lider_cacheado.clone();
            let sede_addr = ctx.address();
            log!("[sede] Retengo multa de robo (${}) por alquiler {}", PREMONTO, alquiler_id);
            println!("[sede] Multa de robo retenida (${}) por alquiler {}", PREMONTO, alquiler_id);

            tokio::spawn(async move {
                match enviar_con_descubrimiento_de_lider(&addrs, &cache, cobro).await {
                    Ok(MsgProcesadorPagosASede::PagoAceptado { pago_id, .. }) => {
                        sede_addr.do_send(GuardarPagoMulta { alquiler_id, pago_id });
                    }
                    Ok(otra) => log!("[sede] Captura de multa: respuesta inesperada {}", otra.describir()),
                    Err(e) => log!("[sede] Captura de multa: error con el procesador de pagos: {}", e),
                }
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistencia::EstadoPersistenteSede;
    use crate::slots::ConsultarEstadoSlot;
    use crate::pagos::GetPagosPendientes;
    use crate::test_helpers::{sede_con_estados, sede_con_estados_path_y_procesador, sede_con_estados_y_procesador, sede_offline_con_estados_y_procesador, stub_procesador_pagos};
    use matenmutex::common::MAX_VALUE;
    use matenmutex::slot::{EstadoSlot, EstadoSlotPersistido};

    #[actix_rt::test]
    async fn solicitar_bici_con_slot_disponible_confirma_con_bici_id_real() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![
            EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Disponible {
                bici_id: "1-3".to_string(),
            },
        ], procesador_addr);

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: MAX_VALUE,
            })
            .await
            .unwrap();

        let bici_alquilada = match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => bici,
            RespuestaSolicitarBici::AlquilerRechazado(motivo) => {
                panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo AlquilerRechazado({motivo})");
            }
            RespuestaSolicitarBici::AlquilerConfirmado(_) => {
                panic!("se esperaba ComprobanteAlquilerEmitido, no AlquilerConfirmado directo");
            }
        };

        assert_eq!(bici_alquilada.bici_id(), "1-3");
        assert!(!bici_alquilada.alquiler_id().is_empty());
        assert!(bici_alquilada.inicio_ts() > 0);

        let alquiler_id = bici_alquilada.alquiler_id().to_string();
        let respuesta_final = sede
            .send(AckComprobanteAlquiler { alquiler_id })
            .await
            .unwrap();

        match respuesta_final {
            RespuestaSolicitarBici::AlquilerConfirmado(bici) => {
                assert_eq!(bici.bici_id(), "1-3");
            }
            other => panic!("se esperaba AlquilerConfirmado, se obtuvo {:?}", other),
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_offline_emite_comprobante_y_encola_pago_pendiente() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_offline_con_estados_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: MAX_VALUE,
            })
            .await
            .unwrap();

        let bici_alquilada = match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => bici,
            other => panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo {:?}", other),
        };
        assert_eq!(bici_alquilada.bici_id(), "1-1");

        let activos = sede.send(GetAlquileresActivos).await.unwrap();
        assert_eq!(activos.len(), 1, "el alquiler debe quedar activo aunque la sede este offline");

        let pendientes = sede.send(GetPagosPendientes).await.unwrap();
        assert_eq!(pendientes.len(), 1, "la preautorizacion debe quedar pendiente");
        assert!(matches!(pendientes[0].mensaje, MsgSedeAProcesadorPagos::PreAutorizarPago { .. }));
        assert_eq!(pendientes[0].alquiler_id.as_deref(), Some(bici_alquilada.alquiler_id()));
    }

    #[actix_rt::test]
    async fn solicitar_bici_sin_slots_disponibles_rechaza() {
        let sede = sede_con_estados(vec![
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "1-2".to_string(),
            },
        ]);

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: 9999999.0,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            RespuestaSolicitarBici::AlquilerConfirmado(_) | RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_, _) => {
                panic!("se esperaba AlquilerRechazado");
            }
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_concurrente_sobre_un_unico_slot_disponible_solo_uno_gana() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![EstadoSlot::Disponible {
            bici_id: "1-1".to_string(),
        }], procesador_addr);

        let sede_a = sede.clone();
        let sede_b = sede.clone();

        let (resp_a, resp_b) = futures::join!(
            sede_a.send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 9999999.0,
            }),
            sede_b.send(SolicitarBici {
                cliente_id: 2,
                coords: (-34.6037, -58.3816),
                dinero: 9999999.0,

            })
        );

        let ganadores = [resp_a.unwrap(), resp_b.unwrap()]
            .into_iter()
            .filter(|r| matches!(r, RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_,_)))
            .count();
        let rechazados = 2 - ganadores;

        assert_eq!(ganadores, 1, "exactamente un cliente debe recibir el comprobante");
        assert_eq!(rechazados, 1, "exactamente un cliente debe ser rechazado");
    }

    #[actix_rt::test]
    async fn solicitar_bici_sin_dinero_rechaza() {
        let sede = sede_con_estados(vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }]);

        let respuesta = sede.send(SolicitarBici {
            cliente_id: 1,
            coords: (-34.2, -58.2),
            dinero: PREMONTO - 1.0,
        }).await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            _ => panic!("Se esperaba alquiler rechazaddo por falta de dinero")
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_suficiente_dinero_solo_descuenta_premonto() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }], procesador_addr);

        let respuesta = sede.send(SolicitarBici {
            cliente_id: 1,
            coords: (-34.6037, -58.3816),
            dinero: PREMONTO + 1000.0,
        }).await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(_, dinero_restante) => {
                assert_eq!(dinero_restante, 1000.0, "El saldo devuelto deberia ser 1000");
            }
            other => panic!("Se esperaba ComprobanteAlquilerEmitido, se obtuvo {:?}", other)
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_preautorizacion_aceptada_emite_comprobante() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let sede = sede_con_estados_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 999999.0,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => {
                assert_eq!(bici.bici_id(), "1-1");
            }
            other => panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo {:?}", other),
        }
    }

    #[actix_rt::test]
    async fn solicitar_bici_con_preautorizacion_rechazada_cancela_la_reserva() {
        let procesador_addr = stub_procesador_pagos(true).await;
        let sede = sede_con_estados_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 1,
                coords: (-34.6037, -58.3816),
                dinero: 999999.0,
            })
            .await
            .unwrap();

        match respuesta {
            RespuestaSolicitarBici::AlquilerRechazado(_) => {}
            other => panic!("se esperaba AlquilerRechazado, se obtuvo {:?}", other),
        }

        let resultado = sede.send(ConsultarEstadoSlot { idx: 0 }).await.unwrap();
        assert_eq!(resultado, ResultadoReserva::Reservada("1-1".to_string()));
    }

    #[actix_rt::test]
    async fn ack_comprobante_alquiler_persiste_alquiler_activo_y_slot_no_disponible_antes_de_responder() {
        let procesador_addr = stub_procesador_pagos(false).await;
        let path = crate::test_helpers::path_temporal("estado_sede_ack");
        let sede = sede_con_estados_path_y_procesador(
            vec![EstadoSlot::Disponible { bici_id: "1-1".to_string() }],
            procesador_addr,
            path.clone(),
        );

        let respuesta = sede
            .send(SolicitarBici {
                cliente_id: 42,
                coords: (-34.6037, -58.3816),
                dinero: MAX_VALUE,
            })
            .await
            .unwrap();

        let bici = match respuesta {
            RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, _) => bici,
            other => panic!("se esperaba ComprobanteAlquilerEmitido, se obtuvo {:?}", other),
        };

        let alquiler_id = bici.alquiler_id().to_string();
        let respuesta_final = sede.send(AckComprobanteAlquiler { alquiler_id: alquiler_id.clone() }).await.unwrap();
        assert!(matches!(respuesta_final, RespuestaSolicitarBici::AlquilerConfirmado(_)));

        let contenido = std::fs::read_to_string(&path).expect("el snapshot debe haberse escrito");
        let snapshot: EstadoPersistenteSede = serde_json::from_str(&contenido).unwrap();

        assert!(
            !snapshot.slots.iter().any(|s| matches!(s, EstadoSlotPersistido::Disponible { bici_id } if bici_id == "1-1")),
            "el slot no debe quedar Disponible tras confirmar el alquiler"
        );

        assert!(
            snapshot.alquileres_activos.iter().any(|a| a.alquiler_id == alquiler_id),
            "alquileres_activos debe contener el alquiler confirmado"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.tmp"));
    }
}
