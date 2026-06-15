use actix::{Addr, AsyncContext, Context, Handler, MailboxError, MessageResult};
use matenmutex::log;
use matenmutex::messages::cliente_sede::{Handshake, MsgClienteASede, PedirSedes, RespuestaHandshake};
use matenmutex::messages::sede_sede::{Heartbeat, MsgSedeASede, SedeInfo};
use matenmutex::types::timestamp_actual;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::devoluciones::FlushNotificacionesPendientes;
use crate::Sede;

impl Handler<Handshake> for Sede {
    type Result = MessageResult<Handshake>;

    fn handle(&mut self, msg: Handshake, _ctx: &mut Self::Context) -> Self::Result {
        if *self.offline.lock().unwrap_or_else(|e| e.into_inner()) && !self.cliente_en_rango(msg.coords) {
            MessageResult(RespuestaHandshake::Rechazado("Sede offline y fuera de rango".parse().unwrap()))
        } else {
            MessageResult(RespuestaHandshake::Aceptado(self.coords))
        }
    }
}

impl Handler<PedirSedes> for Sede {
    type Result = MessageResult<PedirSedes>;

    fn handle(&mut self, msg: PedirSedes, _ctx: &mut Self::Context) -> Self::Result {
        if *self.offline.lock().unwrap_or_else(|e| e.into_inner()) && !self.cliente_en_rango(msg.coords) {
            log!("[sede] PedirSedes rechazado: sede offline y cliente fuera de rango");
            return MessageResult(vec![]);
        }

        let sedes_cercanas: Vec<SedeInfo> = self.sedes_conocidas
            .iter()
            .cloned()
            .collect();
        let mut resultado = sedes_cercanas;
        resultado.push(self.sede_info.clone());
         MessageResult(resultado)
    }
}

impl Handler<Heartbeat> for Sede {
    type Result = ();

    fn handle(&mut self, msg: Heartbeat, ctx: &mut Context<Self>) -> () {
        log!("[sede] Recibe Heartbeat de {}", msg.sede_id);
        log!("[sede] info: {:?}", msg.info); //para debug

        let mut info = msg.info;
        // Liveness con hora local: "lo escuche recien", no la auto-reportada del emisor.
        info.ultimo_update_ts = timestamp_actual();
        info.conectada = true;

        match self.sedes_conocidas.iter_mut().find(|s| s.sede_id == info.sede_id) {
            Some(sede) => {
                if !sede.conectada {
                    log!("[sede {}]: la sede {} volvio a estar VIVA", self.sede_id, info.sede_id);
                    println!("{}[sede {}]: la sede {} esta ONLINE de nuevo{}", crate::GREEN, self.sede_id, info.sede_id, crate::RESET);
                    ctx.address().do_send(FlushNotificacionesPendientes);
                }
                *sede = info;
            }
            None => self.sedes_conocidas.push(info),
        }
    }
}

pub(crate) struct ActualizarEstado {
    pub(crate) estado: bool,
}

impl actix::Message for ActualizarEstado {
    type Result = ();
}

impl Handler<ActualizarEstado> for Sede {
    type Result = ();

    fn handle(&mut self, msg: ActualizarEstado, _: &mut Self::Context) -> Self::Result {
        self.sede_info.conectada = msg.estado;
    }
}

pub(crate) async fn formatear_respuesta<T: Serialize>(resultado: Result<T, MailboxError>) -> String {
    match resultado {
        Ok(valor) => match serde_json::to_string(&valor) {
            Ok(json) => json + "\n",
            Err(e) => {
                log!("Error serializando respuesta: {}", e);
                String::from("ERROR:no se pudo serializar la respuesta\n")
            }
        },
        Err(e) => {
            log!("Error enviando mensaje al actor Sede: {}", e);
            String::from("ERROR:la sede no pudo procesar el mensaje\n")
        }
    }

}

pub(crate) async fn loop_sedes(sede: Addr<Sede>, listener: TcpListener, offline: Arc<Mutex<bool>>, despertar: Arc<Notify>) {
    loop {
        // Offline: Nos dormimos hasta que la CLI vuelva online
        // y dispare notify_waiters. loop_pub sigue aparte, asi que se atienden clientes locales.
        if *offline.lock().unwrap_or_else(|e| e.into_inner()) {
            log!("[sede] offline — aislada de otras sedes, esperando online");
            despertar.notified().await;
            log!("[sede] online — reanudando comunicacion entre sedes");
            sede.do_send(FlushNotificacionesPendientes);
        }

        let (socket, _) = listener.accept().await.unwrap();

        let sede = sede.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {

                let response = match serde_json::from_str::<MsgSedeASede>(&line) {
                    Ok(MsgSedeASede::NotificarDevolucion(msg)) => {
                        match sede.send(msg).await {
                            Ok(ack) => {
                                serde_json::to_string(&ack)
                                    .unwrap() + "\n"
                            }
                            Err(_) => {
                                "ERROR\n".to_string()
                            }
                        }
                    }
                    Err(_) => {
                        "UNKNOWN\n".to_string()
                    }
                };
                writer.write_all(response.as_bytes()).await.ok();
            }
        });
    }
}

pub(crate) async fn loop_pub(sede: Addr<Sede>, public_listener: TcpListener) {
    loop {
        let (socket, addr) = match public_listener.accept().await {
            Ok(conexion) => conexion,
            Err(e) => {
                log!("Error aceptando conexion entrante: {}", e);
                continue;
            }
        };
        log!("New connection from {}", addr);
        let sede = sede.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let response = match serde_json::from_str::<MsgClienteASede>(line.trim()) {
                    Ok(MsgClienteASede::Handshake(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::SolicitarBici(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::AckComprobanteAlquiler(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::DevolverBici(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::PreguntarBicis(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Ok(MsgClienteASede::PedirSedes(msg)) => {
                        formatear_respuesta(sede.send(msg).await).await
                    }
                    Err(_) => format!("UNKNOWN:{}\n", line.trim()),
                };

                if let Err(e) = writer.write_all(response.as_bytes()).await {
                    log!("Error escribiendo respuesta a {}: {}", addr, e);
                    break;
                }
            }
            log!("Connection from {} closed", addr);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::sede_con_estados;
    use matenmutex::messages::sede_sede::{crear_info_dummy, GetSedesConocidas};

    #[actix_rt::test]
    async fn heartbeat_actualiza_sede_ya_conocida_sin_duplicar() {
        let sede = sede_con_estados(vec![]);

        let mut info_sede_extra1 = crear_info_dummy();
        info_sede_extra1.sede_id = 99;
        info_sede_extra1.slots_disponibles = 5;

        sede.send(Heartbeat { sede_id: info_sede_extra1.sede_id, info: info_sede_extra1 })
            .await
            .unwrap();

        let mut info_sede_extra2 = crear_info_dummy();
        info_sede_extra2.sede_id = 99;
        info_sede_extra2.slots_disponibles = 3;

        sede.send(Heartbeat { sede_id: info_sede_extra2.sede_id, info: info_sede_extra2 })
            .await
            .unwrap();

        let conocidas = sede.send(GetSedesConocidas).await.unwrap();
        assert_eq!(conocidas.len(), 1, "no debe duplicarse la sede al recibir un segundo heartbeat");
        assert_eq!(conocidas[0].slots_disponibles, 3, "debe reflejar el heartbeat más reciente");
    }
}
