use actix::{AsyncContext, Context, Handler, ResponseFuture};
use matenmutex::messages::cliente_sede::PreguntarBicis;
use matenmutex::messages::sede_sede::{ActualizarDisponibilidadDevolver, ActualizarDisponibilidadSolicitar};
use matenmutex::slot::{ActualizarSlots, ConsultarBici, SetSlotCounts};
use matenmutex::types::timestamp_actual;

use crate::Sede;

impl Handler<ActualizarDisponibilidadSolicitar> for Sede {
    type Result = ();

    fn handle(&mut self, _msg: ActualizarDisponibilidadSolicitar, _ctx: &mut Self::Context) -> Self::Result {

        self.sede_info.slots_disponibles = self.sede_info.slots_disponibles.saturating_sub(1);
        self.sede_info.slots_vacios      = self.sede_info.slots_vacios.saturating_add(1);
        self.sede_info.ultimo_update_ts = timestamp_actual();
    }
}

impl Handler<ActualizarDisponibilidadDevolver> for Sede { //maybe algo que revise los slots? por ahora bien basicon
    type Result = ();

    fn handle(&mut self, _msg: ActualizarDisponibilidadDevolver, _ctx: &mut Self::Context) -> Self::Result {
        self.sede_info.slots_disponibles = self.sede_info.slots_disponibles.saturating_add(1);
        self.sede_info.slots_vacios      = self.sede_info.slots_vacios.saturating_sub(1);
        self.sede_info.ultimo_update_ts = timestamp_actual();
    }
}

impl Handler<ActualizarSlots> for Sede {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: ActualizarSlots, ctx: &mut Context<Self>) -> Self::Result {
        let requests: Vec<_> = self
            .slots
            .iter()
            .map(|slot| slot.send(ConsultarBici))
            .collect();

        let addr = ctx.address(); // clonable, se puede mover al async block

        Box::pin(async move {
            let resultados = futures::future::join_all(requests).await;
            let lineas: Vec<String> = resultados.into_iter().filter_map(|r| r.ok()).collect();
            let total = lineas.len();
            let ocupados = lineas.iter().filter(|l| !l.contains("Vacio")).count();
            let vacios = total - ocupados;

            addr.do_send(SetSlotCounts { ocupados: ocupados as u8, vacios: vacios as u8 });
        })
    }
}

impl Handler<SetSlotCounts> for Sede {
    type Result = ();

    fn handle(&mut self, msg: SetSlotCounts, _ctx: &mut Context<Self>) {
        self.sede_info.slots_disponibles = msg.ocupados;
        self.sede_info.slots_vacios = msg.vacios;
    }
}

impl Handler<PreguntarBicis> for Sede {
    type Result = ResponseFuture<String>;

    fn handle(&mut self, _msg: PreguntarBicis, _ctx: &mut Context<Self>) -> Self::Result {

        let requests: Vec<_> = self
            .slots
            .iter()
            .map(|slot| slot.send(ConsultarBici)) // mensaje a cada slot
            .collect();

        let sede_id = self.sede_id;

        Box::pin(async move {
            let resultados = futures::future::join_all(requests).await;
            let lineas: Vec<String> = resultados.into_iter().filter_map(|r| r.ok()).collect();

            let ocupados = lineas.iter().filter(|l| !l.contains("Vacio")).count();
            let total = lineas.len();

            let mut respuesta = format!("=== Estado de Sede {} ===\n", sede_id);
            respuesta.push_str(&lineas.join("\n"));
            respuesta.push_str(&format!("\n--- {}/{} slots ocupados ---", ocupados, total));
            respuesta
        })
    }
}

#[cfg(test)]
pub(crate) struct ConsultarEstadoSlot {
    pub(crate) idx: usize,
}

#[cfg(test)]
impl actix::Message for ConsultarEstadoSlot {
    type Result = matenmutex::slot::ResultadoReserva;
}

#[cfg(test)]
impl Handler<ConsultarEstadoSlot> for Sede {
    type Result = ResponseFuture<matenmutex::slot::ResultadoReserva>;

    fn handle(&mut self, msg: ConsultarEstadoSlot, _ctx: &mut Context<Self>) -> Self::Result {
        let slot = self.slots[msg.idx].clone();
        Box::pin(async move { slot.send(matenmutex::slot::Reservar).await.unwrap_or(matenmutex::slot::ResultadoReserva::Rechazada) })
    }
}
