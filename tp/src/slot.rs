use actix::{Actor, Addr, Context, Handler, Message};

pub enum EstadoSlot {
    Vacio,
    Reservado { bici_id: String },
    Disponible { bici_id: String },
}
pub struct Slot {
    pub slot_id: u32,
    pub estado: EstadoSlot,
}

impl Actor for Slot {
    type Context = Context<Self>;
}

pub fn crear_slots(sede_id: u32, num_slots: u32) -> Vec<Addr<Slot>> {
    let slots: Vec<Addr<Slot>> = (1..=num_slots)
        .map(|i| {
            let estado = match rand::random::<u8>() % 2 {
                0 => EstadoSlot::Vacio,
                _ => EstadoSlot::Disponible {
                    bici_id: format!("{}-{}", sede_id, i),
                },
            };
            Slot { slot_id: i, estado }.start()
        })
        .collect();

    slots
}

pub struct ConsultarBici;
impl Message for ConsultarBici {
    type Result = String;
}

impl Handler<ConsultarBici> for Slot {
    type Result = String;

    fn handle(&mut self, _msg: ConsultarBici, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Disponible { bici_id } => format!("Slot {}: (disponible)", self.slot_id),
            EstadoSlot::Reservado { bici_id } => format!("Slot {}: (reservado)", self.slot_id),
            EstadoSlot::Vacio => format!("Slot {}: sin bici", self.slot_id),
        }
    }
}
