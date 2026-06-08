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
            EstadoSlot::Disponible { bici_id } => format!("Slot {}: disponible", self.slot_id),
            EstadoSlot::Reservado { bici_id } => format!("Slot {}: reservado", self.slot_id),
            EstadoSlot::Vacio => format!("Slot {}: Vacio", self.slot_id),
        }
    }
}

pub struct ReservarBici;

impl Message for ReservarBici {
    type Result = Result<(), String>;
}

impl Handler<ReservarBici> for Slot {
    type Result = Result<(), String>;
    fn handle(&mut self, _msg: ReservarBici, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Disponible { bici_id } => {
                let bici_id = bici_id.clone();
                self.estado = EstadoSlot::Reservado { bici_id };
                Ok(())
            }
            _ => Err("Slot no disponible".to_string()), //tal vez no hace falta
        }
    }
}

pub struct ReservarSlot {
    pub bici_id: String,
}

impl Message for ReservarSlot {
    type Result = Result<(), String>;
}

impl Handler<ReservarSlot> for Slot {
    type Result = Result<(), String>;
    fn handle(&mut self, msg: ReservarSlot, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Vacio {} => {
                let bici_id = msg.bici_id;
                self.estado = EstadoSlot::Reservado { bici_id };
                Ok(())
            }
            _ => Err("Slot no disponible".to_string()), //tal vez no hace falta
        }
    }
}


pub struct TomarBici;

impl Message for TomarBici {
    type Result = Result<String, u32>;
}

impl Handler<TomarBici> for Slot {
    type Result = Result<String, u32>;

    fn handle(&mut self, _msg: TomarBici, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Reservado { bici_id } => {
                let bici_id = bici_id.clone();
                self.estado = EstadoSlot::Vacio;
                Ok(bici_id.clone())
            }
            _ => Err(0),
        }
    }
}

pub struct DejarBici;

impl Message for DejarBici {
    type Result = Result<(), u32>;
}

impl Handler<DejarBici> for Slot {
    type Result = Result<(), u32>;

    fn handle(&mut self, _msg: DejarBici, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Reservado { bici_id } => {
                let bici_id = bici_id.clone();
                self.estado = EstadoSlot::Disponible { bici_id };
                Ok(())
            }
            _ => Err(0),
        }
    }
}