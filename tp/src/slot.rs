use actix::{Actor, Addr, Context, Handler, Message, MessageResult};

#[derive(Debug, PartialEq, Eq)]
pub enum ResultadoReserva {
    Reservada(String),
    Rechazada,
}

pub struct Reservar;
impl Message for Reservar {
    type Result = ResultadoReserva;
}

pub struct Confirmar;
impl Message for Confirmar {
    type Result = bool;
}

pub struct Cancelar;
impl Message for Cancelar {
    type Result = bool;
}

pub struct AsegurarBici {
    pub bici_id: String,
}
impl Message for AsegurarBici {
    type Result = bool;
}

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

pub fn crear_slots_con_estados(estados: Vec<EstadoSlot>) -> Vec<Addr<Slot>> {
    estados
        .into_iter()
        .enumerate()
        .map(|(idx, estado)| {
            Slot {
                slot_id: idx as u32 + 1,
                estado,
            }
            .start()
        })
        .collect()
}

pub fn crear_slots(sede_id: u32, num_slots: u32) -> Vec<Addr<Slot>> {
    let estados: Vec<EstadoSlot> = (1..=num_slots)
        .map(|i| match rand::random::<u8>() % 2 {
            0 => EstadoSlot::Vacio,
            _ => EstadoSlot::Disponible {
                bici_id: format!("{}-{}", sede_id, i),
            },
        })
        .collect();

    crear_slots_con_estados(estados)
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

impl Handler<Reservar> for Slot {
    type Result = MessageResult<Reservar>;

    fn handle(&mut self, _msg: Reservar, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Disponible { bici_id } => {
                let bici_id = bici_id.clone();
                self.estado = EstadoSlot::Reservado {
                    bici_id: bici_id.clone(),
                };
                MessageResult(ResultadoReserva::Reservada(bici_id))
            }
            _ => MessageResult(ResultadoReserva::Rechazada),
        }
    }
}

impl Handler<Confirmar> for Slot {
    type Result = bool;

    fn handle(&mut self, _msg: Confirmar, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Reservado { .. } => {
                self.estado = EstadoSlot::Vacio;
                true
            }
            _ => false,
        }
    }
}

impl Handler<Cancelar> for Slot {
    type Result = bool;

    fn handle(&mut self, _msg: Cancelar, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Reservado { bici_id } => {
                let bici_id = bici_id.clone();
                self.estado = EstadoSlot::Disponible { bici_id };
                true
            }
            _ => false,
        }
    }
}

impl Handler<AsegurarBici> for Slot {
    type Result = bool;

    fn handle(&mut self, msg: AsegurarBici, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Vacio => {
                self.estado = EstadoSlot::Disponible {
                    bici_id: msg.bici_id,
                };
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
pub struct ConsultarEstado;

#[cfg(test)]
impl Message for ConsultarEstado {
    type Result = String;
}

#[cfg(test)]
impl Handler<ConsultarEstado> for Slot {
    type Result = String;

    fn handle(&mut self, _msg: ConsultarEstado, _ctx: &mut Context<Self>) -> Self::Result {
        match &self.estado {
            EstadoSlot::Disponible { bici_id } => format!("disponible:{}", bici_id),
            EstadoSlot::Reservado { bici_id } => format!("reservado:{}", bici_id),
            EstadoSlot::Vacio => "vacio".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn estado_de(addr: &Addr<Slot>) -> impl std::future::Future<Output = String> + use<'_> {
        async move { addr.send(ConsultarEstado).await.unwrap() }
    }

    #[actix_rt::test]
    async fn reservar_sobre_disponible_transiciona_a_reservado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot.send(Reservar).await.unwrap();

        assert_eq!(resultado, ResultadoReserva::Reservada("1-1".to_string()));
        assert_eq!(estado_de(&slot).await, "reservado:1-1");
    }

    #[actix_rt::test]
    async fn reservar_sobre_vacio_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Vacio,
        }
        .start();

        let resultado = slot.send(Reservar).await.unwrap();

        assert_eq!(resultado, ResultadoReserva::Rechazada);
        assert_eq!(estado_de(&slot).await, "vacio");
    }

    #[actix_rt::test]
    async fn reservar_sobre_reservado_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot.send(Reservar).await.unwrap();

        assert_eq!(resultado, ResultadoReserva::Rechazada);
        assert_eq!(estado_de(&slot).await, "reservado:1-1");
    }

    #[actix_rt::test]
    async fn confirmar_sobre_reservado_transiciona_a_vacio() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot.send(Confirmar).await.unwrap();

        assert!(resultado);
        assert_eq!(estado_de(&slot).await, "vacio");
    }

    #[actix_rt::test]
    async fn confirmar_sobre_disponible_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot.send(Confirmar).await.unwrap();

        assert!(!resultado);
        assert_eq!(estado_de(&slot).await, "disponible:1-1");
    }

    #[actix_rt::test]
    async fn confirmar_sobre_vacio_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Vacio,
        }
        .start();

        let resultado = slot.send(Confirmar).await.unwrap();

        assert!(!resultado);
        assert_eq!(estado_de(&slot).await, "vacio");
    }

    #[actix_rt::test]
    async fn cancelar_sobre_reservado_restaura_disponible_con_bici_id() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot.send(Cancelar).await.unwrap();

        assert!(resultado);
        assert_eq!(estado_de(&slot).await, "disponible:1-1");
    }

    #[actix_rt::test]
    async fn cancelar_sobre_disponible_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot.send(Cancelar).await.unwrap();

        assert!(!resultado);
        assert_eq!(estado_de(&slot).await, "disponible:1-1");
    }

    #[actix_rt::test]
    async fn cancelar_sobre_vacio_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Vacio,
        }
        .start();

        let resultado = slot.send(Cancelar).await.unwrap();

        assert!(!resultado);
        assert_eq!(estado_de(&slot).await, "vacio");
    }

    #[actix_rt::test]
    async fn asegurar_bici_sobre_vacio_transiciona_a_disponible() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Vacio,
        }
        .start();

        let resultado = slot
            .send(AsegurarBici {
                bici_id: "1-3".to_string(),
            })
            .await
            .unwrap();

        assert!(resultado);
        assert_eq!(estado_de(&slot).await, "disponible:1-3");
    }

    #[actix_rt::test]
    async fn asegurar_bici_sobre_disponible_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Disponible {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot
            .send(AsegurarBici {
                bici_id: "1-3".to_string(),
            })
            .await
            .unwrap();

        assert!(!resultado);
        assert_eq!(estado_de(&slot).await, "disponible:1-1");
    }

    #[actix_rt::test]
    async fn asegurar_bici_sobre_reservado_se_rechaza_sin_mutar_estado() {
        let slot = Slot {
            slot_id: 1,
            estado: EstadoSlot::Reservado {
                bici_id: "1-1".to_string(),
            },
        }
        .start();

        let resultado = slot
            .send(AsegurarBici {
                bici_id: "1-3".to_string(),
            })
            .await
            .unwrap();

        assert!(!resultado);
        assert_eq!(estado_de(&slot).await, "reservado:1-1");
    }

    #[actix_rt::test]
    async fn crear_slots_con_estados_respeta_los_estados_dados() {
        let estados = vec![
            EstadoSlot::Disponible {
                bici_id: "9-1".to_string(),
            },
            EstadoSlot::Vacio,
            EstadoSlot::Reservado {
                bici_id: "9-3".to_string(),
            },
        ];

        let slots = crear_slots_con_estados(estados);

        assert_eq!(slots.len(), 3);
        assert_eq!(estado_de(&slots[0]).await, "disponible:9-1");
        assert_eq!(estado_de(&slots[1]).await, "vacio");
        assert_eq!(estado_de(&slots[2]).await, "reservado:9-3");
    }
}
