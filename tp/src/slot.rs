use actix::{Actor, Addr, Context, Handler, Message, MessageResult};
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum EstadoSlotPersistido {
    Vacio,
    Reservado { bici_id: String },
    Disponible { bici_id: String },
}

impl From<&EstadoSlot> for EstadoSlotPersistido {
    fn from(estado: &EstadoSlot) -> Self {
        match estado {
            EstadoSlot::Vacio => EstadoSlotPersistido::Vacio,
            EstadoSlot::Reservado { bici_id } => EstadoSlotPersistido::Reservado { bici_id: bici_id.clone() },
            EstadoSlot::Disponible { bici_id } => EstadoSlotPersistido::Disponible { bici_id: bici_id.clone() },
        }
    }
}

impl From<EstadoSlotPersistido> for EstadoSlot {
    fn from(estado: EstadoSlotPersistido) -> Self {
        match estado {
            EstadoSlotPersistido::Vacio => EstadoSlot::Vacio,
            EstadoSlotPersistido::Reservado { bici_id } => EstadoSlot::Reservado { bici_id },
            EstadoSlotPersistido::Disponible { bici_id } => EstadoSlot::Disponible { bici_id },
        }
    }
}

pub struct ExportarEstado;

impl Message for ExportarEstado {
    type Result = EstadoSlotPersistido;
}

impl Handler<ExportarEstado> for Slot {
    type Result = MessageResult<ExportarEstado>;

    fn handle(&mut self, _msg: ExportarEstado, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(EstadoSlotPersistido::from(&self.estado))
    }
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
            EstadoSlot::Disponible { bici_id } => format!("Slot {:02}: Disponible (bici {})", self.slot_id, bici_id),
            EstadoSlot::Reservado { bici_id } => format!("Slot {:02}: Reservado (bici {})", self.slot_id, bici_id),
            EstadoSlot::Vacio => format!("Slot {:02}: Vacio", self.slot_id),
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

    #[test]
    fn estado_slot_persistido_round_trip_todas_las_variantes() {
        for estado in [
            EstadoSlotPersistido::Vacio,
            EstadoSlotPersistido::Reservado { bici_id: "1-1".to_string() },
            EstadoSlotPersistido::Disponible { bici_id: "1-2".to_string() },
        ] {
            let json = serde_json::to_string(&estado).unwrap();
            let de: EstadoSlotPersistido = serde_json::from_str(&json).unwrap();
            assert_eq!(de, estado);
        }
    }

    #[test]
    fn from_estado_slot_a_persistido_mapea_variantes_y_bici_id() {
        assert_eq!(EstadoSlotPersistido::from(&EstadoSlot::Vacio), EstadoSlotPersistido::Vacio);
        assert_eq!(
            EstadoSlotPersistido::from(&EstadoSlot::Reservado { bici_id: "1-1".to_string() }),
            EstadoSlotPersistido::Reservado { bici_id: "1-1".to_string() }
        );
        assert_eq!(
            EstadoSlotPersistido::from(&EstadoSlot::Disponible { bici_id: "1-2".to_string() }),
            EstadoSlotPersistido::Disponible { bici_id: "1-2".to_string() }
        );
    }

    #[test]
    fn from_persistido_a_estado_slot_mapea_variantes_y_bici_id() {
        match EstadoSlot::from(EstadoSlotPersistido::Vacio) {
            EstadoSlot::Vacio => {}
            _ => panic!("se esperaba Vacio"),
        }
        match EstadoSlot::from(EstadoSlotPersistido::Reservado { bici_id: "1-1".to_string() }) {
            EstadoSlot::Reservado { bici_id } => assert_eq!(bici_id, "1-1"),
            _ => panic!("se esperaba Reservado"),
        }
        match EstadoSlot::from(EstadoSlotPersistido::Disponible { bici_id: "1-2".to_string() }) {
            EstadoSlot::Disponible { bici_id } => assert_eq!(bici_id, "1-2"),
            _ => panic!("se esperaba Disponible"),
        }
    }

    #[actix_rt::test]
    async fn exportar_estado_devuelve_persistido_para_cada_variante() {
        let vacio = Slot { slot_id: 1, estado: EstadoSlot::Vacio }.start();
        let reservado = Slot { slot_id: 2, estado: EstadoSlot::Reservado { bici_id: "1-2".to_string() } }.start();
        let disponible = Slot { slot_id: 3, estado: EstadoSlot::Disponible { bici_id: "1-3".to_string() } }.start();

        assert_eq!(vacio.send(ExportarEstado).await.unwrap(), EstadoSlotPersistido::Vacio);
        assert_eq!(
            reservado.send(ExportarEstado).await.unwrap(),
            EstadoSlotPersistido::Reservado { bici_id: "1-2".to_string() }
        );
        assert_eq!(
            disponible.send(ExportarEstado).await.unwrap(),
            EstadoSlotPersistido::Disponible { bici_id: "1-3".to_string() }
        );
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

pub struct ActualizarSlots;

impl Message for ActualizarSlots {
    type Result = ();
}
pub struct SetSlotCounts {
    pub ocupados: u8,
    pub vacios: u8,
}

impl Message for SetSlotCounts {
    type Result = ();
}