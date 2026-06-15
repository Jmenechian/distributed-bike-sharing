use matenmutex::common::calcular_distancia;
use matenmutex::messages::cliente_sede::{AckComprobanteAlquiler, DevolverBici, MsgClienteASede, PedirSedes, PreguntarBicis, SolicitarBici};
use matenmutex::messages::sede_cliente::{RespuestaDevolverBici, RespuestaSolicitarBici};
use matenmutex::messages::sede_sede::SedeInfo;
use matenmutex::types::BiciAlquilada;
use std::io;
use tokio::io::{BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use super::red_cliente::enviar_mensaje_a_sede;
use super::Cliente;

impl Cliente {
    fn print_sedes(sedes: &[SedeInfo], coords: (f64, f64)) {
        let mut ordenadas: Vec<&SedeInfo> = sedes.iter().collect();
        ordenadas.sort_by(|a, b| {
            calcular_distancia(a.coords, coords)
                .total_cmp(&calcular_distancia(b.coords, coords))
        });
        for sede in ordenadas {
            let dist = calcular_distancia(sede.coords, coords);
            let estado = if sede.conectada { "online" } else { "offline" };
            println!(
                "Sede {} | coords: ({:.4}, {:.4}) | estado: {} | distancia: {:.2}m | slots disponibles: {} | slots vacios {}",
                sede.sede_id, sede.coords.0, sede.coords.1, estado, dist, sede.slots_disponibles, sede.slots_vacios
            )
        }
    }

    pub(crate) async fn pedir_sedes(&mut self) -> Result<(), io::Error> {
        if !self.esta_en_rango() && !self.estado {
            return Err(io::Error::new(io::ErrorKind::Other, "[error] estoy offline y fuera de rango"));
        }
        let coords = self.coords;
        let (writer, reader) = self.validar_conexion()?;
        let respuesta = Self::enviar_info_sedes_cercanas(coords, writer, reader).await?;

        if let Some(json) = respuesta {
            match serde_json::from_str::<Vec<SedeInfo>>(&json) {
                Ok(sedes) => {
                    if sedes.is_empty() {
                        println!("[cliente] La sede esta offline y fuera de rango");
                        Cliente::print_sedes(&self.sedes_conocidas, coords);
                        return Ok(());
                    }
                    self.sedes_conocidas = sedes;
                }
                Err(e) => {
                    eprintln!("[error] fallo al deserializar sedes: {} | json: {}", e, json);
                }
            }
        } else {
            eprintln!("[error] respuesta vacia, no se recibieron sedes");
        }

        Cliente::print_sedes(&self.sedes_conocidas, coords);
        self.persistir_estado();
        Ok(())
    }

    pub(crate) async fn solicitar_bici(&mut self) -> Result<(), io::Error> {
        if !self.esta_en_rango() && !self.estado {
            return Err(io::Error::new(io::ErrorKind::Other, "[error] estoy offline y fuera de rango"));
        }

        if self.bici_activa.is_some() {
            println!("[cliente] Ya tenes una bici");
            return Ok(());
        }

        let cliente_id = self.cliente_id;
        let coords = self.coords;
        let dinero = self.dinero;
        let (writer, reader) = self.validar_conexion()?;

        let respuesta = Self::enviar_solicitar_bici(cliente_id, coords, dinero, writer, reader).await?;
        let (bici,dinero) = Self::descifrar_respuesta_de_solicitar_bici(respuesta)?; //aca ya esta bien el dinero, si confirma vamos

        let respuesta = self.enviar_ack_comprobante(bici).await?;
        self.descifrar_respuesta_de_ack_comprobante(respuesta, dinero);
        self.persistir_estado();

        Ok(())
    }

    async fn enviar_solicitar_bici(cliente_id: u32, coords: (f64, f64), dinero: f64, writer: &mut OwnedWriteHalf, reader: &mut Lines<BufReader<OwnedReadHalf>>) -> Result<Option<String>, io::Error> {
        let msg = MsgClienteASede::SolicitarBici(SolicitarBici { cliente_id, coords, dinero});
        let respuesta = enviar_mensaje_a_sede(writer, reader, msg).await?;
        Ok(respuesta)
    }

    async fn enviar_info_sedes_cercanas(coords: (f64, f64), writer: &mut OwnedWriteHalf, reader: &mut Lines<BufReader<OwnedReadHalf>>) -> Result<Option<String>, io::Error> {
        let msg = MsgClienteASede::PedirSedes(PedirSedes {coords});
        let respuesta = enviar_mensaje_a_sede(writer, reader, msg).await?;
        Ok(respuesta)
    }

    async fn enviar_ack_comprobante(&mut self, bici: BiciAlquilada) -> Result<Option<String>, io::Error> {
        let alquiler_id = bici.alquiler_id().to_string();
        let (writer, reader) = self.validar_conexion()?;
        let ack = MsgClienteASede::AckComprobanteAlquiler(AckComprobanteAlquiler { alquiler_id: alquiler_id.clone() });
        let respuesta = enviar_mensaje_a_sede(writer, reader, ack).await?;
        Ok(respuesta)
    }

    fn descifrar_respuesta_de_ack_comprobante(&mut self, respuesta: Option<String>, dinero: f64) {
        if let Some(respuesta) = respuesta {
            match serde_json::from_str::<RespuestaSolicitarBici>(&respuesta) {
                Ok(RespuestaSolicitarBici::AlquilerConfirmado(bici_confirmada)) => {
                    println!("Alquiler confirmado: bici {}", bici_confirmada.bici_id());
                    println!("Dinero actual: ${:.2} -> ${:.2}", self.dinero, dinero);
                    self.dinero = dinero; //recien aca que es el ultimo control
                    self.bici_activa = Some(bici_confirmada);
                }
                Ok(RespuestaSolicitarBici::AlquilerRechazado(motivo)) => {
                    println!("Alquiler rechazado: {}", motivo);
                }
                _ => println!("Sede: {}", respuesta),
            }
        }
    }

    fn descifrar_respuesta_de_solicitar_bici(respuesta: Option<String>) -> Result<(BiciAlquilada, f64), io::Error> {
        match respuesta.as_deref().and_then(|l| serde_json::from_str::<RespuestaSolicitarBici>(l).ok()) {
            Some(RespuestaSolicitarBici::ComprobanteAlquilerEmitido(bici, dinero)) => {
                Ok((bici,dinero))
            }
            Some(RespuestaSolicitarBici::AlquilerRechazado(motivo)) => {
                Err(io::Error::new(io::ErrorKind::Other, format!("Alquiler rechazado: {motivo}")))
            }
            _ => { Err(io::Error::new(io::ErrorKind::Other, "Respuesta inesperada de la sede")) }
        }
    }

    pub(crate) async fn devolver_bici(&mut self) -> Result<(), io::Error> {
        if !self.esta_en_rango() && !self.estado {
            return Err(io::Error::new(io::ErrorKind::Other, "[error] estoy offline y fuera de rango"));
        }

        if self.bici_activa.is_none() {
            return Err(io::Error::new(io::ErrorKind::Other, "No hay bici activa para devolver"));
        }

        let bici = self.bici_activa.clone().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "No hay bici activa para devolver")
        })?;
        let coords = self.coords;
        let dinero = self.dinero;
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::DevolverBici(DevolverBici { bici, coords, dinero});
        let linea = enviar_mensaje_a_sede(writer, reader, msg).await?;

        if let Some(linea) = linea {
            match serde_json::from_str::<RespuestaDevolverBici>(&linea) {
                Ok(respuesta) => self.manejar_respuesta_devolver(respuesta),
                Err(_) => println!("Sede: {}", linea),
            }
        }

        self.persistir_estado();
        Ok(())
    }

    pub(crate) async fn info_bicis(&mut self) -> Result<(), io::Error> {
        let (writer, reader) = self.validar_conexion()?;
        let msg = MsgClienteASede::PreguntarBicis(PreguntarBicis {});
        if let Some(linea) = enviar_mensaje_a_sede(writer, reader, msg).await? {
            match serde_json::from_str::<String>(&linea) {
                Ok(texto) => println!("{}", texto),
                Err(_) => println!("{}", linea),
            }
        }
        Ok(())
    }

    fn manejar_respuesta_devolver(&mut self, respuesta: RespuestaDevolverBici) {
        match respuesta {
            RespuestaDevolverBici::DevolucionOk(bici, dinero) => {
                println!("Devolucion exitosa: bici {}", bici.bici_id);
                println!("Dinero actual: ${:.2} -> ${:.2}", self.dinero, dinero);

                self.dinero = dinero;
                self.bici_activa = None;
            }
            RespuestaDevolverBici::DevolucionRechazada(motivo) => {
                println!("Devolucion rechazada: {}", motivo);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matenmutex::types::Bici;

    fn cliente_de_prueba() -> Cliente {
        Cliente::new(1, (-34.6037, -58.3816), 9999999.0)
    }

    fn bici_alquilada_de_prueba(bici_id: &str) -> BiciAlquilada {
        BiciAlquilada::new(
            bici_id.to_string(),
            "alq-1".to_string(),
            "1".to_string(),
            1,
            1000,
            "tok-1".to_string(),
        )
    }

    #[test]
    fn alquiler_confirmado_actualiza_bici_activa() {
        let mut cliente = cliente_de_prueba();
        cliente.bici_activa = Some(bici_alquilada_de_prueba("1-1"));
        assert_eq!(cliente.bici_activa.as_ref().map(|b| b.bici_id()), Some("1-1"));
    }

    #[test]
    fn alquiler_rechazado_no_modifica_bici_activa() {
        let cliente = cliente_de_prueba();
        assert!(cliente.bici_activa.is_none());
    }

    #[test]
    fn devolucion_ok_vacia_bici_activa() {
        let mut cliente = cliente_de_prueba();
        cliente.bici_activa = Some(bici_alquilada_de_prueba("1-1"));

        cliente.manejar_respuesta_devolver(RespuestaDevolverBici::DevolucionOk(Bici {
            bici_id: "1-1".to_string(),
        }, 0.0));

        assert!(cliente.bici_activa.is_none());
    }

    #[test]
    fn devolucion_rechazada_no_modifica_bici_activa() {
        let mut cliente = cliente_de_prueba();
        cliente.bici_activa = Some(bici_alquilada_de_prueba("1-1"));

        cliente.manejar_respuesta_devolver(RespuestaDevolverBici::DevolucionRechazada(
            "no hay lugar".to_string(),
        ));

        assert_eq!(
            cliente.bici_activa.as_ref().map(|b| b.bici_id()),
            Some("1-1")
        );
    }
}
