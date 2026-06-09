use base64::Engine;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SECRET_LEN: usize = 32;
pub const NONCE_LEN: usize = 12;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct PayloadToken {
    pub bici_id: String,
    pub alquiler_id: String,
    pub cliente_id: String,
    pub sede_origen_id: u32,
    pub inicio_ts: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    Base64Invalido,
    BloqueDemasiadoCorto,
    AutenticacionFallida,
    PayloadInvalido,
    SerializacionFallida,
    CifradoFallido,
}

pub fn generar_token(payload: &PayloadToken, secret: &[u8; SECRET_LEN]) -> Result<String, TokenError> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(secret));
    let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);

    let plaintext = serde_json::to_vec(payload).map_err(|_| TokenError::SerializacionFallida)?;
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_ref())
        .map_err(|_| TokenError::CifradoFallido)?;

    let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ciphertext);

    Ok(base64::engine::general_purpose::STANDARD.encode(blob))
}

pub fn validar_token(token: &str, secret: &[u8; SECRET_LEN]) -> Result<PayloadToken, TokenError> {
    let blob = base64::engine::general_purpose::STANDARD
        .decode(token)
        .map_err(|_| TokenError::Base64Invalido)?;

    if blob.len() < NONCE_LEN {
        return Err(TokenError::BloqueDemasiadoCorto);
    }

    let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = ChaCha20Poly1305::new(Key::from_slice(secret));
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| TokenError::AutenticacionFallida)?;

    serde_json::from_slice(&plaintext).map_err(|_| TokenError::PayloadInvalido)
}

pub fn cargar_secret(raw: &str) -> [u8; SECRET_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload_de_prueba() -> PayloadToken {
        PayloadToken {
            bici_id: "1-1".to_string(),
            alquiler_id: "alquiler-123".to_string(),
            cliente_id: "cliente-9".to_string(),
            sede_origen_id: 1,
            inicio_ts: 1_700_000_000,
        }
    }

    fn secreto_de_prueba() -> [u8; SECRET_LEN] {
        cargar_secret("secreto-compartido-de-prueba")
    }

    #[test]
    fn generar_y_validar_token_recupera_el_payload_original() {
        let secreto = secreto_de_prueba();
        let payload = payload_de_prueba();

        let token = generar_token(&payload, &secreto).expect("generar_token no debe fallar con datos validos");
        let recuperado = validar_token(&token, &secreto).expect("token valido debe decodificar");

        assert_eq!(recuperado, payload);
    }

    #[test]
    fn token_con_ciphertext_alterado_es_rechazado() {
        let secreto = secreto_de_prueba();
        let payload = payload_de_prueba();

        let token = generar_token(&payload, &secreto).expect("generar_token no debe fallar con datos validos");
        let mut bytes = token.into_bytes();
        // Flip the last base64 character to corrupt the authenticated ciphertext.
        let ultimo = bytes.len() - 1;
        bytes[ultimo] = if bytes[ultimo] == b'A' { b'B' } else { b'A' };
        let token_alterado = String::from_utf8(bytes).expect("ascii valido");

        let resultado = validar_token(&token_alterado, &secreto);

        assert!(resultado.is_err());
    }

    #[test]
    fn token_validado_con_secreto_incorrecto_es_rechazado() {
        let secreto_generador = cargar_secret("secreto-A");
        let secreto_validador = cargar_secret("secreto-B");
        let payload = payload_de_prueba();

        let token = generar_token(&payload, &secreto_generador).expect("generar_token no debe fallar con datos validos");
        let resultado = validar_token(&token, &secreto_validador);

        assert_eq!(resultado, Err(TokenError::AutenticacionFallida));
    }

    #[test]
    fn token_con_base64_malformado_es_rechazado() {
        let secreto = secreto_de_prueba();

        let resultado = validar_token("esto no es base64 valido!!!", &secreto);

        assert_eq!(resultado, Err(TokenError::Base64Invalido));
    }

    #[test]
    fn cargar_secret_es_deterministico_para_la_misma_entrada() {
        let a = cargar_secret("mi-secreto-compartido");
        let b = cargar_secret("mi-secreto-compartido");

        assert_eq!(a, b);
    }
}
