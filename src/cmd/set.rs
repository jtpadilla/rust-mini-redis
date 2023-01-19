use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

/// Asigna el valor de una clave
/// 
/// Si ya existe un valor con esta clave el valor anterior sera sobreescrito
#[derive(Debug)]
pub struct Set {
    /// clave para acceder al valor
    key: String,

    /// Valor almacenado
    value: Bytes,

    /// Cuando expira el valor
    expire: Option<Duration>,
}

impl Set {
    /// Crea el comando
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// Parsea una instancia de `Set` desde el frame que se ha recibido.
    /// 
    /// Como parametro para el parseado se recibe una instancia de 
    /// `Parse` con todos los argumentos que se han recibido y 
    /// que pueden ser consumidos.
    /// 
    /// # Formato del comando
    /// SET key value [EX seconds|PX milliseconds]
    /// 
    /// # Retorno
    /// Retorna el valor asociado a la clave o Err si el frame esta mal 
    /// formado.
    ///
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // Se lee la clave (este campo es requerido)
        let key = parse.next_string()?;

        // Se lee el valor (este campo es requerido)
        let value = parse.next_bytes()?;

        // La expiracion es opcional (si no hay nada mas entonces se asigna None)
        let expire: Option<Duration>  =  match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // La expiracion esta especificada en segundos
                // El siguiente valor es un numero entero
                let secs = parse.next_int()?;
                Some(Duration::from_secs(secs))
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // La expiracion esta especificada en milisegundos
                // El siguiente valor es un numero entero
                let ms = parse.next_int()?;
                Some(Duration::from_millis(ms))
            }
            Ok(_) => {
                // No se soportan otras opciones
                return Err("currently `SET` only supports the expiration option".into())
            },
            Err(EndOfStream) => {
                // No hay nada que leer (no hay opciones)
                Option::None
            }
            Err(err) => {
                // Cualquier error pasa por aqui..
                // All other errors are bubbled up, resulting in the connection
                // being terminated.
                return Err(err.into())
            },
        };

        Ok(Set { key, value, expire })
    }

    /// Aplica el comando `Get` a la instancia de `Db` especificada.
    /// 
    /// La respuesta es escrita en ´dst´.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {

        // asigna a una clave y valor y opcionalmente una caducidad.
        db.set(self.key, self.value, self.expire);

        // Prepara la respuesta satisfactoria.
        let response = Frame::Simple("OK".to_string());

        debug!(?response);

        // Se envia la respuesta al cliente
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Convierte este comando en su representacion en un Frame.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(ms) = self.expire {
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(ms.as_millis() as u64);
        }
        frame
    }

}
