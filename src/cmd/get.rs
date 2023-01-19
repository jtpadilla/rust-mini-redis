use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

/// Obtiene el valor de una clave.
/// 
/// Si la clave no existe un valor espacion 'nil' es retornado.
/// 
/// En caso de que al valor almacenado para la clave no sea una string
/// se retornara un error porque GET solo gestion valores de tipo string.
#[derive(Debug)]
pub struct Get {
    /// Nombre de la key de la que se obtiene el valor
    key: String,
}

impl Get {
    /// Crea el comando
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// Parsea una instancia de `Get` desde el frame que se ha recibido.
    /// 
    /// Como parametro para el parseado se recibe una instancia de 
    /// `Parse` con todos los argumentos que se han recibido y 
    /// que pueden ser consumidos.
    /// 
    /// # Formato del comando
    /// GET key
    /// 
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // El primer argumento 'GET' ya ha sido consumido.
        //
        // El parseador nos permite acceder a los argumentos pendientes
        // generando un error en caso de que no existan o de que no 
        // sean del tipo esperado.
        let key = parse.next_string()?;

        Ok(
            Get { 
                key 
            }
        )
    }

    /// Aplica el comando `Get` a la instancia de `Db` especificada.
    /// 
    /// La respuesta es escrita en ´dst´.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {

        // Obtiene el valor desde la base de base de datos
        let response = if let Some(value) = db.get(&self.key) {
            // Si hay una entrada para la clave
            Frame::Bulk(value)
        } else {
            // No hay una entrada para la clave
            Frame::Null
        };

        debug!(?response);

        // Se envia la respuesta al cliente
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Convierte este comando en su representacion en un Frame.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }

}
