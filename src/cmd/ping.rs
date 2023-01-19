use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::instrument;

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<String>,
}

impl Ping {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<String>) -> Ping {
        Ping { msg }
    }

    /// Parsea una instancia de `Ping` desde el frame que se ha recibido.
    /// 
    /// Como parametro para el parseado se recibe una instancia de 
    /// `Parse` con todos los argumentos que se han recibido y 
    /// que pueden ser consumidos.
    /// 
    /// # Formato del comando
    /// PING [message]
    /// 
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        // El primer argumento 'PING' ya ha sido consumido.
        //
        // El parseador nos permite acceder a los argumentos pendientes
        // generando un error en caso de que no existan o de que no 
        // sean del tipo esperado.
        match parse.next_string() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Aplica el comando `Ping` (en este caso no utiliza la basde de datos).
    /// 
    /// La respuesta es escrita en ´dst´.
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(Bytes::from(msg)),
        };

        // Se envia la respuesta al cliente
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Convierte este comando en su representacion en un Frame.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(Bytes::from(msg));
        }
        frame
    }
    
}
