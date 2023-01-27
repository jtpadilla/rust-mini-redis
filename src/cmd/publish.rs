use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

/// Publica un mensaje en un canal.
/// 
/// Envia un mensaje sin esperar el reconocimiento de ningun consumidor.
/// Los consumidores pueden subscribirse a los canales para recibir 
/// los mensajes.
/// 
/// Los nombres de los canales no tienen relacion con el espacion de nombres
/// del las claves/valores.
/// 
/// Es decir, puede haver un valor con la clave 'foo' y tambien un canal con
/// el mismo nombre y no hay ninguna interfarencia entre ellos.
#[derive(Debug)]
pub struct Publish {
    //7 Nombre del canal donde el mensaje sera publicado.
    channel: String,

    /// El mensaje que sera publicado
    message: Bytes,
}

impl Publish {
    /// Crea un nuevo comando `Publish'
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// Parsea una instancia de `Publish` desde el frame que se ha recibido.
    /// 
    /// Como parametro para el parseado se recibe una instancia de 
    /// `Parse` con todos los argumentos que se han recibido y 
    /// que pueden ser consumidos.
    /// 
    /// # Formato del comando
    /// PUBLISH channel message
    /// 
    /// Retorna el mensaje que se ha publicado o Err si la trama esta 
    /// mal formada.
    ///
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        // El primer argumento 'PUBLISH' ya ha sido consumido.
        let channel = parse.next_string()?;
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    /// Aplica el comando `Get` a la instancia de `Db` especificada.
    /// 
    /// La respuesta es escrita en ´dst´.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Se le envia el mensaje a todos los subscriptores
        let num_subscribers = db.publish(&self.channel, self.message);

        // El numero de subscriptores es retornado como respuesta.
        //
        // Recordar que el numero de subscriptores no significan que todos los 
        // clientes de los subscriptores hayan recibido la respuesta (
        // pueden perer la conexion antes de recibirla)
        let response = Frame::Integer(num_subscribers as u64);

        // Escribe la respuesta hacia el cliente
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Convierte este comando en su representacion en un Frame.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }

}
