use crate::cmd::{Parse, ParseError, Unknown};
use crate::{Command, Connection, Db, Frame, Shutdown};

use bytes::Bytes;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// Subscribe el cliente a uno o mas canales.
/// 
/// Una vez un client entra en estado subscrito ya no acepta el envio de
/// ningun otro comando, excepto comandos adicionales SUBSCRIBE, PSUBSCRIBE, 
/// UNSUBSCRIBE, PUNSUBSCRIBE, PING y QUIT.
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// Unsubscribes al client de uno o mas canales.
///
/// Cuando no se especifivan canales, el cliente elimina la subscripcion
/// de todos lso canales en los que se subscribio previamente.
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// Stream de mensajes.
/// El stream recibe los mensajes desde el `broadcast::Receiver`.
/// Utilizaremos `stream!` para crear un `Stream` que consume mensajes.
/// Como a los valores de `stream!` no se les puede asignar un nombre,
/// se le aplica un Box al stream mediante un "trail object".
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// Crea un nuevo comando `Subscribe` para escuchar por los comandos especificados.
    pub(crate) fn new(channels: &[String]) -> Subscribe {
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    /// 
    /// Parsea una instancia de `Set` desde el frame que se ha recibido.
    /// 
    /// Como parametro para el parseado se recibe una instancia de 
    /// `Parse` con todos los argumentos que se han recibido y 
    /// que pueden ser consumidos.
    /// 
    /// # Formato del comando
    /// SUBSCRIBE channel [channel ...]
    /// 
    /// # Retorno
    /// Retorna la string `SUBSCRIBE` o Err el el frame esta mal formado.
    /// 
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        // La string `SUBSCRIBE` ya ha sido consumido.
        //
        // Primero se extrae el nombre del primer canal.
        let mut channels = vec![parse.next_string()?];

        // Ahora, el resto de nombres de canal son consumidos.
        loop {
            match parse.next_string() {
                // Un nuevo nombre de canal, se incorpora a la lista de canales.
                Ok(s) => channels.push(s),

                // El error `EndOfStream` indica que no hay nada mas que parsear.
                Err(EndOfStream) => break,

                // Cualquier otro error implica que hay que cerrar la conexion
                Err(err) => return Err(err.into()),
            }
        }

        // Retornamos la instancia de `Subscribe`.
        Ok(
            Subscribe { 
                channels 
            }
        )
        
    }

    /// Se aplica el comando `Subscribe` a la `Db`.
    /// 
    /// Eata funcion es el punto de entrada que incluye la lista
    /// inicial de canales a los que subscribirse. Adicionalmente
    /// otros comandos `subscribe` y `unsubscribe` pueden recibirse 
    /// desde el ciente y en consecuencia la lista de subscripciones
    /// se actrualizara.
    /// 
    /// Este comando a diferencia de los otros comandos del servidor
    /// utilizara la conexion para procesar frames relacionados con
    /// la gestion de subscripciones que le llegaran por la conexion.
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // Cada canal individual de una subscripcion es gestionada
        // mediante un canal `sync::broadcast`. Los mensajes son repartidos 
        // a todos lso clientes que estan subscritos a los canales.
        //
        // Un cliente individual puede subscribirse a multiples canales 
        // y puede dinamicamente añadir y borrar subscripciones a su lista 
        // de subscripciones.
        //
        // Para gestionar todo esto se utiliza un `StreamMap` el cual 
        // permitira hacer un seguimiento de de las subscripciones activas.
        // El `StreamMap`mezcla los mensajes desde los canales individuales
        // de propagacion cuando son recibidos.
        let mut subscriptions = StreamMap::new();

        loop {
            // Los 'channels' con los que se ha construido la instancia de 'Subscribe'
            // son utilizados para las subscripciones iniciales.
            //
            // Cuando llegaran nuevos comandos de subscripciones estas se 
            // incorporaran a la lista de subscripciones en curso.
            //
            // Por tanto existe un vector en el que se mantienen la lista de 
            // subscripciones en curso para cada conexion.
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // La ejecucion del comando 'Subscribe' implica la ejecucion 
            // de un proceso asincrono que permite recibir altas/bajas de subscripciones
            // asi como enviar al cliente los datos recibidos por los canales
            // a los que se estan subscritos.
            // 
            // Esta terea podra:
            // - Recibir un mensaje desde un canal al que se esta subscrito.
            // - Recibir un comando subscribe/unsubscribe desd eel cliente
            // - Recibir una indicacion de shutdown desde el servidor.
            select! {

                // SELECT 1 - Recibe mensajes desde los canales a los que esta subscrito
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }

                // SELECT 2 - Recive frames desde la conexion que ha establecido el cliente
                res = dst.read_frame() => {

                    // Algo ha pasado en la conexion...
                    let frame = match res? {
                        Some(frame) => {
                            // ..  ha llegado un frame.
                            frame
                        },
                        None => {
                            // .. se ha cerrado la conexion.
                            return Ok(())
                        }
                    };

                    // Tenemos un frame, hay que extraer el comando y ejecutarlo
                    // aunque solo los soportados dentro del contexto de un
                    // subscribe.
                    handle_command(frame, &mut self.channels, &mut subscriptions, dst).await?;
                }

                // SELECT 3 - Peticion de parada del servidor
                _ = shutdown.recv() => {
                    // Se ha llegado una solicitud de finalizacion, salimos del bucle.
                    return Ok(());
                }

            };
        }
    }

    /// Convierte este comando en su representacion en un Frame.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // Se crea la subscripcion al canal.
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // If we lagged in consuming messages, just resume.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // Seguimiento de la suscripción en el conjunto de suscripciones de este cliente.
    subscriptions.insert(channel_name.clone(), rx);

    // Se le responde al cliente que la subscripcion ha sido satisfactoria.
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// Gestiona los comandos recibidos dentro del contexto que se crea en
/// la ejecucion de `subscribe`. Unicamente los comandos subscribe y
/// unsubscribe son permitidos.
/// 
/// Una nueva subscripcion es incorporada a `subscribe_to`en lugar de
/// modificar `subscriptions`.
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {

    // Se utiliza de nuevo `Command::from_frame` para determinar que comando se ha recibido.
    match Command::from_frame(frame)? {

        Command::Subscribe(subscribe) => {
            // Se realiza la subscripcion
            // la lista de subcripciones recibidas en el comando se carga 
            // en la lista de subscripciones de la instancia del Subscribe.
            // Yo creo que aqui hay un error porque ademas abria que incorporar
            // en el StreamMap la subscripcion....
            // (ahora no estoy preparado para verfiicar esto)
            subscribe_to.extend(subscribe.channels.into_iter());
        }

        Command::Unsubscribe(mut unsubscribe) => {

            // Si hemos llagado aqui es porque estando dentro del contexto de 
            // una subscripcion se ha recibidos un comando 'Unsubscribe'.
            // La llamada a 'Command::from_frame' loha instanciado y esta 
            // instancia contiene en el atributo 'channels' la lista de 
            // canales de los que hay que retirar la subscripcion.

            if unsubscribe.channels.is_empty() {
                // Si en el 'Unsubscribe' no hay ningun canal, entonces se
                // interpreta que hay que hacer el Unsubscribe de todos 
                // los canales a las que se esta ahora subscrito.
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }

        }

        command => {
            // El comando recibido no es soportado asi que se crea una instancia
            // del comando especial `Unknown' y se delega el tratamiento en el.
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }

    }
    Ok(())
}

/// Crea la respuesta al request subscribe.
///
/// All of these functions take the `channel_name` as a `String` instead of
/// a `&str` since `Bytes::from` can reuse the allocation in the `String`, and
/// taking a `&str` would require copying the data. This allows the caller to
/// decide whether to clone the channel name or not.
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// Crea la respuesta al request unsubscribe.
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// Crea un mensaje que informa al cliente sobre nuevos mensajes en un canal
/// al cual el cliente esta subscrito.
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    /// Crea una nueva instancia del comando `Unsubscribe` con 
    /// los canales que se han proporcionado.
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// Parsea una instancia de `Unsubscribe` desde el frame que se ha recibido.
    /// 
    /// # Formato del comando
    /// UNSUBSCRIBE [channel [channel ...]]
    /// 
    /// 
    /// Retorna el el valor de `Unsubscribe` o Err si la trama esta 
    /// mal formada.
    ///
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        // Se crea un array vacia para poder cargar los canales recibidos
        let mut channels = vec![];

        // Cada entrada en el frame debe ser una string o el fram estara mal formado.
        loop {
            match parse.next_string() {

                // Una string se ha consumidos desde el parse, se colocal en la 
                // lista de canales a los que hacer un unsubscribe.
                Ok(s) => channels.push(s),

                // El error `EndOfStream` indica que no hay mas datos para parsear
                Err(EndOfStream) => break,

                // El resto de errores son englobados y la conexion terminara.
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// Convierte el comando en el `Frame` equivalente.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }

}
