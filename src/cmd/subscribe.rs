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

                // Recibe mensajes desde los canales a los que esta subscrito
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }

                // Recive frames desde la conexion que ha establecido el cliente
                res = dst.read_frame() => {

                    // Si ha habido actividad en la conexion...
                    let frame = match res? {
                        Some(frame) => {
                            // ..  ha llegado un frame.
                            frame
                        },
                        // Esto ocurre cuando el cliente remoto ha cerrado la conexion
                        None => {
                            // .. se ha cerrado la conexion.
                            return Ok(())
                        }
                    };

                    // Tenemos un frame, hay que extraer el comando y ejecutarlo!
                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }

                // Peticion de parada del servidor
                _ = shutdown.recv() => {
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

    // Subscribe to the channel.
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

    // Track subscription in this client's subscription set.
    subscriptions.insert(channel_name.clone(), rx);

    // Respond with the successful subscription
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// Handle a command received while inside `Subscribe::apply`. Only subscribe
/// and unsubscribe commands are permitted in this context.
///
/// Any new subscriptions are appended to `subscribe_to` instead of modifying
/// `subscriptions`.
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {
    // A command has been received from the client.
    //
    // Only `SUBSCRIBE` and `UNSUBSCRIBE` commands are permitted
    // in this context.
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            // The `apply` method will subscribe to the channels we add to this
            // vector.
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // If no channels are specified, this requests unsubscribing from
            // **all** channels. To implement this, the `unsubscribe.channels`
            // vec is populated with the list of channels currently subscribed
            // to.
            if unsubscribe.channels.is_empty() {
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
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

/// Creates the response to a subcribe request.
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

/// Creates the response to an unsubcribe request.
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// Creates a message informing the client about a new message on a channel that
/// the client subscribes to.
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    /// Create a new `Unsubscribe` command with the given `channels`.
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// Parse a `Unsubscribe` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `UNSUBSCRIBE` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Unsubscribe` value is returned. If the frame is
    /// malformed, `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least one entry.
    ///
    /// ```text
    /// UNSUBSCRIBE [channel [channel ...]]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        // There may be no channels listed, so start with an empty vec.
        let mut channels = vec![];

        // Each entry in the frame must be a string or the frame is malformed.
        // Once all values in the frame have been consumed, the command is fully
        // parsed.
        loop {
            match parse.next_string() {
                // A string has been consumed from the `parse`, push it into the
                // list of channels to unsubscribe from.
                Ok(s) => channels.push(s),
                // The `EndOfStream` error indicates there is no further data to
                // parse.
                Err(EndOfStream) => break,
                // All other errors are bubbled up, resulting in the connection
                // being terminated.
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding an `Unsubscribe` command to
    /// send to the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
