mod get;
pub use get::Get;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

/// Enumeracion de los comandos REDIS soportados.
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    /// Parsea el comando desde el Frame recibido.
    /// El frame que se proporciona como parametro 
    /// debe ser una variante 'Frame::Array'
    ///
    /// # Retorno
    /// En caso de exito una variante del comando es retornada, 
    /// en caso contrario se retornara `crate::Error'.
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // El valor es decorado con un `Parse`. Parse proporciona 
        // una API tipo "cursor" que permite parsear los comandos mas facilmente.
        let mut parse = Parse::new(frame)?;

        // Una vez tenemos todos los frames del array accesibles a traves del
        // parseador, obtenemos el primer frame que debe ser una String
        // que contiene el nombre del comando.
        // Este nombre del comando se pasa a minusculas para buscar 
        // la coinicidencia con el comando.
        let command_name = parse.next_string()?.to_lowercase();

        // Una vez identificado el comando se deriva a cada comando el 
        // procesado del resto de parametros.
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                // No se ha reconicido elcomando asi que se retorna 
                // el comando `Unknown`.
                //
                // En este caso se utiliza return para evitar que la ejecucion 
                // siga su curso normal.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // En el curso normal (cuando se ha reconicido y parseado
        // el comando correctamente), se hace una verificacion final
        // llamando al metodo `finish()` del parseador para verificar
        // que no quiedan argumentos para consumir en el parseador.
        // Que queden argumentos por consumir indica una trama irregular.
        parse.finish()?;

        // El comando ha sido parseado satisfactoriamente
        Ok(command)
    }

    /// Aplica el comando a la instancia `Db`proporcionada.
    /// 
    /// Cada comando escribe la respuesta en la conexion
    /// proporcionada `dst`.
    /// 
    /// Para la aplicacion de los comandos sobre las base de datos y su
    /// posterior respuesta se invocan especificamente a un metodo segun 
    /// el comando (tienen distinta firma).
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            // El comando 'Unsubscribe' no opera sobre la base de datos.
            // Solo puede recibir comandos dentro del contexto del 
            // comando `Subscribe`.
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }

    /// Obtiene el nombre del comando
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Publish(_) => "pub",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Ping(_) => "ping",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
