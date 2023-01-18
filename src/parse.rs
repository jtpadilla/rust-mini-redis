use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// Utilidad para parsear un comando.
/// 
/// Los comandos son representados por un array de Frames donde cada 
/// entrada en el array es un "token". Una instancia de `Parse` es 
/// inicializada con un array de frames y proporciona una API del estilo 
/// de un cursor.
/// 
/// Cada instancia de un comando tiene un metodo `parse_frame` que utiliza
/// `Parse` para extraer sus campos.
#[derive(Debug)]
pub(crate) struct Parse {
    /// Iterador para al recorrer un Frame::Array.
    parts: vec::IntoIter<Frame>,
}

/// Error encontrado mientras se parsea un frame.
///
/// Unicamente en error `EndOfStream` es gestionado en runtime. Todos los
/// otros errores terminan con el cierre de la conexion.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// El intentoi de extraer un frame a fallado porque se han consumido todos los frames.
    EndOfStream,

    /// Todos los otros errores
    Other(crate::Error),
}

impl Parse {
    /// Crea un nuevo `Parse` para parsear el contenido de un `frame`.
    /// 
    /// Retorna un `Err` si el frame no es un 'Frame::Array'.
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => {
                // El parametro es un `Frame::Array`, todo Ok.
                array
            },
            frame => {
                // No es un `Frame::Array`, no se puede continuar!
                return Err(format!("protocol error; expected array, got {:?}", frame).into());
            },
        };

        // La expresion da como resultado una instanca de `Parse` que contiene el 
        // iterador al array de `Frame`.
        Ok(
            Parse {
                parts: array.into_iter(),
            }
        )
    }

    /// Retorna la siguiente entrada del iterador o un error si no quedan mas.
    /// 
    /// Este metodo es privado porque sera utilizado por los metodos especificos 
    /// que seran invocados para obtener los distintos tipos de frames.
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// Retorna la siguiente entrada como una string
    /// 
    /// Si la siguiente entrada no puede ser representada como una string entonces
    /// un error sera retornado.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            // Ambos `Simple` and `Bulk` pueden ser representados por una String.
            Frame::Simple(s) => {
                // La string de `Frame::Simple` se puede utilizar directamente.
                Ok(s)
            },
            Frame::Bulk(data) => {
                // `Frame::Bulk` contiene un `bytes::Bytes` con el que se realizan 
                // las siguientes tranformaciones:
                //   - `&data[..]` genera un `&[u8]` (slice de bytes)
                //   - `std::str::from_utf8()` genera un `&str`
                str::from_utf8(&data[..])
                    // Este `.map()' retOrnara un 'Result<String, _>' que es lo que
                    // se espera en caso de que todo haya indo bien.
                    .map(|s| s.to_string())
                    // Sin embargo, si se ha producido un error entrara el 
                    // `.map_err(...)` el cual da como resultado un 'Result<_,  String>'.
                    .map_err(|_| "protocol error; invalid string".into())
                    // Pero expresion resultante que es el resultado del `match` y 
                    // que a su vez se convierte en el resultado de la funcion 
                    // `fn next_string()`.
                    // El resultado es un `Result<String, String>` pero el resultado
                    // que se espera es un `Result<String, ParseError>`.
                    // Esto esta resuelto ya que el compilador encontrara mas abajo 
                    // que `ParseError` implementa el trait `impl From<String> for ParseError`
                    // y mediante esta conversion el compilador hara la adaptacion 
                    // correspondiente.

            },
            frame => {
                // Commo tenemos la impleentacion del trait `From<String> for ParseError`
                // automaticamente podemos invocar `stringInstance.into()` si gracias a 
                // la inferencia de tipos sabemos que el destinatario es un `ParseError`.
                // Como resultado 'StringInstance.into()' se convertira en 
                // 'ParseError::from(stringInstance)'.
                let string = format!(
                    "protocol error; expected simple frame or bulk frame, got {:?}", 
                    frame
                );
                let err = string.into();
                Err(err)
            },
        }
    }

    /// Return the next entry as raw bytes.
    ///
    /// If the next entry cannot be represented as raw bytes, an error is
    /// returned.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            // Both `Simple` and `Bulk` representation may be raw bytes.
            //
            // Although errors are stored as strings and could be represented as
            // raw bytes, they are considered separate types.
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    /// Return the next entry as an integer.
    ///
    /// This includes `Simple`, `Bulk`, and `Integer` frame types. `Simple` and
    /// `Bulk` frame types are parsed.
    ///
    /// If the next entry cannot be represented as an integer, then an error is
    /// returned.
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            // An integer frame type is already stored as an integer.
            Frame::Integer(v) => Ok(v),
            // Simple and bulk frames must be parsed as integers. If the parsing
            // fails, an error is returned.
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected int frame but got {:?}", frame).into()),
        }
    }

    /// Ensure there are no more entries in the array
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
