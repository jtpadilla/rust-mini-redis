//! Proporciona una representacion de tipos de las tramas del protocolo Redis.
//! asi como utilidades para el parseado de estos frames desde un array de bytes.

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// Un frame en el protocolo Redis
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// No hay suficientes datos para parsear un mensaje
    Incomplete,

    /// Codificacion invalida del mensaje
    Other(crate::Error),
}

impl Frame {
    /// Retorna `Frame` con la variante `Array` con un `vector<Frame>` vacio.
    /// La unica forma de crear un 'Frame' es creando una variante de tipo 'Array`
    /// la cual contiene un 'Vector<Frame>` vacio. El resto de metodos nos
    /// permitiran incorporar al vector nuevas instancios de 'Frame::Bulk' 
    /// y 'Frame::Integer(bytes: Bytes)'.
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Incorpora una "bulk" en el array ('self` debe ser un frame de tipo 'Array').
    ///
    /// # Panics
    /// Se emitira un panic si `self` no es un array.
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Incorpora un "integer" en el array ('self` debe ser un frame de tipo 'Array').
    ///
    /// # Panics
    /// Se emitira un panic si `self` no es un array.
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Verifica si el mensaje puede ser decodificado
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Saltamos -> '-1\r\n'
                    skip(src, 4)
                } else {
                    // Leemos la longitud del "bulk string"
                    let len: usize = get_decimal(src)?.try_into()?;

                    // saltamos la longitud del "bulk string" + 2 (\r\n).
                    skip(src, len + 2)
                }
            }
            b'*' => {
                // Leemos la longitud del array
                let len = get_decimal(src)?;

                // Mediante recursividad verificamos cada uno de los elementos del array
                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => {
                // Tipo de frame no soportado
                Err(format!("protocol error; invalid frame type byte `{}`", actual).into())
            }, 
        }
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // Read the bulk string
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    /// Converts the frame to an "unexpected frame" error
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " ")?;
                        part.fmt(fmt)?;
                    }
                }

                Ok(())
            }
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    // Cursor implementa bytes::buf::Buf como "Implementations on Foreign Types"
    // Es decir, la implementacion esta en el fichero con el codigo del Trait Buf
    // no en el fichero con la implementacion de Cursor.
    if !src.has_remaining() {
        // Si no hay mas bytes para consumir se retorna un error.
        return Err(Error::Incomplete);
    }
    // Inicialmente se obtiene un slice de los bytes entra la actual posicion y el final 
    // del buffer.
    // Finalmente se retorna el byte que hay en la posicion 0 del slice
    // La posicion NO AVANZA!
    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    // Cursor implementa bytes::buf::Buf como "Implementations on Foreign Types"
    // Es decir, la implementacion esta en el fichero con el codigo del Trait Buf
    // no en el fichero con la implementacion de Cursor.
    if !src.has_remaining() {
        // Si no hay mas bytes para consumir se retorna un error.
        return Err(Error::Incomplete);
    }
    // Retorna el lsiguiente bytes y avanza una posicion
    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    // Cursor implementa bytes::buf::Buf como "Implementations on Foreign Types"
    // Es decir, la implementacion esta en el fichero con el codigo del Trait Buf
    // no en el fichero con la implementacion de Cursor.
    if src.remaining() < n {
        return Err(Error::Incomplete);
        // Si no estan el numero de bytes indicados para consumir se retorna un error.
    }
    // Se avanza las posiciones indicadas
    src.advance(n);
    Ok(())
}

/// Lee un entero (sin signo) que este codificado en texto en la siguiente linea.
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Intenta obtener una linea
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Obtiene la posicion actual
    let start = src.position() as usize;
    // Se obtiene el slice subyacente
    let inner = src.get_ref();
    // Scan to the second to last byte
    let end = inner.len() - 1;

    for i in start..end {
        if inner[i] == b'\r' && inner[i + 1] == b'\n' {
            // Hemos encontrado una linea, se actualiza la posicion despues de \n
            src.set_position((i + 2) as u64);
            
            // Se retorna la linea
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

// Se implementa core::convert::From 
// para conversion String -> mini_redis::frame::Error
impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

// Utiliza la implementacion automatica de core::convert::Into 
// al implementar core::convert::From 
// para conversion String -> mini_redis::frame::Error
impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

// Utiliza la implementacion automatica de core::convert::Into 
// al implementar core::convert::From 
// para conversion String -> mini_redis::frame::Error
impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

// Utiliza la implementacion automatica de core::convert::Into 
// al implementar core::convert::From 
// para conversion String -> mini_redis::frame::Error
impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

// Implementa `std::error::Error` en `mini_redis::frame::Error'
// para poder retornar el error estipulado de forma general
// para el crate.
impl std::error::Error for Error {}

// Se implementa `fmt::Display`para poder visualizar el error.
impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
