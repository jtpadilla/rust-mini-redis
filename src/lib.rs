//! A minimal (i.e. very incomplete) implementation of a Redis server and
//! client.
//!
//! The purpose of this project is to provide a larger example of an
//! asynchronous Rust project built with Tokio. Do not attempt to run this in
//! production... seriously.
//!
//! # Layout
//!
//! The library is structured such that it can be used with guides. There are
//! modules that are public that probably would not be public in a "real" redis
//! client library.
//!
//! The major components are:
//!
//! * `server`: Redis server implementation. Includes a single `run` function
//!   that takes a `TcpListener` and starts accepting redis client connections.
//!
//! * `client`: an asynchronous Redis client implementation. Demonstrates how to
//!   build clients with Tokio.
//!
//! * `cmd`: implementations of the supported Redis commands.
//!
//! * `frame`: represents a single Redis protocol frame. A frame is used as an
//!   intermediate representation between a "command" and the byte
//!   representation.

pub mod blocking_client;
pub mod client;

pub mod cmd;
pub use cmd::Command;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;
use db::DbDropGuard;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod buffer;
pub use buffer::{buffer, Buffer};

mod shutdown;
use shutdown::Shutdown;

/// Puerto por defecto que se utilizara si no se especifica otro
pub const DEFAULT_PORT: u16 = 6379;

/// Error retornado pro la mayoria de funciones.
/// 
/// En una aplicacion real se puede considerar especializar la
/// gestion de errores del crate por ejemplo definiendo el error
/// como una enumeracion de causas.
/// 
/// Pero para este ejemplo se utilizara un boxed `std::error::Error`.
/// 
/// Por motivos de rendimiento, se evitara el boxing en cualquier 
/// "hot path" o llamadas a metodos muy frecuentes utilizando en este
/// casi un error definido mediante 'enum'. Se utilizara el error 
/// definido como una 'enum' pero de implementara `std::error:Error` lo
/// cual permitira retornarlo para convertirlo en un `Box<dyn std::error::Error>`
pub type Error = Box<dyn std::error::Error + Send + Sync>;

// Un `Result`especializado para las operaciones del crate.
pub type Result<T> = std::result::Result<T, Error>;
