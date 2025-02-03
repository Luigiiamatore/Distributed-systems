const SEC_SIZE: usize = 4096;

pub use deserialize::deserialize_register_command;
pub use serialize::serialize_register_command;

mod deserialize;
mod hmac;
mod message;
mod serialize;
