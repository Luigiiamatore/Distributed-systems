use super::hmac::generate_hmac;
use crate::{
    ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SystemRegisterCommand,
    SystemRegisterCommandContent, MAGIC_NUMBER,
};
use std::io::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    let mut payload = match cmd {
        RegisterCommand::Client(cmd) => serialize_client_command(cmd),
        RegisterCommand::System(cmd) => serialize_system_command(cmd),
    };

    let tag = generate_hmac(&payload, hmac_key);

    payload.extend_from_slice(&tag);
    writer.write_all(&payload).await?;

    Ok(())
}

fn serialize_client_command(command: &ClientRegisterCommand) -> Vec<u8> {
    let mut buffer = Vec::new();

    buffer.extend_from_slice(&MAGIC_NUMBER);
    buffer.extend_from_slice(&[0u8; 3]);
    buffer.push(match command.content {
        ClientRegisterCommandContent::Read => 0x01,
        ClientRegisterCommandContent::Write { .. } => 0x02,
    });
    buffer.extend_from_slice(&command.header.request_identifier.to_be_bytes());
    buffer.extend_from_slice(&command.header.sector_idx.to_be_bytes());

    if let ClientRegisterCommandContent::Write { data } = &command.content {
        buffer.extend_from_slice(&data.0);
    }

    buffer
}

fn serialize_system_command(command: &SystemRegisterCommand) -> Vec<u8> {
    let mut buffer = Vec::new();

    buffer.extend_from_slice(MAGIC_NUMBER.as_slice());
    buffer.extend_from_slice(&[0u8; 2]);
    buffer.extend_from_slice(&command.header.process_identifier.to_be_bytes());
    buffer.push(match command.content {
        SystemRegisterCommandContent::ReadProc => 0x03u8,
        SystemRegisterCommandContent::Value { .. } => 0x04u8,
        SystemRegisterCommandContent::WriteProc { .. } => 0x05u8,
        SystemRegisterCommandContent::Ack => 0x06u8,
    });
    buffer.extend_from_slice(&command.header.msg_ident.into_bytes());
    buffer.extend_from_slice(&command.header.sector_idx.to_be_bytes());

    match &command.content {
        SystemRegisterCommandContent::Value {
            timestamp,
            write_rank,
            sector_data,
        } => {
            buffer.extend_from_slice(&timestamp.to_be_bytes());
            buffer.extend_from_slice(&[0u8; 7]);
            buffer.push(*write_rank);
            buffer.extend_from_slice(&sector_data.0);
        }
        SystemRegisterCommandContent::WriteProc {
            timestamp,
            write_rank,
            data_to_write,
        } => {
            buffer.extend_from_slice(&timestamp.to_be_bytes());
            buffer.extend_from_slice(&[0u8; 7]);
            buffer.push(*write_rank);
            buffer.extend_from_slice(&data_to_write.0);
        }
        _ => {}
    }

    buffer
}
