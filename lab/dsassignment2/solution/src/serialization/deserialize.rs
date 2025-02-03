use super::message::Message;
use crate::serialization::SEC_SIZE;
use crate::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand,
    SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent,
    MAGIC_NUMBER,
};
use std::collections::VecDeque;
use std::io::{Error, ErrorKind::InvalidInput};
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {
    let mut buffer = VecDeque::from([0u8; 4]);

    data.read_exact(buffer.make_contiguous()).await?;

    while buffer.make_contiguous() != MAGIC_NUMBER {
        buffer.push_back(data.read_u8().await?);
        buffer.pop_front();
    }

    let mut remainder_buffer = [0u8; 4];
    data.read_exact(&mut remainder_buffer).await?;

    let header = [buffer.make_contiguous(), remainder_buffer.as_slice()].concat();

    match remainder_buffer[3] {
        0x01 | 0x02 => {
            process_client_command(
                parse_message(
                    data,
                    48 + if remainder_buffer[3] == 0x01 {
                        0
                    } else {
                        SEC_SIZE
                    },
                    header,
                )
                .await?,
                hmac_client_key,
                remainder_buffer[3],
            )
            .await
        }
        0x03 | 0x04 | 0x05 | 0x06 => {
            process_system_command(
                parse_message(
                    data,
                    match remainder_buffer[3] {
                        0x03 | 0x06 => 56,
                        0x04 | 0x05 => 56 + 16 + SEC_SIZE,
                        _ => unreachable!(),
                    },
                    header,
                )
                .await?,
                hmac_system_key,
                remainder_buffer[2],
                remainder_buffer[3],
            )
            .await
        }
        _ => Err(Error::from(InvalidInput)),
    }
}

async fn parse_message(
    reader: &mut (dyn AsyncRead + Send + Unpin),
    bytes: usize,
    header: Vec<u8>,
) -> Result<Message, Error> {
    let mut message_buffer = vec![0u8; bytes];

    reader.read_exact(&mut message_buffer).await?;

    Ok(Message {
        header,
        body: message_buffer[..message_buffer.len() - 32].to_vec(),
        tag: message_buffer[message_buffer.len() - 32..].to_vec(),
    })
}

async fn process_client_command(
    message: Message,
    hmac_key: &[u8; 32],
    op_type: u8,
) -> Result<(RegisterCommand, bool), Error> {
    let reader: &mut (dyn AsyncRead + Send + Unpin) = &mut message.body.as_slice();

    let request_id = reader.read_u64().await?;
    let sector_idx = reader.read_u64().await?;

    Ok((
        RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: request_id,
                sector_idx,
            },
            content: client_command_content(reader, op_type).await?,
        }),
        message.verify_hmac(hmac_key),
    ))
}

async fn client_command_content(
    reader: &mut (dyn AsyncRead + Send + Unpin),
    command_type: u8,
) -> Result<ClientRegisterCommandContent, Error> {
    if command_type == 0x01 {
        Ok(ClientRegisterCommandContent::Read)
    } else {
        let mut data = vec![0u8; SEC_SIZE];
        reader.read_exact(&mut data).await?;

        Ok(ClientRegisterCommandContent::Write {
            data: SectorVec(data),
        })
    }
}

async fn process_system_command(
    message: Message,
    hmac_key: &[u8; 64],
    process_id: u8,
    op_type: u8,
) -> Result<(RegisterCommand, bool), Error> {
    let reader: &mut (dyn AsyncRead + Send + Unpin) = &mut message.body.as_slice();

    let message_id = Uuid::from_u128(reader.read_u128().await?);
    let sector_idx = reader.read_u64().await?;

    Ok((
        RegisterCommand::System(SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: process_id,
                msg_ident: message_id,
                sector_idx,
            },
            content: system_command_content(reader, op_type).await?,
        }),
        message.verify_hmac(hmac_key),
    ))
}

async fn system_command_content(
    reader: &mut (dyn AsyncRead + Send + Unpin),
    op_type: u8,
) -> Result<SystemRegisterCommandContent, Error> {
    match op_type {
        0x03 => Ok(SystemRegisterCommandContent::ReadProc),
        0x04 | 0x05 => {
            let timestamp = reader.read_u64().await?;

            let mut writer_buffer = [0u8; 8];
            reader.read_exact(&mut writer_buffer).await?;
            let writer_rank = writer_buffer[7];

            let mut data = vec![0u8; SEC_SIZE];
            reader.read_exact(&mut data).await?;

            Ok(if op_type == 0x04 {
                SystemRegisterCommandContent::Value {
                    timestamp,
                    write_rank: writer_rank,
                    sector_data: SectorVec(data),
                }
            } else {
                SystemRegisterCommandContent::WriteProc {
                    timestamp,
                    write_rank: writer_rank,
                    data_to_write: SectorVec(data),
                }
            })
        }
        0x06 => Ok(SystemRegisterCommandContent::Ack),
        _ => Err(Error::from(InvalidInput)),
    }
}
