use crate::{HmacSha256, MAGIC_NUMBER};
use hmac::Mac;
use std::collections::VecDeque;
use std::io::Error;
use std::io::ErrorKind;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum MsgCategory {
    Read,
    Data,
    Write,
    Confirmation,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct AckMessage {
    pub category: MsgCategory,
    pub rank_id: u8,
    pub unique_id: Uuid,
}

pub async fn decode_ack(
    stream: &mut (dyn AsyncRead + Send + Unpin),
    key: &[u8; 64],
) -> Result<(AckMessage, bool), Error> {
    let mut buffer = VecDeque::new();

    loop {
        let byte = stream.read_u8().await?;
        buffer.push_back(byte);

        if buffer.len() >= 4 && buffer.make_contiguous() == MAGIC_NUMBER {
            break;
        }
        if buffer.len() > 4 {
            buffer.pop_front();
        }
    }

    let mut padding = [0u8; 2];
    stream.read_exact(&mut padding).await?;

    let rank_id = stream.read_u8().await?;
    let raw_category = stream.read_u8().await?;

    let category = match raw_category {
        0x43 => MsgCategory::Read,
        0x44 => MsgCategory::Data,
        0x45 => MsgCategory::Write,
        0x46 => MsgCategory::Confirmation,
        _ => return Err(Error::new(ErrorKind::InvalidInput, "Unknown message type")),
    };

    let mut content_buffer = [0u8; 48];
    stream.read_exact(&mut content_buffer).await?;

    let uuid_bytes = &content_buffer[..16];
    let hmac_code = &content_buffer[16..];

    let uuid = u128::from_be_bytes(uuid_bytes.try_into().unwrap());

    let payload = [
        buffer.make_contiguous(),
        &mut padding,
        &mut rank_id.to_be_bytes(),
        &mut raw_category.to_be_bytes(),
        &mut uuid.to_be_bytes(),
    ]
    .concat();

    let ack_message = AckMessage {
        rank_id,
        unique_id: Uuid::from_u128(uuid),
        category,
    };

    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&payload);
    let valid = mac.verify_slice(hmac_code).is_ok();

    Ok((ack_message, valid))
}

pub async fn encode_ack(
    ack_message: &AckMessage,
    output: &mut (dyn AsyncWrite + Send + Unpin),
    key: &[u8],
) -> Result<(), Error> {
    let raw_category: u8 = match ack_message.category {
        MsgCategory::Read => 0x43,
        MsgCategory::Data => 0x44,
        MsgCategory::Write => 0x45,
        MsgCategory::Confirmation => 0x46,
    };

    let mut message_bytes = [
        MAGIC_NUMBER.as_slice(),
        &[0u8; 2],
        &ack_message.rank_id.to_be_bytes(),
        &raw_category.to_be_bytes(),
        ack_message.unique_id.as_bytes(),
    ]
    .concat();

    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&message_bytes);
    let mac_result = mac.finalize().into_bytes();

    message_bytes.extend_from_slice(&mac_result);
    output.write_all(&message_bytes).await?;

    Ok(())
}
