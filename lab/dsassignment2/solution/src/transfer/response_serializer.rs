use crate::{HmacSha256, OperationReturn, OperationSuccess, StatusCode, MAGIC_NUMBER};
use hmac::Mac;
use std::io::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone)]
pub enum RegisterResponse {
    Read(OperationResult),
    Write(OperationResult),
}

#[derive(Debug, Clone)]
pub enum OperationResult {
    Success(OperationSuccess),
    Failure(OperationError),
}

#[derive(Debug, Clone)]
pub enum OperationError {
    InvalidMac(u64),
    InvalidSector(u64),
}

pub async fn serialize_register_response(
    response: &RegisterResponse,
    writer: &mut (dyn AsyncWrite + Unpin + Send),
    hmac_key: &[u8],
) -> Result<(), Error> {
    let mut payload = build_response_payload(response);
    add_hmac(&mut payload, hmac_key)?;
    writer.write_all(&payload).await?;
    Ok(())
}

fn build_response_payload(response: &RegisterResponse) -> Vec<u8> {
    let (msg_type, status_code, request_id) = extract_response_info(response);
    let mut payload = Vec::new();

    payload.extend_from_slice(&MAGIC_NUMBER);
    payload.extend_from_slice(&[0, 0]); // Padding
    payload.push(status_code as u8);
    payload.push(msg_type);
    payload.extend_from_slice(&request_id.to_be_bytes());

    if let RegisterResponse::Read(OperationResult::Success(op)) = response {
        if let OperationReturn::Read(data) = &op.op_return {
            payload.extend_from_slice(&data.read_data.0);
        }
    }

    payload
}

fn extract_response_info(response: &RegisterResponse) -> (u8, StatusCode, u64) {
    match response {
        RegisterResponse::Read(result) | RegisterResponse::Write(result) => match result {
            OperationResult::Success(op) => (
                match response {
                    RegisterResponse::Read(_) => 0x41,
                    RegisterResponse::Write(_) => 0x42,
                },
                StatusCode::Ok,
                op.request_identifier,
            ),
            OperationResult::Failure(err) => match err {
                OperationError::InvalidMac(req_id) => (0x42, StatusCode::AuthFailure, *req_id),
                OperationError::InvalidSector(req_id) => {
                    (0x42, StatusCode::InvalidSectorIndex, *req_id)
                }
            },
        },
    }
}

fn add_hmac(payload: &mut Vec<u8>, hmac_key: &[u8]) -> Result<(), Error> {
    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(payload);
    payload.extend_from_slice(&mac.finalize().into_bytes());
    Ok(())
}
