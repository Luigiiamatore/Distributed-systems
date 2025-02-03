use hmac::Hmac;
use sha2::Sha256;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::registers_manager::RegistersManager;
use crate::stubborn_register_client::StubbornRegisterClient;
use crate::transfer::{
    encode_ack, serialize_register_response, AckMessage, MsgCategory, OperationError,
    OperationResult, RegisterResponse,
};

pub use atomic_register::*;
pub use domain::*;
pub use register_client::*;
pub use sectors_manager::*;
pub use serialization::*;

mod atomic_register;
mod domain;
mod register_client;
mod sectors_manager;
mod transfer;

mod registers_manager;
mod serialization;
mod stubborn_register_client;

type HmacSha256 = Hmac<Sha256>;
type SuccessCallbackType = Box<
    dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + std::marker::Send>>
    + std::marker::Send
    + Sync,
>;
type SystemCallbackType = Box<
    dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + std::marker::Send>>
    + std::marker::Send
    + Sync,
>;

pub async fn run_register_process(config: Configuration) {
    let hmac_system_key = Arc::new(config.hmac_system_key);
    let hmac_client_key = Arc::new(config.hmac_client_key);

    let (system_tx, system_rx) = unbounded_channel();
    let (client_tx, client_rx) = unbounded_channel();

    let sectors = build_sectors_manager(config.public.storage_dir).await;
    let client = Arc::new(StubbornRegisterClient::init(
        config.public.tcp_locations.clone(),
        hmac_system_key.clone(),
        config.public.self_rank,
        system_tx.clone(),
    ));

    let local_addr = config
        .public
        .tcp_locations
        .get((config.public.self_rank - 1) as usize)
        .unwrap();

    let tcp_listener = TcpListener::bind((local_addr.0.as_str(), local_addr.1))
        .await
        .unwrap();

    let manager = RegistersManager::init(
        config.public.self_rank,
        client,
        sectors,
        config.public.tcp_locations.len() as u8,
    );

    tokio::spawn(dispatch_commands(
        system_rx,
        client_rx,
        manager,
    ));

    loop {
        let (tcp_stream, _) = tcp_listener.accept().await.unwrap();

        tokio::spawn(handle_connection(
            tcp_stream,
            hmac_system_key.clone(),
            hmac_client_key.clone(),
            config.public.n_sectors,
            config.public.self_rank,
            system_tx.clone(),
            client_tx.clone(),
        ));
    }
}

async fn handle_connection(
    conn_stream: TcpStream,
    hmac_system_key: Arc<[u8; 64]>,
    hmac_client_key: Arc<[u8; 32]>,
    total_sectors: u64,
    self_rank: u8,
    system_channel: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>,
    client_channel: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
) {
    let (mut reader, writer) = conn_stream.into_split();
    let (client_resp_tx, client_resp_rx) = unbounded_channel();
    let (system_resp_tx, system_resp_rx) = unbounded_channel();

    tokio::spawn(handle_writer(
        client_resp_rx,
        system_resp_rx,
        writer,
        hmac_client_key.clone(),
        hmac_system_key.clone(),
    ));

    loop {
        let mut buf = [0u8; 1];
        if let Ok(0) = reader.peek(&mut buf).await {
            return;
        }

        let (cmd, valid) =
            extract_command(&mut reader, &hmac_system_key, &hmac_client_key).await;
        let sector_idx = get_sector_index(&cmd);

        if !validate_command(
            &cmd,
            valid,
            sector_idx,
            total_sectors,
            client_resp_tx.clone(),
        )
            .await
        {
            continue;
        }

        match cmd {
            RegisterCommand::Client(client_cmd) => {
                let client_resp_tx = client_resp_tx.clone();

                let cb: SuccessCallbackType = Box::new(|result| {
                    Box::pin(async move {
                        let response = match &result.op_return {
                            OperationReturn::Read(_) => {
                                RegisterResponse::Read(OperationResult::Success(result))
                            }
                            OperationReturn::Write => {
                                RegisterResponse::Write(OperationResult::Success(result))
                            }
                        };

                        client_resp_tx.send(response).unwrap()
                    })
                });

                client_channel
                    .send((client_cmd, cb))
                    .unwrap();
            }
            RegisterCommand::System(sys_cmd) => {
                let system_resp_tx = system_resp_tx.clone();

                let ack = Arc::new(AckMessage {
                    category: match sys_cmd.content {
                        SystemRegisterCommandContent::ReadProc => MsgCategory::Read,
                        SystemRegisterCommandContent::Value { .. } => MsgCategory::Data,
                        SystemRegisterCommandContent::WriteProc { .. } => MsgCategory::Write,
                        SystemRegisterCommandContent::Ack => MsgCategory::Confirmation,
                    },
                    rank_id: self_rank,
                    unique_id: sys_cmd.header.msg_ident,
                });

                let cb: SystemCallbackType = Box::new(|| {
                    Box::pin(async move { system_resp_tx.send(*ack.deref()).unwrap() })
                });

                system_channel
                    .send((sys_cmd, cb))
                    .unwrap();
            }
        }
    }
}

async fn handle_writer(
    mut client_rx: UnboundedReceiver<RegisterResponse>,
    mut system_rx: UnboundedReceiver<AckMessage>,
    mut writer: OwnedWriteHalf,
    client_key: Arc<[u8; 32]>,
    system_key: Arc<[u8; 64]>,
) {
    loop {
        tokio::select! {
            Some(client_resp) = client_rx.recv() => {
                serialize_register_response(&client_resp, &mut writer, client_key.deref()).await.unwrap();
            },
            Some(ack) = system_rx.recv() => {
                encode_ack(&ack, &mut writer, system_key.deref()).await.unwrap();
            },
            else => break
        }
    }
}

async fn dispatch_commands(
    mut system_rx: UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
    mut client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
    manager: RegistersManager,
) {
    loop {
        tokio::select! {
            Some((sys_cmd, sys_cb)) = system_rx.recv() => {
                manager.submit_system_command(sys_cmd, sys_cb);
            },
            Some((client_cmd, client_cb)) = client_rx.recv() => {
                manager.submit_client_command(client_cmd, client_cb);
            }
        }
    }
}

fn get_sector_index(command: &RegisterCommand) -> SectorIdx {
    match command {
        RegisterCommand::Client(cmd) => cmd.header.sector_idx,
        RegisterCommand::System(cmd) => cmd.header.sector_idx,
    }
}

async fn extract_command(
    reader: &mut (dyn AsyncRead + std::marker::Send + Unpin),
    sys_key: &[u8; 64],
    cli_key: &[u8; 32],
) -> (RegisterCommand, bool) {
    loop {
        if let Ok(command) = deserialize_register_command(reader, sys_key, cli_key).await {
            return command;
        }
    }
}

async fn validate_command(
    cmd: &RegisterCommand,
    hmac_valid: bool,
    sector_idx: SectorIdx,
    total_sectors: u64,
    client_tx: UnboundedSender<RegisterResponse>,
) -> bool {
    if hmac_valid && sector_idx < total_sectors {
        return true;
    }

    if let RegisterCommand::Client(client_cmd) = cmd {
        let req_id = client_cmd.header.request_identifier;

        let error = if !hmac_valid {
            OperationError::InvalidMac(req_id)
        } else {
            OperationError::InvalidSector(req_id)
        };

        let resp = match client_cmd.content {
            ClientRegisterCommandContent::Read => RegisterResponse::Read(OperationResult::Failure(error)),
            ClientRegisterCommandContent::Write { .. } => RegisterResponse::Write(OperationResult::Failure(error)),
        };

        client_tx.send(resp).unwrap();
    }

    false
}
