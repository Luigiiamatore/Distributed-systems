use crate::stubborn_register_client::timer::TimerHandle;
use crate::transfer::{decode_ack, AckMessage, MsgCategory};
use crate::{serialize_register_command, RegisterCommand, SystemRegisterCommand, SystemRegisterCommandContent};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct StubbornLink {
    unacknowledged_messages: Arc<Mutex<HashMap<AckMessage, TimerHandle>>>,
    message_sender: UnboundedSender<Arc<SystemRegisterCommand>>,
    target_process: u8,
}

impl StubbornLink {
    pub fn new(
        target_process: u8,
        network_locations: Vec<(String, u16)>,
        hmac_key: Arc<[u8; 64]>,
    ) -> Self {
        let (address, port) = network_locations.get((target_process - 1) as usize).unwrap();
        let (message_sender, message_receiver) = unbounded_channel();

        let link = StubbornLink {
            unacknowledged_messages: Arc::new(Mutex::new(HashMap::new())),
            message_sender,
            target_process,
        };

        tokio::spawn(connection_loop(
            link.clone(),
            message_receiver,
            address.clone(),
            *port,
            hmac_key,
        ));

        link
    }

    pub async fn queue_message(&self, message: Arc<SystemRegisterCommand>) {
        let acknowledgment = AckMessage {
            category: match message.content {
                SystemRegisterCommandContent::ReadProc => MsgCategory::Read,
                SystemRegisterCommandContent::Value { .. } => MsgCategory::Data,
                SystemRegisterCommandContent::WriteProc { .. } => MsgCategory::Write,
                SystemRegisterCommandContent::Ack => MsgCategory::Confirmation,
            },
            rank_id: self.target_process,
            unique_id: message.header.msg_ident,
        };

        let timer = TimerHandle::start_timer(message, self.message_sender.clone());
        self.unacknowledged_messages
            .lock()
            .await
            .insert(acknowledgment, timer);
    }

    async fn confirm_acknowledgment(&self, ack: AckMessage) {
        if let Some(timer) = self.unacknowledged_messages.lock().await.remove(&ack) {
            timer.stop().await;
        }
    }
}

async fn connection_loop(
    link: StubbornLink,
    message_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
    address: String,
    port: u16,
    hmac_key: Arc<[u8; 64]>,
) {
    let (stream_tx, stream_rx) = unbounded_channel();

    tokio::spawn(send_pending_messages(message_queue, stream_rx, hmac_key.clone()));

    loop {
        let connection_result = TcpStream::connect((address.as_str(), port)).await;
        if connection_result.is_err() {
            continue;
        }

        let stream = connection_result.unwrap();
        let (reader, writer) = stream.into_split();

        stream_tx.send(writer).unwrap();

        process_acknowledgments(link.clone(), reader, hmac_key.clone()).await;
    }
}

async fn send_pending_messages(
    mut message_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
    mut write_stream_rx: UnboundedReceiver<OwnedWriteHalf>,
    hmac_key: Arc<[u8; 64]>,
) {
    let mut active_stream = match write_stream_rx.recv().await {
        Some(stream) => stream,
        None => return,
    };

    loop {
        tokio::select! {
            Some(new_stream) = write_stream_rx.recv() => active_stream = new_stream,
            Some(message) = message_queue.recv() => {
                let _ = serialize_register_command(
                    &RegisterCommand::System(message.deref().clone()),
                    &mut active_stream,
                    hmac_key.deref(),
                )
                .await;
            }
        }
    }
}

async fn process_acknowledgments(
    link: StubbornLink,
    mut read_stream: OwnedReadHalf,
    hmac_key: Arc<[u8; 64]>,
) {
    loop {
        if let Ok(ack) = receive_acknowledgment(&mut read_stream, hmac_key.clone()).await {
            link.confirm_acknowledgment(ack).await;
        }
    }
}

async fn receive_acknowledgment(
    stream: &mut OwnedReadHalf,
    hmac_key: Arc<[u8; 64]>,
) -> Result<AckMessage, ()> {
    loop {
        let mut buffer = [0u8; 1];
        if stream.peek(&mut buffer).await.is_err() {
            return Err(());
        }

        if let Ok((ack, true)) = decode_ack(stream, hmac_key.deref()).await {
            return Ok(ack);
        }
    }
}
