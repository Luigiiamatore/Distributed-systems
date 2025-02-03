use crate::{
    build_atomic_register, AtomicRegister, ClientRegisterCommand, RegisterClient, SectorIdx,
    SectorsManager, SuccessCallbackType, SystemCallbackType, SystemRegisterCommand,
};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Clone)]
pub struct RegisterHandler {
    client_sender: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
    system_sender: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>,
}

impl RegisterHandler {
    pub async fn create(
        sector: SectorIdx,
        rank: u8,
        process_count: u8,
        client: Arc<dyn RegisterClient>,
        storage: Arc<dyn SectorsManager>,
    ) -> Self {
        let (client_tx, client_rx) = unbounded_channel();
        let (system_tx, system_rx) = unbounded_channel();
        let (completion_tx, completion_rx) = unbounded_channel();

        let register = build_atomic_register(rank, sector, client, storage, process_count).await;

        tokio::spawn(Self::process_events(
            client_rx,
            system_rx,
            completion_rx,
            completion_tx,
            register,
        ));

        Self {
            client_sender: client_tx,
            system_sender: system_tx,
        }
    }

    async fn process_events(
        mut client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
        mut system_rx: UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
        mut completion_rx: UnboundedReceiver<()>,
        completion_tx: UnboundedSender<()>,
        mut register: Box<dyn AtomicRegister>,
    ) {
        let mut processing_client_command = false;

        loop {
            if processing_client_command {
                if let Some(done) =
                    Self::handle_while_processing(&mut system_rx, &mut completion_rx, &mut register)
                        .await
                {
                    processing_client_command = !done;
                }
            } else {
                if let Some(start) =
                    Self::handle_when_free(&mut client_rx, &mut system_rx, completion_tx.clone(), &mut register)
                        .await
                {
                    processing_client_command = start;
                }
            }
        }
    }

    async fn handle_when_free(
        client_rx: &mut UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
        system_rx: &mut UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
        completion_tx: UnboundedSender<()>,
        register: &mut Box<dyn AtomicRegister>,
    ) -> Option<bool> {
        tokio::select! {
            biased;
            Some((cmd, callback)) = client_rx.recv() => {
                register.client_command(cmd, Box::new(move |success| {
                    Box::pin(async move {
                        let _ = completion_tx.send(());
                        callback(success).await;
                    })
                })).await;
                Some(true)
            },
            Some((cmd, callback)) = system_rx.recv() => {
                register.system_command(cmd).await;
                callback().await;
                Some(false)
            },
            else => None,
        }
    }

    async fn handle_while_processing(
        system_rx: &mut UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
        completion_rx: &mut UnboundedReceiver<()>,
        register: &mut Box<dyn AtomicRegister>,
    ) -> Option<bool> {
        tokio::select! {
            biased;
            Some(()) = completion_rx.recv() => Some(true),
            Some((cmd, callback)) = system_rx.recv() => {
                register.system_command(cmd).await;
                callback().await;
                Some(false)
            },
            else => None,
        }
    }

    pub fn enqueue_client_command(&self, cmd: ClientRegisterCommand, callback: SuccessCallbackType) {
        let _ = self.client_sender.send((cmd, callback));
    }

    pub fn enqueue_system_command(&self, cmd: SystemRegisterCommand, callback: SystemCallbackType) {
        let _ = self.system_sender.send((cmd, callback));
    }
}
