use crate::registers_manager::register_handler::RegisterHandler;
use crate::{
    ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager, SuccessCallbackType,
    SystemCallbackType, SystemRegisterCommand,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

mod register_handler;

#[derive(Clone)]
pub struct RegistersManager {
    client_sender: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
    system_sender: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>,
}

impl RegistersManager {
    pub fn init(
        rank: u8,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        total_processes: u8,
    ) -> Self {
        let (system_sender, system_receiver) = unbounded_channel();
        let (client_sender, client_receiver) = unbounded_channel();

        tokio::spawn(Self::event_loop(
            rank,
            register_client,
            sectors_manager,
            total_processes,
            system_receiver,
            client_receiver,
        ));

        Self {
            client_sender,
            system_sender,
        }
    }

    async fn event_loop(
        rank: u8,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        total_processes: u8,
        mut system_receiver: UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
        mut client_receiver: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
    ) {
        let mut handlers: HashMap<SectorIdx, RegisterHandler> = HashMap::new();

        loop {
            tokio::select! {
                Some((cmd, callback)) = system_receiver.recv() => {
                    let handler = Self::get_or_create_handler(
                        cmd.header.sector_idx,
                        &mut handlers,
                        rank,
                        register_client.clone(),
                        sectors_manager.clone(),
                        total_processes
                    ).await;

                    handler.enqueue_system_command(cmd, callback);
                },
                Some((cmd, callback)) = client_receiver.recv() => {
                    let handler = Self::get_or_create_handler(
                        cmd.header.sector_idx,
                        &mut handlers,
                        rank,
                        register_client.clone(),
                        sectors_manager.clone(),
                        total_processes
                    ).await;

                    handler.enqueue_client_command(cmd, callback);
                },
                else => break,
            }
        }
    }

    async fn get_or_create_handler(
        sector: SectorIdx,
        handlers: &mut HashMap<SectorIdx, RegisterHandler>,
        rank: u8,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        total_processes: u8,
    ) -> RegisterHandler {
        if let Some(handler) = handlers.get(&sector) {
            return handler.clone();
        }

        let new_handler = RegisterHandler::create(
            sector,
            rank,
            total_processes,
            register_client,
            sectors_manager,
        )
            .await;

        handlers.insert(sector, new_handler.clone());
        new_handler
    }

    pub fn submit_client_command(&self, cmd: ClientRegisterCommand, callback: SuccessCallbackType) {
        let _ = self.client_sender.send((cmd, callback));
    }

    pub fn submit_system_command(&self, cmd: SystemRegisterCommand, callback: SystemCallbackType) {
        let _ = self.system_sender.send((cmd, callback));
    }
}
