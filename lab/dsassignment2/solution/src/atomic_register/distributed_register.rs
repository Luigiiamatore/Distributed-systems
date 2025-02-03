use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;
use crate::{AtomicRegister, Broadcast, ClientRegisterCommand, ClientRegisterCommandContent, OperationReturn, OperationSuccess, ReadReturn, RegisterClient, SectorIdx, SectorsManager, SectorVec, SuccessCallbackType, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};

#[derive(PartialEq, Eq)]
enum OperationStatus {
    Reading(u64),
    Writing(u64),
    Idle,
}

#[derive(PartialEq, Eq)]
enum ProtocolStep {
    Gathering,
    Distributing,
}

#[derive(Clone)]
struct LocalState {
    value: SectorVec,
    timestamp: u64,
    writer_id: u8,
}

impl LocalState {
    fn compare_states(first: &Self, second: &Self) -> Ordering {
        first.timestamp.cmp(&second.timestamp)
            .then(first.writer_id.cmp(&second.writer_id))
    }

    fn as_tuple(&self) -> (SectorVec, u64, u8) {
        (self.value.clone(), self.timestamp, self.writer_id)
    }
}

pub struct DistributedRegister {
    id: u8,
    sector_index: SectorIdx,
    total_processes: u8,
    client: Arc<dyn RegisterClient>,
    storage: Arc<dyn SectorsManager>,

    current_state: LocalState,
    data_collections: HashMap<u8, LocalState>,
    acknowledgments: HashSet<u8>,
    operation: OperationStatus,
    operation_id: Option<Uuid>,
    temp_read: Option<SectorVec>,
    temp_write: Option<SectorVec>,
    step: ProtocolStep,

    callback: Option<SuccessCallbackType>,
}

impl DistributedRegister {
    async fn process_value(&mut self, op_id: Uuid, sender: u8, state: LocalState) {
        if self.operation_id != Some(op_id) || self.step != ProtocolStep::Gathering {
            return;
        }

        self.data_collections.insert(sender, state);

        if self.data_collections.len() <= self.total_processes as usize / 2 {
            return;
        }

        self.data_collections.insert(self.id, self.current_state.clone());
        let mut sorted_data: Vec<_> = self.data_collections.values().cloned().collect();
        sorted_data.sort_by(LocalState::compare_states);
        let latest_state = sorted_data.last().unwrap().clone();

        self.temp_read = Some(latest_state.value.clone());
        self.data_collections.clear();
        self.acknowledgments.clear();
        self.step = ProtocolStep::Distributing;

        match self.operation {
            OperationStatus::Reading(_) => {
                let write_request = self.create_write_request(latest_state.value.clone(), latest_state.timestamp, latest_state.writer_id);
                self.client.broadcast(Broadcast { cmd: Arc::new(write_request) }).await;
            },
            OperationStatus::Writing(_) => {
                self.current_state = LocalState {
                    value: self.temp_write.clone().unwrap(),
                    timestamp: latest_state.timestamp + 1,
                    writer_id: self.id,
                };
                self.storage.write(self.sector_index, &self.current_state.as_tuple()).await;

                let write_request = self.create_write_request(self.current_state.value.clone(), self.current_state.timestamp, self.current_state.writer_id);
                self.client.broadcast(Broadcast { cmd: Arc::new(write_request) }).await;
            },
            _ => {},
        }
    }

    async fn process_ack(&mut self, op_id: Uuid, sender: u8) {
        if self.operation_id != Some(op_id) || self.step != ProtocolStep::Distributing {
            return;
        }

        self.acknowledgments.insert(sender);

        if self.acknowledgments.len() <= self.total_processes as usize / 2 || self.operation == OperationStatus::Idle {
            return;
        }

        self.acknowledgments.clear();
        self.step = ProtocolStep::Gathering;

        let success = match self.operation {
            OperationStatus::Reading(req_id) => OperationSuccess {
                request_identifier: req_id,
                op_return: OperationReturn::Read(ReadReturn { read_data: self.temp_read.clone().unwrap() }),
            },
            OperationStatus::Writing(req_id) => OperationSuccess {
                request_identifier: req_id,
                op_return: OperationReturn::Write,
            },
            _ => unreachable!(),
        };

        self.callback.take().unwrap()(success).await;
        self.operation = OperationStatus::Idle;
        self.operation_id = None;
    }

    fn create_read_request(&self) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.id,
                msg_ident: self.operation_id.unwrap(),
                sector_idx: self.sector_index,
            },
            content: SystemRegisterCommandContent::ReadProc,
        }
    }

    fn create_value_response(&self, op_id: Uuid) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.id,
                msg_ident: op_id,
                sector_idx: self.sector_index,
            },
            content: SystemRegisterCommandContent::Value {
                timestamp: self.current_state.timestamp,
                write_rank: self.current_state.writer_id,
                sector_data: self.current_state.value.clone(),
            },
        }
    }

    fn create_write_request(&self, data: SectorVec, timestamp: u64, writer_id: u8) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.id,
                msg_ident: self.operation_id.unwrap(),
                sector_idx: self.sector_index,
            },
            content: SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank: writer_id,
                data_to_write: data,
            },
        }
    }

    fn create_ack_response(&self, op_id: Uuid) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.id,
                msg_ident: op_id,
                sector_idx: self.sector_index,
            },
            content: SystemRegisterCommandContent::Ack,
        }
    }

    pub async fn init(
        sector_index: SectorIdx,
        id: u8,
        total_processes: u8,
        client: Arc<dyn RegisterClient>,
        storage: Arc<dyn SectorsManager>,
    ) -> Self {
        let value = storage.read_data(sector_index).await;
        let (timestamp, writer_id) = storage.read_metadata(sector_index).await;

        Self {
            id,
            sector_index,
            total_processes,
            client,
            storage,
            current_state: LocalState { value, timestamp, writer_id },
            data_collections: HashMap::new(),
            acknowledgments: HashSet::new(),
            operation: OperationStatus::Idle,
            operation_id: None,
            temp_read: None,
            temp_write: None,
            step: ProtocolStep::Gathering,
            callback: None,
        }
    }
}

#[async_trait::async_trait]
impl AtomicRegister for DistributedRegister {
    async fn client_command(&mut self, command: ClientRegisterCommand, callback: SuccessCallbackType) {
        if self.operation != OperationStatus::Idle {
            panic!("Cannot execute multiple operations simultaneously.");
        }

        self.operation_id = Some(Uuid::new_v4());
        self.data_collections.clear();
        self.acknowledgments.clear();

        self.operation = match command.content {
            ClientRegisterCommandContent::Read => OperationStatus::Reading(command.header.request_identifier),
            ClientRegisterCommandContent::Write { data } => {
                self.temp_write = Some(data);
                OperationStatus::Writing(command.header.request_identifier)
            }
        };

        self.callback = Some(callback);
        self.client.broadcast(Broadcast { cmd: Arc::new(self.create_read_request()) }).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        let op_id = cmd.header.msg_ident;
        let sender = cmd.header.process_identifier;

        match cmd.content {
            SystemRegisterCommandContent::ReadProc => {
                self.client.send(crate::Send {
                    cmd: Arc::new(self.create_value_response(op_id)),
                    target: sender,
                }).await;
            },
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } => {
                self.process_value(op_id, sender, LocalState { value: sector_data, timestamp, writer_id: write_rank }).await;
            },
            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                let new_state = LocalState { value: data_to_write, timestamp, writer_id: write_rank };
                if LocalState::compare_states(&self.current_state, &new_state).is_lt() {
                    self.current_state = new_state;
                    self.storage.write(self.sector_index, &self.current_state.as_tuple()).await;
                }

                self.client.send(crate::Send {
                    cmd: Arc::new(self.create_ack_response(op_id)),
                    target: sender,
                }).await;
            },
            SystemRegisterCommandContent::Ack => {
                self.process_ack(op_id, sender).await;
            },
        }
    }
}
