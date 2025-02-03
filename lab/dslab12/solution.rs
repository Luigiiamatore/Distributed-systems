use module_system::{Handler, ModuleRef, System};
use std::collections::{HashMap, VecDeque};

/// Marker trait indicating that a broadcast implementation provides
/// guarantees specified in the assignment description.
pub(crate) trait ReliableBroadcast<const N: usize> {}

#[async_trait::async_trait]
pub(crate) trait ReliableBroadcastRef<const N: usize>: Send + Sync + 'static {
    async fn send(&self, msg: Operation);
}

#[async_trait::async_trait]
impl<T, const N: usize> ReliableBroadcastRef<N> for ModuleRef<T>
where
    T: ReliableBroadcast<N> + Handler<Operation> + Send,
{
    async fn send(&self, msg: Operation) {
        self.send(msg).await;
    }
}

/// Marker trait indicating that a client implementation
/// follows specification from the assignment description.
pub(crate) trait EditorClient {}

#[async_trait::async_trait]
pub(crate) trait ClientRef: Send + Sync + 'static {
    async fn send(&self, msg: Edit);
}

#[async_trait::async_trait]
impl<T> ClientRef for ModuleRef<T>
where
    T: EditorClient + Handler<Edit> + Send,
{
    async fn send(&self, msg: Edit) {
        self.send(msg).await;
    }
}

/// Actions (edits) which can be applied to a text.
#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Debug))]
pub(crate) enum Action {
    /// Insert the character at the position.
    Insert { idx: usize, ch: char },
    /// Delete a character at the position.
    Delete { idx: usize },
    /// A _do nothing_ operation. `Nop` cannot be issued by a client.
    /// `Nop` can only be issued by a process or result from a transformation.
    Nop,
}

impl Action {
    /// Apply the action to the text.
    pub(crate) fn apply_to(&self, text: &mut String) {
        match self {
            Action::Insert { idx, ch } => {
                text.insert(*idx, *ch);
            }
            Action::Delete { idx } => {
                text.remove(*idx);
            }
            Action::Nop => {
                // Do nothing.
            }
        }
    }
}

/// Client's request to edit the text.
#[derive(Clone)]
pub(crate) struct EditRequest {
    /// Total number of operations a client has applied to its text so far.
    pub(crate) num_applied: usize,
    /// Action (edit) to be applied to a text.
    pub(crate) action: Action,
}

/// Response to a client with action (edit) it should apply to its text.
#[derive(Clone)]
pub(crate) struct Edit {
    pub(crate) action: Action,
}

#[derive(Clone, Debug)]
pub(crate) struct Operation {
    /// Rank of a process which issued this operation.
    pub(crate) process_rank: usize,
    /// Action (edit) to be applied to a text.
    pub(crate) action: Action,
}

impl Operation {
    /// Function that transform the operation `self` with respect to `concurrent`.
    pub(crate) fn transform_wrt(&self, concurrent: &Operation) -> Operation {
        match (&self.action, &concurrent.action) {
            // Insert vs Insert
            (Action::Insert { idx: p1, ch }, Action::Insert { idx: p2, .. }) => {
                if p1 < p2 || (p1 == p2 && self.process_rank < concurrent.process_rank) {
                    self.clone()
                } else {
                    self.new_insert(p1 + 1, *ch)
                }
            }
            // Delete vs Delete
            (Action::Delete { idx: p1 }, Action::Delete { idx: p2 }) => {
                if p1 < p2 {
                    self.clone()
                } else if p1 == p2 {
                    self.new_nop()
                } else {
                    self.new_delete(p1 - 1)
                }
            }
            // Insert vs Delete
            (Action::Insert { idx: p1, ch }, Action::Delete { idx: p2 }) => {
                if p1 <= p2 {
                    self.clone()
                } else {
                    self.new_insert(p1 - 1, *ch)
                }
            }
            // Delete vs Insert
            (Action::Delete { idx: p1 }, Action::Insert { idx: p2, .. }) => {
                if p1 < p2 {
                    self.clone()
                } else {
                    self.new_delete(p1 + 1)
                }
            }
            // Default case
            _ => self.clone(),
        }
    }

    /// Create a new `Insert` Operation.
    fn new_insert(&self, idx: usize, ch: char) -> Operation {
        Operation {
            process_rank: self.process_rank,
            action: Action::Insert { idx, ch },
        }
    }

    /// Create a new `Delete` Operation.
    fn new_delete(&self, idx: usize) -> Operation {
        Operation {
            process_rank: self.process_rank,
            action: Action::Delete { idx },
        }
    }

    /// Create a new `Nop` Operation.
    fn new_nop(&self) -> Operation {
        Operation {
            process_rank: self.process_rank,
            action: Action::Nop,
        }
    }
}

/// Process of the system.
pub(crate) struct Process<const N: usize> {
    /// Rank of the process.
    rank: usize,
    /// Reference to the broadcast module.
    broadcast: Box<dyn ReliableBroadcastRef<N>>,
    /// Reference to the process's client.
    client: Box<dyn ClientRef>,

    /// Log of applied operations.
    operation_log: Vec<(usize, Operation)>,
    /// Flag for EditRequest already received for this round.
    is_round_active: bool,
    /// Current round number.
    current_round: usize,
    /// Flag for every process to check if we received the operation from them in this round.
    process_received_flags: HashMap<usize, bool>,
    /// Queue of pending requests from the client.
    pending_client_requests: VecDeque<EditRequest>,
    /// Queue of pending operations received from other processes.
    pending_process_operations: VecDeque<Operation>,
}

impl<const N: usize> Process<N> {
    pub(crate) async fn new(
        system: &mut System,
        rank: usize,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
        client: Box<dyn ClientRef>,
    ) -> ModuleRef<Self> {
        let self_ref = system
            .register_module(Self {
                rank,
                broadcast,
                client,
                operation_log: Vec::new(),
                is_round_active: false,
                current_round: 0,
                process_received_flags: HashMap::new(),
                pending_client_requests: VecDeque::new(),
                pending_process_operations: VecDeque::new(),
            })
            .await;
        self_ref
    }

    /// Setup round variables.
    fn initialize_round(&mut self) {
        self.is_round_active = true;
        self.current_round += 1;
        self.reset_process_received_flags();
    }

    /// Set `process_received_flags` to false for every other process in the system.
    fn reset_process_received_flags(&mut self) {
        self.process_received_flags.clear();
        for i in 0..N {
            if i != self.rank {
                self.process_received_flags.insert(i, false);
            }
        }
    }

    /// Issue a new EditRequest from the client.
    async fn handle_edit_request(&mut self, edit_request: EditRequest) {
        // Build the Operation from the EditRequest (with rank = N because it's from the client).
        let operation = Operation {
            process_rank: N,
            action: edit_request.action.clone(),
        };

        // Transform the operation with respect to num_applied.
        let mut new_operation = operation.clone();
        for (_, logged_op) in self.operation_log.iter().skip(edit_request.num_applied) {
            new_operation = new_operation.transform_wrt(logged_op);
        }

        // Send the Edit with the EditRequest to the client
        self.client.send(Edit {
            action: new_operation.clone().action,
        }).await;

        // Set the rank as the self.rank
        new_operation.process_rank = self.rank;

        // Append the operation to the log
        self.operation_log.push((self.current_round, new_operation.clone()));
        // Send the operation to all the other processes
        self.broadcast.send(new_operation).await;
    }

    /// Issue a new Nop.
    async fn handle_nop_operation(&mut self) {
        // Build the Nop operation.
        let operation = Operation {
            process_rank: self.rank,
            action: Action::Nop,
        };

        // Send it to the client.
        self.client.send(Edit {
            action: Action::Nop,
        }).await;

        // Append the operation to the log.
        self.operation_log.push((self.current_round, operation.clone()));
        // Send the operation to all the other processes.
        self.broadcast.send(operation).await;
    }

    /// Finalize the round by changing the rank of each operation in the log and making them of this process and handling client's EditRequests in the queue.
    async fn finalize_round(&mut self) {
        self.is_round_active = false;

        // for (round, ref mut log_operation) in self.operation_log.iter_mut() {
        //     if *round == self.current_round {
        //         log_operation.process_rank = self.rank;
        //     }
        // }

        if let Some(edit_request) = self.pending_client_requests.pop_front() {
            self.initialize_round();
            self.handle_edit_request(edit_request).await;
        }
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: Operation) {
        // If it's the first operation of the round...
        if !self.is_round_active {
            // ... we start a new round...
            self.initialize_round();
            // ... and we send a Nop operation.
            self.handle_nop_operation().await;
        }

        // Get the rank of the sending process.
        let msg_rank = msg.process_rank;

        // If it's not the first Operation received from a certain process, the operation is appended and handled in the next rounds.
        if let Some(true) = self.process_received_flags.get(&msg_rank) {
            self.pending_process_operations.push_back(msg.clone());
            return;
        }

        // Mark as received from msg_rank for this round.
        self.process_received_flags.insert(msg_rank, true);

        // Transform the incoming operation wrt the other operations in the same round.
        let mut new_operation = msg.clone();
        for (round, logged_op) in self.operation_log.iter() {
            if *round == self.current_round {
                new_operation = new_operation.transform_wrt(&logged_op);
            }
        }

        // Send the new Operation to the client.
        self.client.send(Edit {
            action: new_operation.clone().action,
        }).await;

        // Append the operation to the log.
        self.operation_log.push((self.current_round, new_operation));
        // Check if the round is finished
        if self.process_received_flags.values().all(|&received| received) {
            self.finalize_round().await;
        }
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, request: EditRequest) {
        // Add the request to the client's queue
        self.pending_client_requests.push_back(request);

        // If the round has not started yet...
        if !self.is_round_active {
            // ...start the new round
            self.initialize_round();

            // Retrieve the oldest operation from client
            let edit_request = self.pending_client_requests.pop_front().unwrap();
            // Handle the EditRequest
            self.handle_edit_request(edit_request).await;

            // Handle the previously received Operations from other processes
            for enqueued_process in self.pending_process_operations.clone().iter_mut() {
                self.handle(&_self_ref, enqueued_process.clone()).await;
            }
        }
    }
}
