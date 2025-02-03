use crate::persistent_state::PersistentState;
use crate::volatile_state::VolatileState;
use bincode::{deserialize, serialize};
pub use domain::*;
use module_system::{Handler, ModuleRef, System, TimerHandle};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

mod domain;
mod persistent_state;
mod volatile_state;

#[non_exhaustive]
pub struct Raft {
    /// Server basic configuration.
    config: ServerConfig,
    /// State (both persistent and volatile) of the server.
    state: State,
    /// Map (index, sender) of indexes of pending client's requests in the log.
    pending_client_requests: HashMap<usize, UnboundedSender<ClientRequestResponse>>,
    /// Map of all sessions opened with clients.
    sessions: HashMap<Uuid, ClientSession>,
    /// Snapshot (if present) of the servers.
    snapshot: Option<Snapshot>,

    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Box<dyn RaftSender>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let (snapshot, sessions) = Self::init_snapshot(&stable_storage).await;

        let leader_snapshot: Option<Snapshot> = if let Some(leader_snapshot) = stable_storage.get("leader_snapshot").await {
            Some(deserialize(leader_snapshot.as_slice()).unwrap())
        } else { None };

        // Get the persistent state from the stable_storage.
        let persistent_state: PersistentState = stable_storage.get("persistent_state").await
            .map_or_else(
                || PersistentState::init(first_log_entry_timestamp, config.servers.clone()),
                |persistent_state| deserialize(persistent_state.as_slice()).unwrap(),
            );

        // Create the volatile_state.
        let volatile_state = VolatileState::init(SystemTime::now() - *config.election_timeout_range.start(), &snapshot, leader_snapshot);

        let state = State {
            persistent: persistent_state,
            volatile: volatile_state,
        };

        // Build the Raft reference
        let mut server = Self {
            config,
            state,
            pending_client_requests: HashMap::new(),
            sessions,
            snapshot,
            state_machine,
            stable_storage,
            message_sender,
        };

        if let Some(snapshot) = &mut server.snapshot {
            server.state_machine.initialize(snapshot.data.as_slice()).await;
        }

        let self_ref = system.register_module(server).await;

        // Send `Init` to set up the election_timer.
        self_ref.send(Init {}).await;

        self_ref
    }

    async fn init_snapshot(stable_storage: &Box<dyn StableStorage>) -> (Option<Snapshot>, HashMap<Uuid, ClientSession>) {
        match stable_storage.get("snapshot").await {
            Some(snapshot) => {
                let snapshot = deserialize::<Snapshot>(snapshot.as_slice()).unwrap();
                let session = snapshot.sessions.clone();
                (Some(snapshot), session)
            }
            None => (None, HashMap::new())
        }
    }

    /// Update the current term with `new_term`, reset `voted_for` and restart the election timeout.
    async fn update_term(&mut self, new_term: u64) {
        self.state.update_term(new_term).await;
        self.restart_election_timer().await;
        // No reliable state update called here, must be called separately.
    }

    /// Save the persistent state in the `stable_storage`.
    async fn update_persistent_state(&mut self) {
        self.stable_storage.put("persistent_state", serialize(&self.state.persistent).unwrap().as_slice()).await.unwrap();
    }

    /// Restart the election timer.
    async fn restart_election_timer(&mut self) {
        // If there is an election timer that's running, it should be stopped.
        if let Some(election_timer) = self.state.volatile.election_timeout.take() {
            election_timer.stop().await;
        }

        // Generate the duration for the election timeout.
        let election_timeout_duration = rand::thread_rng().gen_range(self.config.election_timeout_range.clone());

        // Crete the TimeHandle for the ElectionTimeout event.
        self.state.volatile.election_timeout = Some(self.state.volatile.raft_ref.clone().unwrap().request_tick(ElectionTimeout {}, election_timeout_duration).await);
    }

    /// Apply the commands with index less than the `committed_index` to the state machine of the server.
    async fn update_state_machine(&mut self) {
        // Commits every entry between the `last_applied` index and the `committed_index`.
        while self.state.volatile.last_applied < self.state.volatile.committed_index {
            self.state.volatile.last_applied += 1;
            let last_applied = self.state.volatile.last_applied;

            // Retrieve the log entry for the given index.
            let entry = self.state.persistent.get_log_entry(last_applied).clone();

            match entry.content {
                LogEntryContent::Command { data, client_id, sequence_num, lowest_sequence_num_without_response } => {
                    // If there's no session or the `lowest_sequence_num_without_response` is greater than the `sequence_num` or the session has just expired, answer with `SessionExpired`.
                    if self.sessions.get(&client_id).is_none_or(|session| sequence_num < session.lowest_sequence_num_without_response
                        || session.last_activity + self.config.session_expiration < entry.timestamp) {
                        if let Some(channel) = self.pending_client_requests.get(&last_applied) {
                            let _ = channel.send(ClientRequestResponse::CommandResponse(CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::SessionExpired,
                            }));
                        }
                        return;
                    }
                    let session = self.sessions.get_mut(&client_id).unwrap();

                    if let Some(response) = session.responses.get(&sequence_num) {
                        if let Some(channel) = self.pending_client_requests.get(&last_applied) {
                            let _ = channel.send(ClientRequestResponse::CommandResponse(CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::CommandApplied { output: response.clone() },
                            }));
                        }
                        return;
                    }

                    session.responses.retain(|&key, _| key >= session.lowest_sequence_num_without_response);
                    session.last_activity = entry.timestamp;
                    if lowest_sequence_num_without_response > session.lowest_sequence_num_without_response {
                        session.lowest_sequence_num_without_response = lowest_sequence_num_without_response;
                    }

                    let command_applied = self.state_machine.apply(data.as_slice()).await;
                    session.responses.insert(sequence_num, command_applied.clone());

                    if let Some(channel) = self.pending_client_requests.get(&last_applied) {
                        let _ = channel.send(ClientRequestResponse::CommandResponse(CommandResponseArgs {
                            client_id,
                            sequence_num,
                            content: CommandResponseContent::CommandApplied { output: command_applied },
                        }));
                    }
                }
                LogEntryContent::RegisterClient => {
                    let client_id = Uuid::from_u128(last_applied as u128);

                    self.sessions.insert(client_id, ClientSession {
                        last_activity: entry.timestamp,
                        responses: HashMap::new(),
                        lowest_sequence_num_without_response: 0,
                    });

                    if let Some(channel) = self.pending_client_requests.remove(&(last_applied)) {
                        let _ = channel.send(ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
                            content: RegisterClientResponseContent::ClientRegistered {
                                client_id,
                            }
                        }));
                    }
                }
                _ => {}
            }
        }
    }

    /// Update the `commit_index` based on how many servers replicated the commands.
    fn update_commit_index(&mut self) {
        // Only Leader can commit new entries.
        let RaftType::Leader { match_indexes, .. } = &mut self.state.volatile.raft_type else { return; };

        // For every entry in the log, with the same term as the current one, if it's replicated from the majority of the processes, it should be committed.
        for index in (self.state.volatile.committed_index + 1)..=self.state.persistent.last_index_log() {
            if self.state.persistent.get_log_entry(index).term != self.state.persistent.current_term { continue; }

            if Self::replicated_by_majority(match_indexes, index, self.config.servers.len()) {
                self.state.volatile.committed_index = index;
            } else { break; }
        }
    }

    /// Convert the server into a Follower.
    async fn convert_to_follower(&mut self,) {
        // If the server was a Leader, the heartbeat timeout should be stopped.
        if let RaftType::Leader { timer_heartbeat, .. } = &self.state.volatile.raft_type {
            timer_heartbeat.stop().await;
        }
        self.state.volatile.raft_type = RaftType::Follower { leader_id: None, leader_snapshot: None };
    }

    /// Convert the server into a Candidate.
    fn convert_to_candidate(&mut self) {
        self.state.volatile.raft_type = RaftType::Candidate { votes_received: HashSet::new() };
    }

    /// Convert the server into a Leader.
    async fn convert_to_leader(&mut self) {
        // Adding a NoOp operation to the log.
        self.state.persistent.log.push(LogEntry {
            content: LogEntryContent::NoOp,
            term: self.state.persistent.current_term,
            timestamp: SystemTime::now(),
        });
        self.update_persistent_state().await;

        // Build the maps to manage log replication between servers.
        let next_indexes = self.config.servers.iter()
            .map(|server| (server.clone(), self.state.persistent.last_index_log()))
            .collect();
        let match_indexes = self.config.servers.iter()
            .map(|server| (*server, 0))
            .collect();

        // Change the type of the server.
        self.state.volatile.raft_type = RaftType::Leader {
            timer_heartbeat: self.state.volatile.raft_ref.clone().unwrap().request_tick(
                HeartbeatTimeout {}, self.config.heartbeat_timeout,
            ).await,
            next_indexes,
            match_indexes,
            snapshot_chunks: HashMap::new(),
            heartbeat_response_received: HashSet::new(),
        };

        // Restart the election timeout and start sending
        self.restart_election_timer().await;
        // Send to every server in the cluster a heartbeat (excluded itself).
        self.send_heartbeats().await
    }

    /// A Follower increments its current term and transitions to candidate state. It votes for itself and issues `RequestVote` in parallel to each of the other servers in the cluster.
    async fn start_leader_election(&mut self) {
        // Only Candidate can start a new leader election.
        let RaftType::Candidate { votes_received } = &mut self.state.volatile.raft_type else { return; };
        votes_received.clear();

        self.update_term(self.state.persistent.current_term + 1).await;
        self.state.persistent.voted_for = Some(self.config.self_id);
        self.update_persistent_state().await;

        // Fake the reception of a granted vote from the Candidate itself.
        self.handle_vote_response(self.config.self_id, self.state.persistent.current_term, true).await;

        // Build the message `RequestVote`.
        let request_vote = RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term: self.state.persistent.current_term,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: self.state.persistent.last_index_log(),
                last_log_term: self.state.persistent.log.last().unwrap().term,
            }),
        };

        // Broadcast the request vote to all servers (except for itself).
        for server in self.config.servers.iter() {
            if *server != self.config.self_id {
                self.message_sender.send(server, request_vote.clone()).await;
            }
        }
    }

    async fn send_heartbeats(&mut self) {
        let RaftType::Leader { heartbeat_response_received, .. } = &mut self.state.volatile.raft_type else { return; };

        // The Leader inserts itself in the set of servers that answered to its heartbeat.
        heartbeat_response_received.insert(self.config.self_id);
        self.state.volatile.timestamp_last_heartbeat = SystemTime::now();

        if heartbeat_response_received.len() > self.config.servers.len() / 2 {
            heartbeat_response_received.clear();
            self.restart_election_timer().await;
        }

        // Send to every server in the cluster a heartbeat (excluded itself).
        for server in self.config.servers.clone().iter() {
            if *server != self.config.self_id {
                self.send_heartbeat_to_server(server).await;
            }
        }
    }

    async fn send_heartbeat_to_server(&mut self, server: &Uuid) {
        // Retrieve the `next_index` and `match_index` for `server`.
        let RaftType::Leader {
            next_indexes, match_indexes,
            snapshot_chunks, ..
        } = &mut self.state.volatile.raft_type else { return; };
        let next_index = next_indexes.get(server).unwrap();
        let match_index = match_indexes.get(server).unwrap();

        let message = if *next_index < self.state.persistent.index_first_entry {
            let Some(snapshot) = &self.snapshot.clone() else { panic!("No snapshot available") };

            let offset = *snapshot_chunks.get(server).unwrap();

            let last_included_index = snapshot.last_index;
            let last_included_term = snapshot.last_term;

            let last_config = if offset == 0 {
                Some(snapshot.last_config.clone())
            } else { None };
            let client_sessions = if offset == 0 {
                Some(snapshot.sessions.clone())
            } else { None };

            let (data, done) = if offset + self.config.snapshot_chunk_size < snapshot.data.len() - 1 {
                (snapshot.data[offset..(offset + self.config.snapshot_chunk_size)].to_vec(), false)
            } else {
                (snapshot.data[offset..].to_vec(), true)
            };

            RaftMessage {
                header: RaftMessageHeader { source: self.config.self_id, term: self.state.persistent.current_term },
                content: RaftMessageContent::InstallSnapshot(InstallSnapshotArgs {
                    last_included_index,
                    last_included_term,
                    last_config,
                    client_sessions,
                    offset,
                    data,
                    done,
                }),
            }
        } else {
            // Retrieve the (next) batch of entries to send to the server.
            let entries = if *next_index == *match_index + 1 {
                let mut entries = vec![];
                for i in 0..self.config.append_entries_batch_size {
                    if let Some(log_entry) = self.state.persistent.log.get(next_index + i - self.state.persistent.index_first_entry) {
                        entries.push(log_entry.clone());
                    } else { break; } // It means that there are no more entries in the log.
                }
                entries
            } else { vec![] };

            // Save the `prev_log_index` and the `prev_log_term` to send to the server.
            let prev_log_index = *next_index - 1;
            let prev_log_term = if prev_log_index < self.state.persistent.index_first_entry {
                let Some(snapshot) = &self.snapshot else { return; };
                snapshot.last_term
            } else {
                self.state.persistent.get_log_entry(prev_log_index).term
            };

            // Build the message.
            RaftMessage {
                header: RaftMessageHeader {
                    source: self.config.self_id,
                    term: self.state.persistent.current_term,
                },
                content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.state.volatile.committed_index,
                }),
            }
        };

        // Send the message to the server.
        self.message_sender.send(server, message).await;
    }

    /// Send `RequestVoteResponse` to source, granting the vote based on `vote_granted`.
    async fn send_request_vote_response(&mut self, source: Uuid, vote_granted: bool) {
        self.message_sender.send(
            &source,
            RaftMessage {
                header: RaftMessageHeader { source: self.config.self_id, term: self.state.persistent.current_term },
                content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs { vote_granted }),
            },
        ).await;
    }

    /// Send `AppendEntriesResponse` to source to notify the outcome of an `AppendEntries`.
    async fn send_append_entries_response(&mut self, source: Uuid, success: bool, last_verified_log_index: usize) {
        self.message_sender.send(&source, RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term: self.state.persistent.current_term,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success,
                last_verified_log_index,
            }),
        }).await;
    }

    /// Handle the `AppendEntries` message, invoked by leader to replicate log entries or used as heartbeat.
    async fn handle_append_entries(&mut self, msg_source: Uuid, msg_term: u64, prev_log_index: usize, prev_log_term: u64, entries: Vec<LogEntry>, leader_commit: usize) {
        // If the term of the message is outdated, refuse the AppendEntries.
        if msg_term < self.state.persistent.current_term {
            self.send_append_entries_response(msg_source, false, prev_log_index + entries.len()).await;
            return;
        }

        // AppendEntries is like a heartbeat, so it restarts the election timer.
        self.restart_election_timer().await;
        // Update `timestamp_last_heartbeat`.
        self.state.volatile.timestamp_last_heartbeat = SystemTime::now();

        // If the server is a Candidate (and because of up-to-date `msg_term`), it becomes a Follower.
        // else if it is a Follower, make sure that the leader is the correct one.
        if self.state.is_candidate() {
            self.state.volatile.raft_type = RaftType::Follower { leader_id: Some(msg_source), leader_snapshot: None }
        } else if let RaftType::Follower { ref mut leader_id, ..} = self.state.volatile.raft_type {
            *leader_id = Some(msg_source);
        }

        // If the log at position `prev_log_index` has a different term than the expected one, send a success = false.
        if !self.state.persistent.matches(prev_log_index, prev_log_term) {
            let Some(snapshot) = &self.snapshot else {
                self.send_append_entries_response(msg_source, false, prev_log_index + entries.len()).await;
                return;
            };

            if snapshot.last_index != prev_log_index || snapshot.last_term != prev_log_term {
                self.send_append_entries_response(msg_source, false, prev_log_index + entries.len()).await;
                return;
            }
        }

        for i in 0..entries.len() {
            let actual_index = prev_log_index + 1 + i;
            if self.state.persistent.log.get(actual_index).is_some_and(|log_entry| log_entry.term != entries[i].term) {
                self.state.persistent.log.truncate(actual_index);
            }
            if self.state.persistent.log.get(actual_index).is_none() {
                self.state.persistent.log.push(entries[i].clone());
            }
        }
        self.update_persistent_state().await;

        if self.state.volatile.committed_index < leader_commit {
            self.state.volatile.committed_index = cmp::min(leader_commit, self.state.persistent.last_index_log());
            self.update_state_machine().await;
        }

        self.send_append_entries_response(msg_source, true, prev_log_index + entries.len()).await;
    }

    /// Handle the `AppendEntriesResponse` message.
    async fn handle_append_entries_response(&mut self, msg_source: Uuid, msg_term: u64, success: bool, last_verified_log_index: usize) {
        // Outdated messages are not important.
        if msg_term < self.state.persistent.current_term { return; }

        // Only Leader can handle these messages.
        let RaftType::Leader {
            next_indexes,
            match_indexes,
            heartbeat_response_received,
            ..
        } = &mut self.state.volatile.raft_type else { return; };

        // If the request isn't successful, try with the previous entry in the log.
        if !success {
            *next_indexes.get_mut(&msg_source).unwrap() -= 1;
            self.send_heartbeat_to_server(&msg_source).await;
            return;
        }

        // If the request is successful, go on with the entries.
        next_indexes.insert(msg_source, last_verified_log_index + 1);
        match_indexes.insert(msg_source, last_verified_log_index);

        // for index in (self.state.volatile.committed_index + 1)..=last_verified_log_index {
        //     if self.state.persistent.get_log_entry(index).term != self.state.persistent.current_term { continue; }
        //
        //     if Self::replicated_by_majority(match_indexes, index, self.config.servers.len()) {
        //         self.state.volatile.committed_index = index;
        //     } else { break; }
        // }

        let can_send = *next_indexes.get(&msg_source).unwrap() <= self.state.persistent.last_index_log();

        heartbeat_response_received.insert(msg_source);
        if heartbeat_response_received.len() > self.config.servers.len() / 2 {
            heartbeat_response_received.clear();
            self.restart_election_timer().await;
        }

        self.update_commit_index();
        self.update_state_machine().await;

        if can_send {
            self.send_heartbeat_to_server(&msg_source).await;
        }
    }

    /// Handle the `RequestVote` message, invoked by candidates to gather votes.
    async fn handle_request_vote(&mut self, term: u64, source: Uuid, last_log_index: usize, last_log_term: u64) {
        // If the RequestVote is outdated, vote `false`.
        if term < self.state.persistent.current_term {
            self.send_request_vote_response(source, false).await;
            return;
        }

        // If `voted_for` is `None` or equal to `source`, and candidate's log is at least up-to-date as receiver's log, vote `true`.
        if (self.state.persistent.voted_for.is_none()
            || self.state.persistent.voted_for.is_some_and(|voted_for| voted_for == source))
            && (self.state.persistent.is_other_up_to_date(last_log_term, last_log_index)) {
            self.state.persistent.voted_for = Some(source);
            self.update_persistent_state().await;
            self.send_request_vote_response(source, true).await;
            return;
        }

        self.send_request_vote_response(source, false).await;
    }

    /// Handle the `RequestVoteResponse`, received after sending `RequestVote`.
    async fn handle_vote_response(&mut self, source: Uuid, msg_term: u64, vote_granted: bool) {
        // If the vote is `false` or if the answer is from another term, return.
        if !vote_granted || msg_term != self.state.persistent.current_term { return; }

        // The server (a Candidate) add the source to the votes received.
        let RaftType::Candidate { ref mut votes_received } = &mut self.state.volatile.raft_type else { return; };
        votes_received.insert(source);

        // Check if the Candidate reached the majority of responses.
        if votes_received.len() > (self.config.servers.len() / 2) {
            // If so, convert it to a Leader.
            self.convert_to_leader().await;
        }
    }

    /// Handle the `InstallSnapshot` request from the Leader.
    async fn handle_install_snapshot(&mut self, msg_source: Uuid, msg_term: u64, last_included_index: usize, last_included_term: u64, last_config: Option<HashSet<Uuid>>, client_sessions: Option<HashMap<Uuid, ClientSession>>, offset: usize, mut data: Vec<u8>, done: bool) {
        if msg_term < self.state.persistent.current_term {
            self.message_sender.send(&msg_source, self.gen_snapshot_response(last_included_index, offset)).await;
            return;
        }

        // Update heartbeat timestamp and reset election timer.
        self.restart_election_timer().await;
        self.state.volatile.timestamp_last_heartbeat = SystemTime::now();

        // Convert to follower if necessary.
        if self.state.is_candidate() {
            self.convert_to_follower().await;
        } else if let RaftType::Follower { leader_id, .. } = &mut self.state.volatile.raft_type {
            *leader_id = Some(msg_source);
        }

        // Ensure the current node is a follower.
        let RaftType::Follower { leader_id, leader_snapshot } = &mut self.state.volatile.raft_type else { return; };

        // Handle snapshot initialization or continuation.
        if offset == 0 {
            *leader_snapshot = Some(Snapshot {
                data,
                last_index: last_included_index,
                last_term: last_included_term,
                last_config: last_config.unwrap(),
                sessions: client_sessions.unwrap(),
            });
            self.stable_storage.put("leader_snapshot", serialize(&leader_snapshot.clone().unwrap()).unwrap().as_slice()).await.unwrap();
        } else {
            let Some(new_snapshot) = leader_snapshot else { return; };
            new_snapshot.data.truncate(offset);
            new_snapshot.data.append(&mut data);
            self.stable_storage.put("leader_snapshot", serialize(new_snapshot).unwrap().as_slice()).await.unwrap();
        }
        // Persist snapshot to file.
        //self.stable_storage.put("leader_snapshot", serialize(&leader_snapshot).unwrap().as_slice()).await.unwrap();

        // If snapshot transfer is not complete, respond and return.
        if !done {
            self.message_sender.send(&msg_source, self.gen_snapshot_response(last_included_index, offset)).await;
            return;
        }

        let Some(new_snapshot) = leader_snapshot else { return; };

        // Determine the last committed index and term.
        let (last_index, last_term) = match &self.snapshot {
            Some(current_snapshot) if new_snapshot.last_index > current_snapshot.last_index => {
                (current_snapshot.last_index, current_snapshot.last_term)
            }
            _ => {
                self.snapshot = Some(new_snapshot.clone());
                *leader_id = Some(msg_source);
                (new_snapshot.last_index, new_snapshot.last_term)
            }
        };

        // Apply snapshot to log and state machine.
        if self.state.persistent.matches(last_index, last_term) {
            self.state.persistent.log = self.state.persistent.log.split_off(last_index);
            self.state.persistent.index_first_entry = last_index + 1;
        } else {
            self.state.persistent.log.clear();
            self.state.persistent.index_first_entry = last_index + 1;
            self.state_machine.initialize(self.snapshot.clone().unwrap().data.as_slice()).await;
            self.state.volatile.committed_index = last_index;
            self.state.volatile.last_applied = last_index;
        }

        // Persist state and acknowledge snapshot.
        self.update_persistent_state().await;
        self.message_sender.send(&msg_source, self.gen_snapshot_response(last_included_index, offset)).await;
    }

    async fn handle_install_snapshot_response(&mut self, msg_source: Uuid, msg_term: u64, last_included_index: usize, offset: usize) {
        if msg_term < self.state.persistent.current_term { return; }

        let RaftType::Leader {
            next_indexes,
            match_indexes,
            snapshot_chunks,
            heartbeat_response_received,
            ..
        } = &mut self.state.volatile.raft_type else { return; };

        if let Some(next_snapshot_chunk) = snapshot_chunks.get_mut(&msg_source) {
            let Some(snapshot) = &self.snapshot else { return; };

            if snapshot.last_index == last_included_index && *next_snapshot_chunk == offset {
                let new_offset = *next_snapshot_chunk + self.config.snapshot_chunk_size;

                let mut can_send = false;
                if new_offset > snapshot.data.len() {
                    snapshot_chunks.remove(&msg_source);
                    *next_indexes.get_mut(&msg_source).unwrap() = last_included_index + 1;
                    *match_indexes.get_mut(&msg_source).unwrap() = last_included_index;

                    if *next_indexes.get(&msg_source).unwrap() <= self.state.persistent.last_index_log() {
                        can_send = true;
                    }
                } else {
                    *next_snapshot_chunk = new_offset;
                    can_send = true;
                }

                heartbeat_response_received.insert(msg_source);
                if heartbeat_response_received.len() > self.config.servers.len() / 2 {
                    heartbeat_response_received.clear();
                    self.restart_election_timer().await;
                }

                if can_send {
                    self.send_heartbeat_to_server(&msg_source).await;
                }
            }
        }
    }

    fn gen_snapshot_response(&self, last_included_index: usize, offset: usize) -> RaftMessage {
        RaftMessage {
            header: RaftMessageHeader { source: self.config.self_id, term: self.state.persistent.current_term },
            content: RaftMessageContent::InstallSnapshotResponse(InstallSnapshotResponseArgs {
                last_included_index,
                offset,
            }),
        }
    }

    /// Return `true` if the entry linked to `index` is replicated from the majority of the cluster, `false` otherwise.
    fn replicated_by_majority(match_indexes: &HashMap<Uuid, usize>, index: usize, cluster_size: usize) -> bool {
        match_indexes.values().filter(|m| **m >= index).count() + 1 > (cluster_size / 2)
    }

    fn take_last_config(&self) -> HashSet<Uuid> {
        let mut start_index: usize = 0;
        let mut last_config = if let Some(snapshot) = &self.snapshot {
            start_index = snapshot.last_index;
            snapshot.last_config.clone()
        } else {
            self.config.servers.clone()
        };

        for i in start_index..self.state.volatile.committed_index {
            let log_entry = self.state.persistent.get_log_entry(i).clone();
            if let LogEntry { content: LogEntryContent::Configuration { servers }, .. } = log_entry {
                last_config = servers;
            }
        }

        last_config

    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, _msg: Init) {
        self.state.volatile.raft_ref = Some(self_ref.clone());
        self.restart_election_timer().await;
    }
}

#[async_trait::async_trait]
impl Handler<HeartbeatTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: HeartbeatTimeout) {
        if let RaftType::Leader { .. } = self.state.volatile.raft_type {
            self.send_heartbeats().await;
        }
    }
}

#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: ElectionTimeout) {
        // If the server is a Follower, it becomes a Candidate.
        if self.state.is_follower() {
            self.convert_to_candidate();
        }

        // If the server is a candidate, it starts a new leader election. If it's a Leader, it becomes a Follower.
        if self.state.is_candidate() {
            self.start_leader_election().await;
        } else if self.state.is_leader() {
            self.convert_to_follower().await;
            self.restart_election_timer().await;
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        if let RaftMessageContent::RequestVote(..) = msg.content {
            if SystemTime::now().duration_since(self.state.volatile.timestamp_last_heartbeat).unwrap() < *self.config.election_timeout_range.start() { return; }
        }

        // If the server is not up to date, convert it into follower and update the term
        if self.state.persistent.current_term < msg.header.term {
            self.convert_to_follower().await;
            self.update_term(msg.header.term).await;
            self.update_persistent_state().await;
        } // From now on, both the servers have the same term.

        // Check the content of the message.
        match msg.content {
            RaftMessageContent::AppendEntries(AppendEntriesArgs { prev_log_index, prev_log_term, entries, leader_commit }) =>
                self.handle_append_entries(msg.header.source, msg.header.term, prev_log_index, prev_log_term, entries, leader_commit).await,
            RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs { success, last_verified_log_index }) =>
                self.handle_append_entries_response(msg.header.source, msg.header.term, success, last_verified_log_index).await,
            RaftMessageContent::RequestVote(RequestVoteArgs { last_log_index, last_log_term }) =>
                self.handle_request_vote(msg.header.term, msg.header.source, last_log_index, last_log_term).await,
            RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs { vote_granted }) =>
                self.handle_vote_response(msg.header.source, msg.header.term, vote_granted).await,
            RaftMessageContent::InstallSnapshot(InstallSnapshotArgs { last_included_index, last_included_term, last_config, client_sessions, offset, data, done }) =>
                self.handle_install_snapshot(msg.header.source, msg.header.term, last_included_index, last_included_term, last_config, client_sessions, offset, data, done).await,
            RaftMessageContent::InstallSnapshotResponse(InstallSnapshotResponseArgs { last_included_index, offset }) =>
                self.handle_install_snapshot_response(msg.header.source, msg.header.term, last_included_index, offset).await
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        match msg.content {
            ClientRequestContent::Command { command, client_id, sequence_num, lowest_sequence_num_without_response } => {
                // If the server is a Leader...
                if self.state.is_leader() {
                    // We build the LogEntry
                    let log_entry = LogEntry {
                        content: LogEntryContent::Command {
                            data: command,
                            client_id,
                            sequence_num,
                            lowest_sequence_num_without_response,
                        },
                        term: self.state.persistent.current_term,
                        timestamp: SystemTime::now(),
                    };

                    self.state.persistent.log.push(log_entry);
                    self.update_commit_index();
                    self.pending_client_requests.insert(self.state.persistent.last_index_log(), msg.reply_to);
                    self.update_persistent_state().await;

                    // Send to every server in the cluster a heartbeat (excluded itself).
                    self.send_heartbeats().await;
                } else {
                    // If the server is a Follower, send the leader_id, else send None.
                    let leader_hint = if let RaftType::Follower { leader_id, .. } = self.state.volatile.raft_type {
                        leader_id
                    } else { None };

                    _ = msg.reply_to.send(ClientRequestResponse::CommandResponse(CommandResponseArgs {
                        client_id,
                        sequence_num,
                        content: CommandResponseContent::NotLeader { leader_hint },
                    }));
                }
            }
            ClientRequestContent::RegisterClient => {
                if self.state.is_leader() {
                    let entry = LogEntry {
                        content: LogEntryContent::RegisterClient,
                        term: self.state.persistent.current_term,
                        timestamp: SystemTime::now(),
                    };

                    self.state.persistent.log.push(entry);
                    self.pending_client_requests.insert(self.state.persistent.last_index_log(), msg.reply_to);
                    self.update_persistent_state().await;

                    // Send to every server in the cluster a heartbeat (excluded itself).
                    self.send_heartbeats().await;
                } else {
                    let leader_id = if let RaftType::Follower { leader_id, .. } = self.state.volatile.raft_type {
                        leader_id
                    } else {
                        None
                    };

                    let response = ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
                        content: RegisterClientResponseContent::NotLeader { leader_hint: leader_id },
                    });

                    _ = msg.reply_to.send(response);
                }
            }
            ClientRequestContent::Snapshot => {
                if let Some(snapshot) = &self.snapshot {
                    if snapshot.last_index == self.state.volatile.committed_index {
                        let _ = msg.reply_to.send(ClientRequestResponse::SnapshotResponse(SnapshotResponseArgs {
                            content: SnapshotResponseContent::NothingToSnapshot {
                                last_included_index: self.state.volatile.committed_index,
                            },
                        }));
                        return;
                    }
                }

                let data = self.state_machine.serialize().await;
                let last_index = self.state.volatile.committed_index;
                let last_term = self.state.persistent.get_log_entry(last_index).term;
                let last_config = self.take_last_config();
                self.snapshot = Some(Snapshot {
                    data,
                    last_index,
                    last_term,
                    last_config,
                    sessions: self.sessions.clone(),
                });

                self.stable_storage.put("snapshot", serialize(&self.snapshot.clone().unwrap()).unwrap().as_slice()).await.unwrap();

                self.state.persistent.log = self.state.persistent.log.split_off(last_index + 1);
                self.state.persistent.index_first_entry = last_index + 1;
                self.update_persistent_state().await;

                if let RaftType::Leader { snapshot_chunks, .. } = &mut self.state.volatile.raft_type {
                    for server in self.config.servers.iter() {
                        if *server != self.config.self_id {
                            snapshot_chunks.insert(server.clone(), 0);
                        }
                    }
                }

                let _ = msg.reply_to.send(ClientRequestResponse::SnapshotResponse(SnapshotResponseArgs {
                    content: SnapshotResponseContent::SnapshotCreated { last_included_index: last_index },
                }));
            }
            ClientRequestContent::AddServer { .. } => unimplemented!("Cluster membership changes omitted"),
            ClientRequestContent::RemoveServer { .. } => unimplemented!("Cluster membership changes omitted")
        }
    }
}

struct State {
    /// Persistent state that will be saved every time there's a modification.
    persistent: PersistentState,
    /// Volatile state, every information that won't be saved.
    volatile: VolatileState,
}

impl State {
    /// Return `true` if `self` is a Follower, `false` otherwise.
    fn is_follower(&self) -> bool {
        matches!(self.volatile.raft_type, RaftType::Follower { .. })
    }

    /// Return `true` if `self` is a Candidate, `false` otherwise.
    fn is_candidate(&self) -> bool {
        matches!(self.volatile.raft_type, RaftType::Candidate { .. })
    }

    /// Return `true` if `self` is a Leader, `false` otherwise.
    fn is_leader(&self) -> bool {
        matches!(self.volatile.raft_type, RaftType::Leader { .. })
    }

    /// Update the current term with `new_term` and reset `voted_for`.
    async fn update_term(&mut self, new_term: u64) {
        assert!(self.persistent.current_term < new_term);
        self.persistent.current_term = new_term;
        self.persistent.voted_for = None;
    }
}

/// Enum representing the type of server in the Raft system.
enum RaftType {
    Leader {
        timer_heartbeat: TimerHandle,
        next_indexes: HashMap<Uuid, usize>,
        match_indexes: HashMap<Uuid, usize>,
        snapshot_chunks: HashMap<Uuid, usize>,
        heartbeat_response_received: HashSet<Uuid>,
    },
    Candidate { votes_received: HashSet<Uuid> },
    Follower {
        leader_id: Option<Uuid>,
        leader_snapshot: Option<Snapshot>,
    },
}

/// Message a process sends to itself to initialize the `self_ref` attribute and then start the election timer.
struct Init {}

/// Message that a Leader receives when it has to send again Heartbeats (AppendEntries) to the other processes.
#[derive(Clone)]
struct HeartbeatTimeout {}

/// Message that a process receives when it hadn't received Heartbeats (AppendEntries) for a fixed amount of time.
#[derive(Clone)]
struct ElectionTimeout {}


#[derive(Serialize, Deserialize, Clone)]
struct Snapshot {
    data: Vec<u8>,
    last_index: usize,
    last_term: u64,
    last_config: HashSet<Uuid>,
    sessions: HashMap<Uuid, ClientSession>,
}