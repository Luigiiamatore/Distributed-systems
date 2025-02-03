use crate::{Raft, RaftType, Snapshot};
use module_system::{ModuleRef, TimerHandle};
use std::time::SystemTime;

pub struct VolatileState {
    /// Reference to the Raft server that own Self as a volatile state.
    pub raft_ref: Option<ModuleRef<Raft>>,
    /// Indicates the current type of process (default: Follower).
    pub raft_type: RaftType,
    /// `TimerHandle` that handles the election timeout.
    pub election_timeout: Option<TimerHandle>,
    /// Moment of the last received heartbeat.
    pub timestamp_last_heartbeat: SystemTime,
    /// Index of highest log entry known to be committed (default: 0).
    pub committed_index: usize,
    /// Index of highest log entry applied to state machine (default: 0).
    pub last_applied: usize,
}

impl VolatileState {
    /// Returns the first configuration for VolatileState.
    pub fn init(timestamp_last_heartbeat: SystemTime, snapshot: &Option<Snapshot>, leader_snapshot: Option<Snapshot>) -> VolatileState {
        let index = if let Some(snap) = snapshot { snap.last_index } else { 0 };
        Self::setup_state(None, RaftType::Follower { leader_id: None, leader_snapshot }, None, timestamp_last_heartbeat, index, index)
    }

    /// Set up the VolatileState with given parameters.
    pub fn setup_state(
        raft_ref: Option<ModuleRef<Raft>>,
        raft_type: RaftType,
        election_timeout: Option<TimerHandle>,
        timestamp_last_heartbeat: SystemTime,
        committed_index: usize,
        last_applied: usize,
    ) -> VolatileState {
        Self {
            raft_ref,
            raft_type,
            election_timeout,
            timestamp_last_heartbeat,
            committed_index,
            last_applied,
        }
    }
}
