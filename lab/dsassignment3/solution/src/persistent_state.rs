use crate::{LogEntry, LogEntryContent};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct PersistentState {
    /// Latest term server has seen (default: 0).
    pub current_term: u64,
    /// Candidate ID that received vote in current term (or None).
    pub voted_for: Option<Uuid>,
    /// The log of operations (at creation add a LogEntryContent::Configuration with the list of servers).
    pub log: Vec<LogEntry>,
    /// Index related to the first entry of the log.
    pub index_first_entry: usize,
}

impl PersistentState {
    /// Returns the first configuration for PersistentState.
    pub fn init(timestamp: SystemTime, servers: HashSet<Uuid>) -> PersistentState {
        let current_term = 0;
        let voted_for = None;
        let log = vec![LogEntry {
            content: LogEntryContent::Configuration { servers },
            term: 0,
            timestamp,
        }];
        let index_first_entry = 0;

        Self::setup_state(current_term, voted_for, log, index_first_entry)
    }

    /// Set up the PersistentState with given parameters.
    pub fn setup_state(
        current_term: u64,
        voted_for: Option<Uuid>,
        log: Vec<LogEntry>,
        index_first_entry: usize,
    ) -> PersistentState {
        Self {
            current_term,
            voted_for,
            log,
            index_first_entry,
        }
    }

    pub fn last_index_log(&self) -> usize {
        self.index_first_entry + self.log.len() - 1
    }

    pub fn get_log_entry(&self, index: usize) -> LogEntry {
        self.log[index - self.index_first_entry].clone()
    }

    /// Check if the other log is up to date at least as compared to the internal one.
    pub fn is_other_up_to_date(&self, other_term: u64, other_idx: usize) -> bool {
        (other_term >= self.log.last().unwrap().term)
            && (other_idx >= self.index_first_entry + self.log.len() - 1)
    }

    /// Check if there exists an entry in the log with `prev_log_index` and if there's one, check if the entry have the same term as `prev_log_term`.
    pub fn matches(&self, prev_log_index: usize, prev_log_term: u64) -> bool {
        self.log
            .get(prev_log_index)
            .map_or(false, |entry| entry.term == prev_log_term)
    }
}
