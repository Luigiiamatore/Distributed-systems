#[cfg(test)]
pub(crate) mod tests {
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use ntest::timeout;
    use std::time::Duration;
    use uuid::Uuid;

    use crate::solution::{
        Disable, ProcessConfig, Raft, RaftMessage, RaftMessageContent, RaftMessageHeader,
    };
    use crate::{ExecutorSender, RamStorage};
    use module_system::{Handler, ModuleRef, System};

    #[tokio::test]
    #[timeout(1500)]
    async fn single_process_transitions_to_leader() {
        // Given:
        let mut system = System::new().await;

        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let self_id = Uuid::new_v4();
        let raft = Raft::new(
            &mut system,
            ProcessConfig {
                self_id,
                election_timeout: Duration::from_millis(200),
                processes_count: 1,
            },
            Box::<RamStorage>::default(),
            Box::new(sender.clone()),
        )
        .await;
        let spy = RaftSpy::new(&mut system, raft, tx).await;
        sender.insert(self_id, Box::new(spy)).await;

        // When:
        tokio::time::sleep(Duration::from_millis(700)).await;
        let msgs = extract_messages(&mut rx);

        // Then:
        assert_has_heartbeat_from_leader(self_id, &msgs);

        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(1500)]
    async fn new_leader_is_elected_after_crash() {
        // Given:
        let mut system = System::new().await;

        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let mut ids = vec![];
        let mut rafts = vec![];
        for i in 0..4 {
            let id = Uuid::new_v4();
            ids.push(id);
            rafts.push(
                Raft::new(
                    &mut system,
                    ProcessConfig {
                        self_id: id,
                        election_timeout: Duration::from_millis(200 * (i + 1)),
                        processes_count: 4,
                    },
                    Box::<RamStorage>::default(),
                    Box::new(sender.clone()),
                )
                .await,
            );
        }
        for i in 0..3 {
            sender.insert(ids[i], Box::new(rafts[i].clone())).await;
        }
        let spy = RaftSpy::new(&mut system, rafts.last().unwrap().clone(), tx).await;
        sender.insert(*ids.last().unwrap(), Box::new(spy)).await;

        // When:
        tokio::time::sleep(Duration::from_millis(400)).await;
        rafts[0].send(Disable).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        let msgs = extract_messages(&mut rx);
        let heartbeats = heartbeats_from_leader(&msgs);

        // Then:
        let first_leader_heartbeats = heartbeats
            .iter()
            .take_while(|(_, leader_id)| *leader_id == ids[0]);
        assert!(first_leader_heartbeats.clone().count() > 0);
        assert!(first_leader_heartbeats
            .clone()
            .all(|(header, _)| header.term == 1));
        let second_leader_heartbeats = heartbeats
            .iter()
            .skip_while(|(_, leader_id)| *leader_id == ids[0]);
        assert!(second_leader_heartbeats.clone().count() > 0);
        assert!(second_leader_heartbeats
            .clone()
            .all(|(header, leader_id)| *leader_id == ids[1] && header.term == 2));
        assert_eq!(
            first_leader_heartbeats.count() + second_leader_heartbeats.count(),
            heartbeats.len()
        );

        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(1500)]
    async fn candidate_restart_election_after_timeout() {
        // Given:
        let mut system = System::new().await;

        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let mut ids = vec![];
        let mut rafts = vec![];

        for _ in 0..3 {
            let self_id = Uuid::new_v4();
            ids.push(self_id);
            rafts.push(
                Raft::new(
                    &mut system, 
                    ProcessConfig {
                        self_id,
                        election_timeout: Duration::from_millis(300),
                        processes_count: 3,
                    },
                    Box::<RamStorage>::default(),
                    Box::new(sender.clone()),
                ).await,
            );  
        }

        for i in 0..3 {
            sender.insert(ids[i], Box::new(rafts[i].clone())).await;
        }

        let spy = RaftSpy::new(&mut system, rafts[0].clone(), tx).await;
        sender.insert(ids[0], Box::new(spy)).await;

        // When:
        tokio::time::sleep(Duration::from_millis(700)).await;
        let msgs = extract_messages(&mut rx);

        // Then:
        let request_vote_msgs: Vec<_> = msgs
            .iter()
            .filter(|msg| matches!(msg.content, RaftMessageContent::RequestVote { .. }))
            .collect();

        assert!(
            request_vote_msgs.len() > 3,
            "Candidate did not restart election after failing to gather a majority of votes."
        );

        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn leader_election_with_multiple_crashes() {
        // Given:
        let mut system = System::new().await;

        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let mut ids = vec![];
        let mut rafts = vec![];

        for i in 0..5 {
            let self_id = Uuid::new_v4();
            ids.push(self_id);
            rafts.push(
                Raft::new(
                    &mut system,
                    ProcessConfig {
                        self_id,
                        election_timeout: Duration::from_millis(300 + i as u64 * 50),
                        processes_count: 5,
                    },
                    Box::<RamStorage>::default(),
                    Box::new(sender.clone()),
                ).await,
            );
        }
        for i in 0..5 {
            sender.insert(ids[i], Box::new(rafts[i].clone())).await;
        }

        let spy = RaftSpy::new(&mut system, rafts[4].clone(), tx).await;
        sender.insert(ids[4], Box::new(spy)).await;

        rafts[0].send(Disable).await;
        rafts[1].send(Disable).await;

        tokio::time::sleep(Duration::from_millis(800)).await;

        let msgs = extract_messages(&mut rx);

        // Then:
        let heartbeats = heartbeats_from_leader(&msgs);
        assert!(
            !heartbeats.is_empty(),
            "No leader was elected after multiple crashes."
        );

        let leader_id = heartbeats[0].1;
        assert!(
            heartbeats.iter().all(|(_, id)| *id == leader_id),
            "Multiple leaders detected after crashes."
        );

        assert!(
            leader_id != ids[0] && leader_id != ids[1],
            "Crashed process was incorrectly elected as leader"
        );

        let term = heartbeats[0].0.term;
        assert!(
            heartbeats.iter().all(|(header, _)| header.term == term),
            "Heartbeats have inconsistent terms"
        );

        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(2000)]
    async fn follower_becomes_candidate_after_heartbeat_timeout() {
        // Given:
        let mut system = System::new().await;

        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let mut ids = vec![];
        let mut rafts = vec![];

        for _ in 0..3 {
            let id = Uuid::new_v4();
            ids.push(id);
            rafts.push(
                Raft::new(
                    &mut system,
                    ProcessConfig {
                        self_id: id,
                        election_timeout: Duration::from_millis(300),
                        processes_count: 3,
                    },
                    Box::<RamStorage>::default(),
                    Box::new(sender.clone()),
                )
                .await,
            );
        }
        for i in 0..3 {
            sender.insert(ids[i], Box::new(rafts[i].clone())).await;
        }

        let spy = RaftSpy::new(&mut system, rafts[0].clone(), tx).await;
        sender.insert(ids[0], Box::new(spy)).await;

        // When: Do not send heartbeats from any leader
        tokio::time::sleep(Duration::from_millis(700)).await;
        let msgs = extract_messages(&mut rx);

        // Then:
        // Check if a follower has become a candidate by looking for RequestVote messages
        let request_vote_msgs: Vec<_> = msgs
            .iter()
            .filter(|msg| matches!(msg.content, RaftMessageContent::RequestVote { .. }))
            .collect();
        assert!(
            !request_vote_msgs.is_empty(),
            "No follower transitioned to candidate after missing heartbeats."
        );

        // Ensure the RequestVote messages are sent by one of the original followers
        let requestor_ids: Vec<_> = request_vote_msgs
            .iter()
            .filter_map(|msg| {
                if let RaftMessageContent::RequestVote { candidate_id } = msg.content {
                    Some(candidate_id)
                } else {
                    None
                }
            })
            .collect();
        assert!(
            requestor_ids.len() > 0 && requestor_ids.iter().all(|id| ids.contains(id)),
            "RequestVote messages were not sent by valid followers."
        );

        system.shutdown().await;
    }

    fn assert_has_heartbeat_from_leader(expected_leader: Uuid, msgs: &Vec<RaftMessage>) {
        heartbeats_from_leader(msgs)
            .iter()
            .map(|t| t.1)
            .find(|leader_id| leader_id == &expected_leader)
            .expect("No heartbeat from expected leader!");
    }

    pub(crate) fn heartbeats_from_leader(
        msgs: &Vec<RaftMessage>,
    ) -> Vec<(RaftMessageHeader, Uuid)> {
        let mut res = Vec::new();
        for msg in msgs {
            if let RaftMessageContent::Heartbeat { leader_id } = msg.content {
                res.push((msg.header, leader_id));
            }
        }
        res
    }

    pub(crate) fn extract_messages<T>(rx: &mut UnboundedReceiver<T>) -> Vec<T> {
        let mut msgs = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            msgs.push(msg);
        }
        msgs
    }

    pub(crate) struct RaftSpy {
        pub(crate) raft: ModuleRef<Raft>,
        pub(crate) tx: UnboundedSender<RaftMessage>,
    }

    impl RaftSpy {
        pub(crate) async fn new(
            system: &mut System,
            raft: ModuleRef<Raft>,
            tx: UnboundedSender<RaftMessage>,
        ) -> ModuleRef<Self> {
            system.register_module(Self { raft, tx }).await
        }
    }

    #[async_trait::async_trait]
    impl Handler<RaftMessage> for RaftSpy {
        async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
            let raft = self.raft.clone();
            let tx = self.tx.clone();

            let _ = tx.send(msg);
            raft.send(msg).await
        }
    }

    #[async_trait::async_trait]
    impl Handler<Disable> for RaftSpy {
        async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: Disable) {
            self.raft.send(msg).await;
        }
    }
}
