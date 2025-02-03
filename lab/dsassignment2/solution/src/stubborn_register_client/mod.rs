use crate::stubborn_register_client::stubborn_link::StubbornLink;
use crate::{Broadcast, RegisterClient, SystemCallbackType, SystemRegisterCommand};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

mod stubborn_link;
mod timer;

pub struct StubbornRegisterClient {
    connections: HashMap<u8, StubbornLink>,
    internal_channel: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>,
    local_rank: u8,
    total_processes: u8,
}

#[async_trait::async_trait]
impl RegisterClient for StubbornRegisterClient {
    async fn send(&self, msg: crate::Send) {
        if self.local_rank == msg.target {
            let callback: SystemCallbackType = Box::new(|| Box::pin(async move {}));
            self.internal_channel.send((msg.cmd.deref().clone(), callback)).unwrap();
        } else if let Some(link) = self.connections.get(&msg.target) {
            link.queue_message(msg.cmd).await;
        }
    }

    async fn broadcast(&self, msg: Broadcast) {
        for process_id in 1..=self.total_processes {
            self.send(crate::Send {
                cmd: msg.cmd.clone(),
                target: process_id,
            })
                .await;
        }
    }
}

impl StubbornRegisterClient {
    pub fn init(
        network_locations: Vec<(String, u16)>,
        hmac_key: Arc<[u8; 64]>,
        local_rank: u8,
        internal_channel: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>,
    ) -> Self {
        let mut connections = HashMap::new();

        for (idx, _) in network_locations.iter().enumerate() {
            let rank = (idx + 1) as u8;

            if rank != local_rank {
                connections.insert(
                    rank,
                    StubbornLink::new(rank, network_locations.clone(), hmac_key.clone()),
                );
            }
        }

        Self {
            connections,
            internal_channel,
            local_rank,
            total_processes: network_locations.len() as u8,
        }
    }
}
