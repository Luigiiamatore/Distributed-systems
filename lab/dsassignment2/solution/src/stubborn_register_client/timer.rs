use crate::SystemRegisterCommand;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::time;

struct TerminateTimer {}

pub struct TimerHandle {
    termination_sender: Sender<TerminateTimer>,
}

impl TimerHandle {
    pub async fn stop(self) {
        self.termination_sender.try_send(TerminateTimer {}).unwrap();
    }

    pub fn start_timer(
        message: Arc<SystemRegisterCommand>,
        sender: UnboundedSender<Arc<SystemRegisterCommand>>,
    ) -> Self {
        let (termination_sender, mut termination_receiver) = mpsc::channel(1);

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(250));

            loop {
                tokio::select! {
                    _ = termination_receiver.recv() => break,
                    _ = interval.tick() => sender.send(message.clone()).unwrap(),
                }
            }
        });

        TimerHandle {
            termination_sender,
        }
    }
}
