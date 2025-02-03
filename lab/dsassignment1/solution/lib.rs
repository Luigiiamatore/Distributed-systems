use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time;

pub trait Message: Send + 'static {}
impl<T: Send + 'static> Message for T {}

pub trait Module: Send + 'static {}
impl<T: Send + 'static> Module for T {}

/// A trait for modules capable of handling messages of type `M`.
#[async_trait::async_trait]
pub trait Handler<M: Message>: Module {
    /// Handles the message. A module must be able to access a `ModuleRef` to itself through `self_ref`.
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: M);
}

#[async_trait::async_trait]
trait Handlee<T: Module>: Message {
    async fn get_handled(self: Box<Self>, module_ref: &ModuleRef<T>, module: &mut T);
}

#[async_trait::async_trait]
impl<M: Message, T: Handler<M>> Handlee<T> for M {
    async fn get_handled(self: Box<Self>, module_ref: &ModuleRef<T>, module: &mut T) {
        module.handle(module_ref, *self).await;
    }
}

struct ShutdownModule {}

struct StopTimer {}

/// A handle returned by ModuleRef::request_tick(), can be used to stop sending further ticks.
/// You can add fields to this struct
#[derive(Clone)]
pub struct TimerHandle {
    stop_tx: Sender<StopTimer>,
}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to ModuleRef::request_tick().
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        self.stop_tx.try_send(StopTimer {}).unwrap_or_default();
    }
}

// You can add fields to this struct.
pub struct System {
    shutdown_senders: Vec<mpsc::Sender<ShutdownModule>>,
}
impl System {
    /// Registers the module in the system.
    /// Returns a ModuleRef, which can be used then to send messages to the module.
    pub async fn register_module<T: Module>(&mut self, module: T) -> ModuleRef<T> {
        let (msg_tx, mut msg_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

        let module_ref = ModuleRef {
            mod_ref: Arc::new(Mutex::new(module)),
            msg_tx,
            tick_handles: Arc::new(Mutex::new(Vec::new()))
        };
        let ref_clone = module_ref.clone();

        self.shutdown_senders.push(shutdown_tx);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => {
                        for handle in ref_clone.tick_handles.lock().await.deref_mut().drain(..) {
                            handle.stop().await;
                        }

                        break;
                    },
                    msg = msg_rx.recv() => {
                        let ref_clone2 = ref_clone.clone();
                        msg.unwrap()
                            .get_handled(&ref_clone2, ref_clone2.mod_ref.lock().await.deref_mut())
                            .await;
                    }
                }
            }
        });
        module_ref
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        Self {
            shutdown_senders: Vec::new(),
        }
    }

    /// Gracefully shuts the system down.
    pub async fn shutdown(&mut self) {
        let mut join_set = JoinSet::new();

        for tx in self.shutdown_senders.drain(..) {
            tx.try_send(ShutdownModule {}).unwrap_or_default();
            join_set.spawn(async move {
                tx.closed().await;
            });
        }

        let _ = join_set.join_all().await;
    }
}

/// A reference to a module used for sending messages.
pub struct ModuleRef<T: Module + ?Sized> {
    mod_ref: Arc<Mutex<T>>,
    msg_tx: UnboundedSender<Box<dyn Handlee<T>>>,
    tick_handles: Arc<Mutex<Vec<TimerHandle>>>,
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        if !self.msg_tx.is_closed() {
            self.msg_tx.send(Box::new(msg)).unwrap();
        }
    }
    /// Schedules a message to be sent to the module periodically with the given interval.
    /// The first tick is sent after the interval elapses.
    /// Every call to this function results in sending new ticks and does not cancel
    /// ticks resulting from previous calls.
    pub async fn request_tick<M>(&self, message: M, delay: Duration) -> TimerHandle
    where
        M: Message + Clone,
        T: Handler<M>,
    {
        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        let ref_clone = self.clone();
        
        tokio::spawn(async move {
            let mut interval = time::interval(delay);
            interval.tick().await;

            loop {
                tokio::select! {
                    biased;
                    _ = stop_rx.recv() => {
                        break;
                    },
                    _ = interval.tick() => {
                        ref_clone.send(message.clone()).await;
                    }
                }
            }
        });
        let timer_handle = TimerHandle { stop_tx: stop_tx };
        self.tick_handles.lock().await.push(timer_handle.clone());
        timer_handle
    }
}

impl<T: Module> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        Self {
            mod_ref: self.mod_ref.clone(),
            msg_tx: self.msg_tx.clone(),
            tick_handles: self.tick_handles.clone(),
        }
    }
}
