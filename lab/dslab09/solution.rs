use module_system::{Handler, ModuleRef};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

// Enum sui tipi di prodotto che posso salvare
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub(crate) enum ProductType {
    Electronics,
    Toys,
    Books,
}

// Messaggio del TM contenente il sender e il contenuto del messaggio
#[derive(Clone)]
pub(crate) struct StoreMsg {
    sender: ModuleRef<DistributedStore>,
    content: StoreMsgContent,
}

// Enum sui tipi di messaggio da parte del TM
#[derive(Clone, Debug)]
pub(crate) enum StoreMsgContent {
    /// Transaction Manager initiates voting for the transaction.
    RequestVote(Transaction),
    /// If every process is ok with transaction, TM issues commit.
    Commit,
    /// System-wide abort.
    Abort,
}

// Messaggio di risposta verso il TM
#[derive(Clone)]
pub(crate) struct NodeMsg {
    content: NodeMsgContent,
}

// Enum sul contenuto del messaggio di risposta verso il TM
#[derive(Clone, Debug)]
pub(crate) enum NodeMsgContent {
    /// Process replies to TM whether it can/cannot commit the transaction.
    RequestVoteResponse(TwoPhaseResult),
    /// Process acknowledges to TM committing/aborting the transaction.
    FinalizationAck,
}

pub(crate) struct TransactionMessage {
    /// Request to change price.
    pub(crate) transaction: Transaction,

    /// Called after 2PC completes (i.e., the transaction was decided to be
    /// committed/aborted by DistributedStore). This must be called after responses
    /// from all processes acknowledging commit or abort are collected.
    pub(crate) completed_callback:
        Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
}

// Possibili risultati della 2PC
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TwoPhaseResult {
    Ok,
    Abort,
}

// Struct che definisce da cosa è costituito un prodotto
#[derive(Copy, Clone, Debug)]
pub(crate) struct Product {
    pub(crate) identifier: Uuid,
    pub(crate) pr_type: ProductType,
    pub(crate) price: u64,
}

// Struct con informazioni sul tipo di prodotto di cui voglio cambiare il prezzo
#[derive(Copy, Clone, Debug)]
pub(crate) struct Transaction {
    pub(crate) pr_type: ProductType,
    pub(crate) shift: i32,
}

// Query sul prodotto che restituisce il prezzo di quel prodotto
#[derive(Debug)]
pub(crate) struct ProductPriceQuery {
    pub(crate) product_ident: Uuid,
    pub(crate) result_sender: Sender<ProductPrice>,
}

// Prezzo del prodotto (Option perché può essere anche `None`)
#[derive(Copy, Clone, Debug)]
pub(crate) struct ProductPrice(pub(crate) Option<u64>);

/// Message which disables a node. Used for testing.
pub(crate) struct Disable;

#[derive(PartialEq, Eq)]
enum DistributedStoreStatus {
    Ready,
    Pending,
    Commit
}

#[derive(PartialEq, Eq)]
enum NodeStatus {
    Ready,
    Busy
}

/// DistributedStore.
/// This structure serves as TM.
// Add any fields you need.
pub(crate) struct DistributedStore {
    nodes: Vec<ModuleRef<Node>>,
    completed_callback: Option<Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>,
    reply_counter: usize,
    status: DistributedStoreStatus,
    transaction_result: Option<TwoPhaseResult>
}

impl DistributedStore {
    pub(crate) fn new(nodes: Vec<ModuleRef<Node>>) -> Self {
        Self {
            nodes,
            completed_callback: None,
            reply_counter: 0,
            status: DistributedStoreStatus::Ready,
            transaction_result: None
        }
    }
}

/// Node of DistributedStore.
/// This structure serves as a process of the distributed system.
// Add any fields you need.
pub(crate) struct Node {
    products: Vec<Product>,
    pending_transaction: Option<Transaction>,
    enabled: bool,
    status: NodeStatus
}

impl Node {
    pub(crate) fn new(products: Vec<Product>) -> Self {
        Self {
            products,
            pending_transaction: None,
            enabled: true,
            status: NodeStatus::Ready
        }
    }
}

#[async_trait::async_trait]
impl Handler<NodeMsg> for DistributedStore {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: NodeMsg) {
        match self.status {
            DistributedStoreStatus::Pending => {
                if let NodeMsgContent::RequestVoteResponse(two_phase_result) = msg.content {
                    // after receiving the first `Abort`, the TM should abort the transaction
                    if two_phase_result == TwoPhaseResult::Abort {
                        self.transaction_result = Some(TwoPhaseResult::Abort);
                    }

                    // number of answers from node increased by one
                    self.reply_counter += 1;

                    // if every nodes had answered...
                    if self.reply_counter == self.nodes.len() {
                        self.reply_counter = 0;
                        self.status = DistributedStoreStatus::Commit;

                        // ... i send them messages with the `Commit` or `Abort` message
                        for node in self.nodes.iter() {
                            let content = if self.transaction_result.unwrap() == TwoPhaseResult::Ok {
                                StoreMsgContent::Commit
                            } else {
                                StoreMsgContent::Abort
                            };
                            
                            node.send(StoreMsg {
                                sender: self_ref.clone(),
                                content,
                            })
                            .await;
                        }
    
                    }
                }
            }
            DistributedStoreStatus::Commit => {
                if let NodeMsgContent::FinalizationAck = msg.content {
                    self.reply_counter += 1;

                    if self.reply_counter == self.nodes.len() {            
                        let callback = self.completed_callback.take().unwrap();
                        callback(self.transaction_result.unwrap()).await;

                        self.completed_callback = None;
                        self.reply_counter = 0;
                        self.status = DistributedStoreStatus::Ready;
                        self.transaction_result = None;
                    }
                }
            }
            _ => {}   
        }
    }
}

#[async_trait::async_trait]
impl Handler<StoreMsg> for Node {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: StoreMsg) {
        if self.enabled {
            match msg.content {
                StoreMsgContent::RequestVote(transaction) => {
                    if self.status == NodeStatus::Ready {
                        if transaction.shift < 0 {
                            for product in self.products.iter() {
                                if product.pr_type == transaction.pr_type && product.price <= (-transaction.shift) as u64 {
                                    msg.sender.send(NodeMsg {
                                        content: NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Abort)
                                    }).await;
                                    return
                                }
                            }
                        }
    
                        msg.sender.send(NodeMsg {
                            content: NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Ok)
                        }).await;
    
                        self.pending_transaction = Some(transaction);
                        self.status = NodeStatus::Busy;
                    }
                },
                StoreMsgContent::Commit => {
                    if self.status == NodeStatus::Busy {
                        let shift = self.pending_transaction.unwrap().shift;

                        for product in self.products.iter_mut() {
                            if product.pr_type == self.pending_transaction.unwrap().pr_type {
                                if shift < 0 {
                                    product.price -= (-shift) as u64;
                                } else {
                                    product.price += shift as u64;
                                }
                            }
                        }

                        msg.sender.send(NodeMsg {
                            content: NodeMsgContent::FinalizationAck
                        }).await;

                        self.pending_transaction = None;
                        self.status = NodeStatus::Ready;
                    }
                },
                StoreMsgContent::Abort => {
                    msg.sender.send(NodeMsg {
                        content: NodeMsgContent::FinalizationAck
                    }).await;

                    self.pending_transaction = None;
                    self.status = NodeStatus::Ready;
                },
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ProductPriceQuery> for Node {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: ProductPriceQuery) {
        if self.enabled {
            for product in self.products.iter() {
                if msg.product_ident == product.identifier {
                    msg.result_sender.send(ProductPrice{ 0: Some(product.price) }).unwrap();
                    return;
                }
            }
            msg.result_sender.send(ProductPrice { 0: None }).unwrap();
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: Disable) {
        self.enabled = false;
    }
}

#[async_trait::async_trait]
impl Handler<TransactionMessage> for DistributedStore {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: TransactionMessage) {
        if self.status == DistributedStoreStatus::Ready {
            self.status = DistributedStoreStatus::Pending;
            self.transaction_result = Some(TwoPhaseResult::Ok);
            self.completed_callback = Some(msg.completed_callback);

            for node in self.nodes.iter() {
                node.send(StoreMsg {
                    sender: self_ref.clone(),
                    content: StoreMsgContent::RequestVote(msg.transaction)
                }).await;
            }
        }
    }
}
