#[cfg(test)]
mod tests {
    use ntest::timeout;
    use tokio::sync::oneshot::{channel, Receiver, Sender};

    use crate::solution::{
        DistributedStore, Node, Product, ProductPrice, ProductPriceQuery, ProductType, Transaction,
        TransactionMessage, TwoPhaseResult,
    };
    use module_system::System;
    use uuid::Uuid;

    #[tokio::test]
    #[timeout(300)]
    async fn transaction_with_two_nodes_completes() {
        // Given:
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = channel();
        let products = vec![Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: 180,
        }];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let node1 = system.register_module(Node::new(products)).await;
        let distributed_store = system
            .register_module(DistributedStore::new(vec![node0, node1]))
            .await;

        // When:
        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: -50,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).unwrap();
                    })
                }),
            })
            .await;

        // Then:
        assert_eq!(TwoPhaseResult::Ok, transaction_done_rx.await.unwrap());
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(300)]
    async fn abort_transaction_due_to_negative_price() {
        // Given:
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = channel();
        let products = vec![Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: 180,
        }];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let node1 = system.register_module(Node::new(products)).await;
        let distributed_store = system
            .register_module(DistributedStore::new(vec![node0, node1]))
            .await;

        // When:
        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: -180,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).unwrap();
                    })
                }),
            })
            .await;

        // Then:
        assert_eq!(TwoPhaseResult::Abort, transaction_done_rx.await.unwrap());
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(300)]
    async fn abort_transaction_due_to_only_one_negative_price() {
        // Given:
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = channel();
        let products1 = vec![
            Product {
                identifier: Uuid::new_v4(),
                pr_type: ProductType::Electronics,
                price: 500,
            },
            Product {
                identifier: Uuid::new_v4(),
                pr_type: ProductType::Electronics,
                price: 100,
            },
        ];

        let products2 = vec![
            Product {
                identifier: Uuid::new_v4(),
                pr_type: ProductType::Books,
                price: 20,
            },
            Product {
                identifier: Uuid::new_v4(),
                pr_type: ProductType::Electronics,
                price: 400,
            },
        ];

        let node0 = system.register_module(Node::new(products1)).await;
        let node1 = system.register_module(Node::new(products2)).await;
        let distributed_store = system
            .register_module(DistributedStore::new(vec![node0, node1]))
            .await;

        // When:
        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: -150,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).unwrap();
                    })
                }),
            })
            .await;

        // Then:
        assert_eq!(TwoPhaseResult::Abort, transaction_done_rx.await.unwrap());
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(300)]
    async fn query_returns_price_of_product() {
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx): (
            Sender<ProductPrice>,
            Receiver<ProductPrice>,
        ) = channel();
        let identifier: Uuid = Uuid::from_u128(13);
        let products = vec![Product {
            identifier: identifier.clone(),
            pr_type: ProductType::Electronics,
            price: 200,
        }];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let _distributed_store = system
            .register_module(DistributedStore::new(vec![node0.clone()]))
            .await;

        node0
            .send(ProductPriceQuery {
                product_ident: identifier.clone(),
                result_sender: transaction_done_tx,
            })
            .await;

        assert_eq!(200, transaction_done_rx.await.unwrap().0.unwrap());
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(300)]
    async fn query_element_not_in_storage() {
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx): (
            Sender<ProductPrice>,
            Receiver<ProductPrice>,
        ) = channel();
        let products = vec![
            Product {
                identifier: Uuid::from_u128(789),
                pr_type: ProductType::Electronics,
                price: 200,
            },
            Product {
                identifier: Uuid::from_u128(45),
                pr_type: ProductType::Books,
                price: 20,
            },
            Product {
                identifier: Uuid::from_u128(4578),
                pr_type: ProductType::Toys,
                price: 30,
            },
        ];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let _distributed_store = system
            .register_module(DistributedStore::new(vec![node0.clone()]))
            .await;

        node0
            .send(ProductPriceQuery {
                product_ident: Uuid::from_u128(34),
                result_sender: transaction_done_tx,
            })
            .await;

        assert_eq!(None, transaction_done_rx.await.unwrap().0);
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(300)]
    async fn multiple_transactions_work() {
        // Given:
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = channel();
        let products = vec![Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: 180,
        },
                            Product {
                                identifier: Uuid::new_v4(),
                                pr_type: ProductType::Books,
                                price: 25,
                            }];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let node1 = system.register_module(Node::new(products)).await;
        let distributed_store = system
            .register_module(DistributedStore::new(vec![node0, node1]))
            .await;

        // When:
        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: -50,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).unwrap();
                    })
                }),
            })
            .await;

        // Then:
        assert_eq!(TwoPhaseResult::Ok, transaction_done_rx.await.unwrap());

        let (transaction_done_tx2, transaction_done_rx2) = channel();

        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Books,
                    shift: -15,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx2.send(result).unwrap();
                    })
                }),
            })
            .await;

        assert_eq!(TwoPhaseResult::Ok, transaction_done_rx2.await.unwrap());

        system.shutdown().await;
    }
}