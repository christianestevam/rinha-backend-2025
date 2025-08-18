use crate::models::payment::{PaymentRequest, Payment};
use crate::services::payment_processor_client::PaymentProcessorClient;
use crate::services::http_client_pool::HttpClientPool;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use tracing::{info, error};

pub struct OptimizedBatchProcessor {
    processor_client: Arc<PaymentProcessorClient>,
    http_pool: Arc<HttpClientPool>,
    batch_size: usize,
    flush_interval: Duration,
}

impl OptimizedBatchProcessor {
    pub fn new(processor_client: Arc<PaymentProcessorClient>) -> Self {
        Self {
            processor_client,
            http_pool: Arc::new(HttpClientPool::new(10)), // Pool de 10 clientes
            batch_size: 50,
            flush_interval: Duration::from_millis(100),
        }
    }

    pub async fn process_payments_optimized(
        &self,
        mut receiver: mpsc::Receiver<PaymentRequest>,
    ) -> mpsc::Receiver<Payment> {
        let (sender, processed_receiver) = mpsc::channel::<Payment>(2000);
        let processor_client = Arc::clone(&self.processor_client);

        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(50);
            let mut flush_timer = interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    // Recebe pagamentos
                    maybe_payment = receiver.recv() => {
                        match maybe_payment {
                            Some(payment) => {
                                batch.push(payment);
                                
                                // Processa batch quando cheio
                                if batch.len() >= 50 {
                                    Self::process_batch(
                                        &processor_client, 
                                        &sender, 
                                        &mut batch
                                    ).await;
                                }
                            }
                            None => break,
                        }
                    }
                    
                    // Timer para flush periódico
                    _ = flush_timer.tick() => {
                        if !batch.is_empty() {
                            Self::process_batch(
                                &processor_client, 
                                &sender, 
                                &mut batch
                            ).await;
                        }
                    }
                }
            }
        });

        processed_receiver
    }

    async fn process_batch(
        processor_client: &Arc<PaymentProcessorClient>,
        sender: &mpsc::Sender<Payment>,
        batch: &mut Vec<PaymentRequest>,
    ) {
        let batch_size = batch.len();
        info!("Processing batch of {} payments", batch_size);

        // Processa em paralelo usando futures
        let mut handles = Vec::with_capacity(batch_size);
        
        for payment_req in batch.drain(..) {
            let client = Arc::clone(processor_client); // Agora processor_client é &Arc<...>
            let handle = tokio::spawn(async move {
                client.process_payment(payment_req).await
            });
            handles.push(handle);
        }

        // Aguarda todos em paralelo
        let results = futures::future::join_all(handles).await;
        
        for result in results {
            if let Ok(Some(payment)) = result {
                if sender.send(payment).await.is_err() {
                    error!("Failed to send processed payment");
                    break;
                }
            }
        }

        info!("Batch of {} payments processed", batch_size);
    }
}