use crate::models::payment::{PaymentRequest, Payment};
use crate::services::payment_processor_client::PaymentProcessorClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tracing::{info, warn};

pub struct BatchProcessor {
    processor_client: Arc<PaymentProcessorClient>,
    batch_size: usize,
    batch_timeout: Duration,
}

impl BatchProcessor {
    pub fn new(processor_client: Arc<PaymentProcessorClient>) -> Self {
        Self {
            processor_client,
            batch_size: 50,
            batch_timeout: Duration::from_millis(10),
        }
    }

    pub async fn process_payments_in_batches(
        &self,
        mut receiver: mpsc::Receiver<PaymentRequest>,
    ) -> mpsc::Receiver<Payment> {
        let (processed_sender, processed_receiver) = mpsc::channel(1000);
        let processor_client = Arc::clone(&self.processor_client);
        let batch_size = self.batch_size;
        let batch_timeout = self.batch_timeout;

        tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut last_batch_time = Instant::now();

            loop {
                tokio::select! {
                    // Recebe novo pagamento
                    payment = receiver.recv() => {
                        match payment {
                            Some(req) => {
                                batch.push(req);
                                
                                // Processa se batch estiver cheio
                                if batch.len() >= batch_size {
                                    Self::process_batch(
                                        &processor_client,
                                        &mut batch,
                                        &processed_sender
                                    ).await;
                                    last_batch_time = Instant::now();
                                }
                            }
                            None => break, // Channel fechado
                        }
                    }
                    
                    // Timeout para processar batch parcial
                    _ = sleep(batch_timeout) => {
                        if !batch.is_empty() && last_batch_time.elapsed() >= batch_timeout {
                            Self::process_batch(
                                &processor_client,
                                &mut batch,
                                &processed_sender
                            ).await;
                            last_batch_time = Instant::now();
                        }
                    }
                }
            }
        });

        processed_receiver
    }

    async fn process_batch(
        processor_client: &PaymentProcessorClient,
        batch: &mut Vec<PaymentRequest>,
        sender: &mpsc::Sender<Payment>,
    ) {
        if batch.is_empty() {
            return;
        }

        info!("Processing batch of {} payments", batch.len());

        // Processa todos os pagamentos do batch em paralelo
        let futures: Vec<_> = batch
            .drain(..)
            .map(|req| processor_client.process_payment(req))
            .collect();

        let results = futures::future::join_all(futures).await;

        // Envia resultados processados
        for result in results.into_iter().flatten() {
            if let Err(e) = sender.send(result).await {
                warn!("Failed to send processed payment: {:?}", e);
            }
        }
    }
}