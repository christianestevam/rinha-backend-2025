use crate::models::payment::{Payment, PaymentRequest};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::SystemTime;

pub struct OptimizedPaymentProcessor {
    batch_queue: Arc<RwLock<VecDeque<PaymentRequest>>>,
    batch_size: usize,
    processing: Arc<RwLock<bool>>,
}

impl OptimizedPaymentProcessor {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_queue: Arc::new(RwLock::new(VecDeque::new())),
            batch_size,
            processing: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn queue_payment(&self, payment: PaymentRequest) {
        let mut queue = self.batch_queue.write().await;
        queue.push_back(payment);

        if queue.len() >= self.batch_size {
            drop(queue); // Release lock before processing
            self.process_batch().await;
        }
    }

    async fn process_batch(&self) {
        let mut processing = self.processing.write().await;
        if *processing {
            return; // Already processing
        }
        *processing = true;
        drop(processing);

        let batch = {
            let mut queue = self.batch_queue.write().await;
            let batch_size = queue.len().min(self.batch_size);
            queue.drain(..batch_size).collect::<Vec<_>>()
        };

        if !batch.is_empty() {
            self.process_payment_batch(batch).await;
        }

        let mut processing = self.processing.write().await;
        *processing = false;
    }

    async fn process_payment_batch(&self, batch: Vec<PaymentRequest>) {
        for request in batch {
            // Simulate payment processing
            let _processed_payment = self.simulate_payment_processing(&request).await;
        }
    }

    async fn simulate_payment_processing(&self, request: &PaymentRequest) -> Option<Payment> {
        // Simular processamento
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        Some(Payment {
            id: request.id.clone(),
            amount: request.amount,
            processor: "optimized".to_string(),
            fee: request.amount / 20,
            processed_at: Some(SystemTime::now()), // Adicionar campo faltante
        })
    }

    pub async fn force_process(&self) {
        self.process_batch().await;
    }

    pub async fn queue_size(&self) -> usize {
        let queue = self.batch_queue.read().await;
        queue.len()
    }
}