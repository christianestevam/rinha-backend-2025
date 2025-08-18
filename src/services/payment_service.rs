use crate::models::payment::{Payment, PaymentRequest};
use crate::services::payment_processor_client::PaymentProcessorClient;
use crate::services::atomic_metrics::AtomicMetrics;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

pub type PaymentStorage = Arc<DashMap<String, Payment>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryFilters {
    pub from_date: Option<String>,
    pub to_date: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SummaryResult {
    pub total_amount_cents: u64,
    pub total_fee_cents: u64,
    pub count: u64,
    pub count_processed: u64,
    pub count_failed: u64,
}

pub struct PaymentService {
    storage: PaymentStorage,
    processor_client: Arc<PaymentProcessorClient>,
    payment_sender: mpsc::Sender<PaymentRequest>,
    metrics: Arc<AtomicMetrics>,
}

#[derive(Debug)]
pub enum ServiceError {
    QueueFull,
    ProcessingError,
}

impl PaymentService {
    pub fn new(
        storage: PaymentStorage,
        processor_client: Arc<PaymentProcessorClient>,
        payment_sender: mpsc::Sender<PaymentRequest>,
    ) -> Self {
        Self {
            storage,
            processor_client,
            payment_sender,
            metrics: Arc::new(AtomicMetrics::new()),
        }
    }

    pub async fn submit_payment(&self, request: PaymentRequest) -> Result<(), ServiceError> {
        self.metrics.increment_submitted();
        
        let payment = Payment {
            id: request.id.clone(),
            amount: request.amount,
            processor: "pending".to_string(),
            fee: 0,
            processed_at: None,
        };
        
        self.storage.insert(request.id.clone(), payment);
        
        self.payment_sender.send(request).await
            .map_err(|_| ServiceError::QueueFull)?;
        
        Ok(())
    }

    pub async fn get_summary(&self, _filters: SummaryFilters) -> SummaryResult {
        let mut total_amount = 0u64;
        let mut total_fee = 0u64;
        let mut count = 0u64;
        let mut count_processed = 0u64;
        let mut count_failed = 0u64;

        for payment in self.storage.iter() {
            count += 1;
            if payment.processed_at.is_some() {
                total_amount += payment.amount;
                total_fee += payment.fee;
                if payment.processor != "failed" {
                    count_processed += 1;
                } else {
                    count_failed += 1;
                }
            }
        }

        SummaryResult {
            total_amount_cents: total_amount,
            total_fee_cents: total_fee,
            count,
            count_processed,
            count_failed,
        }
    }

    pub async fn process_payments_async(&self, mut receiver: mpsc::Receiver<PaymentRequest>) {
        info!("Starting payment processor worker");
        
        while let Some(request) = receiver.recv().await {
            self.process_single_payment(request).await;
        }
    }

    async fn process_single_payment(&self, request: PaymentRequest) {
        info!("Processing payment: {}", request.id);
        
        match self.processor_client.process_payment(request.clone()).await {
            Some(processed_payment) => {
                self.storage.insert(request.id.clone(), processed_payment);
                self.metrics.increment_processed();
                info!("Payment {} processed successfully", request.id);
            }
            None => {
                let failed_payment = Payment {
                    id: request.id.clone(),
                    amount: request.amount,
                    processor: "failed".to_string(),
                    fee: 0,
                    processed_at: Some(SystemTime::now()),
                };
                self.storage.insert(request.id, failed_payment);
                self.metrics.increment_failed();
                warn!("Payment processing failed");
            }
        }
    }

    pub fn get_payment(&self, id: &str) -> Option<Payment> {
        self.storage.get(id).map(|entry| entry.clone())
    }

    pub async fn get_metrics(&self) -> serde_json::Value {
        let submitted = self.metrics.get_submitted();
        let processed = self.metrics.get_processed();
        let failed = self.metrics.get_failed();

        serde_json::json!({
            "submitted": submitted,
            "processed": processed,
            "failed": failed,
            "success_rate": if submitted > 0 { 
                (processed as f64 / submitted as f64) * 100.0 
            } else { 
                0.0 
            },
            "processors": {
                "default": self.get_processor_status("default").await,
                "fallback": self.get_processor_status("fallback").await,
            }
        })
    }

    async fn get_processor_status(&self, processor: &str) -> serde_json::Value {
        let health = self.processor_client.health_check(processor).await;
        let breaker_status = self.processor_client
            .get_breaker_status(processor)
            .await;

        serde_json::json!({
            "healthy": health,
            "circuit_breaker": format!("{:?}", breaker_status)
        })
    }

    // Métodos para compatibilidade com metrics
    pub fn get_total_payments(&self) -> u64 {
        self.storage.len() as u64
    }

    pub fn get_total_amount(&self) -> u64 {
        self.storage.iter()
            .map(|entry| entry.amount)
            .sum()
    }

    pub fn get_total_fees(&self) -> u64 {
        self.storage.iter()
            .map(|entry| entry.fee)
            .sum()
    }

    pub async fn get_circuit_breaker_status(&self, processor: &str) -> String {
        match self.processor_client.get_breaker_status(processor).await {
            Some(status) => format!("{:?}", status),
            None => "unknown".to_string(),
        }
    }
}