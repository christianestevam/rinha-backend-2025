use crate::models::payment::{PaymentRequest, Payment};
use crate::services::payment_processor_client::PaymentProcessorClient;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;
use tracing::{error, info};

pub type PaymentStorage = Arc<Mutex<Vec<Payment>>>;

pub async fn process_payments(
    mut receiver: Receiver<PaymentRequest>,
    storage: PaymentStorage,
    processor_client: Arc<PaymentProcessorClient>,
) {
    info!("Payment processor started");

    while let Some(req) = receiver.recv().await {
        info!("Processing payment: {}", req.id);

        // Usa o client real para processar o pagamento
        match processor_client.process_payment(req.clone()).await {
            Some(payment) => {
                info!(
                    "Payment {} processed successfully via {}",
                    payment.id, payment.processor
                );

                // Armazena o pagamento processado
                let mut store = storage.lock().unwrap();
                store.push(payment);
            }
            None => {
                error!("Failed to process payment: {}", req.id);
                // Opcionalmente, poderíamos adicionar à uma fila de retry
            }
        }
    }
}