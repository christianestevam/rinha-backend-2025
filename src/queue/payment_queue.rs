use crate::models::payment::PaymentRequest;
use tokio::sync::mpsc::{self, Sender, Receiver};

pub fn create_queue(buffer: usize) -> (Sender<PaymentRequest>, Receiver<PaymentRequest>) {
    mpsc::channel(buffer)
}