use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentRequest {
    pub id: String,
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payment {
    pub id: String,
    pub amount: u64,
    pub processor: String,
    pub fee: u64,
    pub processed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorResponse {
    pub success: bool,
    pub fee: Option<u64>,
    pub error_message: Option<String>,
}

// Payload para enviar aos Payment Processors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorPayload {
    #[serde(rename = "correlationId")]
    pub correlation_id: String,
    pub amount: u64,
    #[serde(rename = "requestedAt")]
    pub requested_at: u64, // timestamp em milissegundos
}