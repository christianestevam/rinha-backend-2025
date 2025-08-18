use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
};
use serde_json::Value;
use std::sync::Arc;
use tracing::{info, error};

use crate::services::{PaymentService, ServiceError};
use crate::models::payment::PaymentRequest;

pub async fn create_payment(
    State(service): State<Arc<PaymentService>>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let request: PaymentRequest = match serde_json::from_value(payload) {
        Ok(req) => req,
        Err(e) => {
            error!("Invalid payment request: {}", e);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    info!("Received payment request: {}", request.id);

    match service.submit_payment(request).await {
        Ok(_) => {
            info!("Payment submitted successfully");
            Ok(Json(serde_json::json!({
                "status": "accepted",
                "message": "Payment submitted for processing"
            })))
        }
        Err(ServiceError::QueueFull) => {
            error!("Queue is full");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
        Err(e) => {
            error!("Failed to submit payment: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}