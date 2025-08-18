use axum::{extract::State, response::Json};
use std::sync::Arc;
use crate::services::PaymentService;

pub async fn get_metrics(
    State(payment_service): State<Arc<PaymentService>>,
) -> Json<serde_json::Value> {
    let total_payments = payment_service.get_total_payments();
    let total_amount = payment_service.get_total_amount();
    let total_fees = payment_service.get_total_fees();

    // Get circuit breaker status
    let default_breaker = payment_service
        .get_circuit_breaker_status("default")
        .await;

    let fallback_breaker = payment_service
        .get_circuit_breaker_status("fallback")
        .await;

    Json(serde_json::json!({
        "total_payments": total_payments,
        "total_amount_cents": total_amount,
        "total_fees_cents": total_fees,
        "circuit_breakers": {
            "default": default_breaker,
            "fallback": fallback_breaker
        },
        "detailed_metrics": payment_service.get_metrics().await
    }))
}