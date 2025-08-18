mod app;
mod handlers;
mod models;
mod services;
mod queue;
mod utils;

use app::config::Config;
use axum::{
    http::StatusCode,
    routing::{get, post},
    Router,
};
use handlers::*;
use services::{PaymentService, PaymentProcessorClient};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;
use dashmap::DashMap;
use models::payment::PaymentRequest;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::from_env();
    info!("Starting Rinha Backend 2025 server on port {}", config.server_port);

    let storage = Arc::new(DashMap::new());
    let processor_client = Arc::new(PaymentProcessorClient::new(&config));
    let (payment_sender, payment_receiver) = mpsc::channel::<PaymentRequest>(config.queue_buffer_size);

    let payment_service = Arc::new(PaymentService::new(
        storage,
        processor_client.clone(),
        payment_sender,
    ));

    // Health check task
    tokio::spawn({
        let processor_client = processor_client.clone();
        async move {
            loop {
                let default_health = processor_client.health_check("default").await;
                let fallback_health = processor_client.health_check("fallback").await;
                
                info!("Processor health - Default: {}, Fallback: {}", 
                      if default_health { "healthy" } else { "unhealthy" },
                      if fallback_health { "healthy" } else { "unhealthy" });
                
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            }
        }
    });

    // Payment processing task
    tokio::spawn({
        let payment_service = payment_service.clone();
        async move {
            payment_service.process_payments_async(payment_receiver).await;
        }
    });

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/payments", post(payments::create_payment))
        .route("/payments-summary", get(payments_summary::get_summary))
        .route("/metrics", get(metrics::get_metrics))
        .with_state(payment_service);

    let addr = format!("0.0.0.0:{}", config.server_port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    info!("Server listening on {}", addr);

    axum::serve(listener, app).await.unwrap();
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}