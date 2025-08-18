use axum::{
    extract::{Query, State},
    response::Json,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;

use crate::services::{PaymentService, SummaryFilters};

#[derive(Deserialize)]
pub struct SummaryQuery {
    #[serde(rename = "de")]
    from_date: Option<String>,
    #[serde(rename = "ate")]
    to_date: Option<String>,
}

pub async fn get_summary(
    State(service): State<Arc<PaymentService>>,
    Query(query): Query<SummaryQuery>,
) -> Json<serde_json::Value> {
    info!("Getting payments summary");

    let filters = SummaryFilters {
        from_date: query.from_date,
        to_date: query.to_date,
    };

    let summary = service.get_summary(filters).await;

    Json(serde_json::json!({
        "total_amount_cents": summary.total_amount_cents,
        "total_fee_cents": summary.total_fee_cents,
        "count": summary.count,
        "count_processed": summary.count_processed,
        "count_failed": summary.count_failed
    }))
}