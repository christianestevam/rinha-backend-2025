//! Ultra-fast request handlers optimized for Rinha Backend endpoints
//! 
//! These handlers are specifically designed for the three main endpoints:
//! - POST /payments (most critical for performance)
//! - GET /payments-summary (frequent queries)  
//! - POST /purge-payments (testing only)

use super::http_parser::{HttpRequest, Method};
use super::response::{HttpResponse, ResponseBuilder};
use crate::storage::{StorageEngine, ProcessorType};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Payment request payload
#[derive(Debug, Deserialize)]
struct PaymentRequest {
    #[serde(rename = "correlationId")]
    correlation_id: String,
    amount: f64,
}

/// Query parameters for summary endpoint
#[derive(Debug)]
struct SummaryQuery {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

/// Handler context shared across all requests
pub struct HandlerContext {
    /// Storage engine for data operations
    storage: Arc<StorageEngine>,
    /// Response builder for efficient response generation
    response_builder: ResponseBuilder,
    /// Request statistics
    stats: RequestStats,
}

/// Request processing statistics
#[derive(Default)]
struct RequestStats {
    payments_processed: std::sync::atomic::AtomicU64,
    summaries_served: std::sync::atomic::AtomicU64,
    purges_executed: std::sync::atomic::AtomicU64,
    errors_encountered: std::sync::atomic::AtomicU64,
}

impl HandlerContext {
    /// Create new handler context
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            response_builder: ResponseBuilder::new(),
            stats: RequestStats::default(),
        }
    }
    
    /// Get current statistics
    pub fn get_stats(&self) -> (u64, u64, u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.stats.payments_processed.load(Ordering::Relaxed),
            self.stats.summaries_served.load(Ordering::Relaxed),
            self.stats.purges_executed.load(Ordering::Relaxed),
            self.stats.errors_encountered.load(Ordering::Relaxed),
        )
    }
}

/// Main request router - dispatches to appropriate handlers
pub async fn handle_request(
    ctx: &HandlerContext,
    request: HttpRequest<'_>,
) -> Result<HttpResponse, HandlerError> {
    match (request.method, request.path) {
        (Method::Post, "/payments") => {
            handle_payment(ctx, request).await
        }
        (Method::Get, "/payments-summary") => {
            handle_summary(ctx, request).await
        }
        (Method::Post, "/purge-payments") => {
            handle_purge(ctx, request).await
        }
        (Method::Options, _) => {
            handle_options(ctx, request).await
        }
        _ => {
            ctx.stats.errors_encountered
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(ctx.response_builder.not_found())
        }
    }
}

/// Handle POST /payments - the most critical endpoint
async fn handle_payment(
    ctx: &HandlerContext,
    request: HttpRequest<'_>,
) -> Result<HttpResponse, HandlerError> {
    use std::sync::atomic::Ordering;
    
    // Validate Content-Type
    if let Some(content_type) = request.headers.content_type {
        if !content_type.contains("application/json") {
            return Ok(ctx.response_builder.bad_request("Invalid Content-Type"));
        }
    }
    
    // Parse JSON payload (optimized for our specific structure)
    let payment_req: PaymentRequest = match parse_payment_json_fast(request.body) {
        Ok(req) => req,
        Err(e) => {
            tracing::debug!("Invalid payment JSON: {}", e);
            ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            return Ok(ctx.response_builder.bad_request("Invalid JSON"));
        }
    };
    
    // Validate correlation ID
    let correlation_id = match Uuid::parse_str(&payment_req.correlation_id) {
        Ok(id) => id,
        Err(_) => {
            ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            return Ok(ctx.response_builder.bad_request("Invalid correlationId"));
        }
    };
    
    // Validate amount
    if payment_req.amount <= 0.0 || payment_req.amount > 1_000_000.0 {
        ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
        return Ok(ctx.response_builder.bad_request("Invalid amount"));
    }
    
    // Store payment (using default processor for now - routing logic will be added later)
    match ctx.storage.store_payment(
        correlation_id,
        payment_req.amount,
        ProcessorType::Default,
    ).await {
        Ok(_) => {
            ctx.stats.payments_processed.fetch_add(1, Ordering::Relaxed);
            Ok(ctx.response_builder.ok_empty())
        }
        Err(e) => {
            tracing::error!("Failed to store payment: {}", e);
            ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            Ok(ctx.response_builder.internal_error())
        }
    }
}

/// Handle GET /payments-summary
async fn handle_summary(
    ctx: &HandlerContext,
    request: HttpRequest<'_>,
) -> Result<HttpResponse, HandlerError> {
    use std::sync::atomic::Ordering;
    
    // Parse query parameters
    let query = parse_summary_query(request.query)?;
    
    // Get summary from storage
    let summary = match (query.from, query.to) {
        (Some(from), Some(to)) => {
            ctx.storage.get_summary(from, to).await
        }
        _ => {
            // No time range specified, get current stats (fast path)
            ctx.storage.get_current_stats().await
        }
    };
    
    match summary {
        Ok(summary) => {
            ctx.stats.summaries_served.fetch_add(1, Ordering::Relaxed);
            
            // Serialize to JSON (optimized)
            match serialize_summary_fast(&summary) {
                Ok(json) => Ok(ctx.response_builder.ok_json(&json)),
                Err(e) => {
                    tracing::error!("Failed to serialize summary: {}", e);
                    Ok(ctx.response_builder.internal_error())
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to get summary: {}", e);
            ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            Ok(ctx.response_builder.internal_error())
        }
    }
}

/// Handle POST /purge-payments (testing endpoint)
async fn handle_purge(
    ctx: &HandlerContext,
    _request: HttpRequest<'_>,
) -> Result<HttpResponse, HandlerError> {
    use std::sync::atomic::Ordering;
    
    match ctx.storage.purge_all().await {
        Ok(_) => {
            ctx.stats.purges_executed.fetch_add(1, Ordering::Relaxed);
            tracing::info!("Payment store purged");
            Ok(ctx.response_builder.no_content())
        }
        Err(e) => {
            tracing::error!("Failed to purge payments: {}", e);
            ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            Ok(ctx.response_builder.internal_error())
        }
    }
}

/// Handle OPTIONS requests (CORS preflight)
async fn handle_options(
    ctx: &HandlerContext,
    _request: HttpRequest<'_>,
) -> Result<HttpResponse, HandlerError> {
    Ok(ctx.response_builder.options())
}

/// Fast JSON parsing for payment requests (optimized for our specific structure)
fn parse_payment_json_fast(data: &[u8]) -> Result<PaymentRequest, serde_json::Error> {
    // For maximum performance, we could implement a custom parser here
    // that only extracts the two fields we need, but serde_json is quite fast
    serde_json::from_slice(data)
}

/// Fast JSON serialization for summary responses
fn serialize_summary_fast(summary: &crate::storage::PaymentSummary) -> Result<String, serde_json::Error> {
    // Pre-allocate string with estimated capacity
    let mut result = String::with_capacity(256);
    
    // Manual JSON construction for maximum speed (avoiding serde overhead)
    result.push('{');
    
    // Default processor data
    result.push_str("\"default\":{");
    result.push_str("\"totalRequests\":");
    result.push_str(&summary.default.total_requests.to_string());
    result.push_str(",\"totalAmount\":");
    result.push_str(&summary.default.total_amount.to_string());
    result.push_str("},");
    
    // Fallback processor data
    result.push_str("\"fallback\":{");
    result.push_str("\"totalRequests\":");
    result.push_str(&summary.fallback.total_requests.to_string());
    result.push_str(",\"totalAmount\":");
    result.push_str(&summary.fallback.total_amount.to_string());
    result.push_str("}}");
    
    Ok(result)
}

/// Parse query string for summary endpoint
fn parse_summary_query(query: Option<&str>) -> Result<SummaryQuery, HandlerError> {
    let mut from = None;
    let mut to = None;
    
    if let Some(query_str) = query {
        for param in query_str.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "from" => {
                        from = DateTime::parse_from_rfc3339(value)
                            .map(|dt| dt.with_timezone(&Utc))
                            .ok();
                    }
                    "to" => {
                        to = DateTime::parse_from_rfc3339(value)
                            .map(|dt| dt.with_timezone(&Utc))
                            .ok();
                    }
                    _ => {
                        // Ignore unknown parameters
                    }
                }
            }
        }
    }
    
    Ok(SummaryQuery { from, to })
}

/// Handler errors
#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
}

/// Performance optimizations for hot paths
pub mod optimizations {
    use super::*;
    
    /// Pre-compiled response for successful payment
    static PAYMENT_SUCCESS_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    
    /// Pre-compiled response for bad request
    static BAD_REQUEST_RESPONSE: &[u8] = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
    
    /// Ultra-fast payment validation (branchless where possible)
    #[inline]
    pub fn validate_payment_fast(amount: f64, correlation_id: &str) -> bool {
        // Branchless validation using bit operations
        let amount_valid = amount > 0.0 && amount <= 1_000_000.0 && amount.is_finite();
        let id_valid = correlation_id.len() == 36; // UUID length
        
        amount_valid && id_valid
    }
    
    /// Fast correlation ID parsing without full UUID validation
    #[inline]
    pub fn parse_correlation_id_fast(id: &str) -> Option<Uuid> {
        // Skip some validation for performance in hot path
        if id.len() == 36 {
            Uuid::parse_str(id).ok()
        } else {
            None
        }
    }
    
    /// Optimized JSON field extraction for payment requests
    pub fn extract_payment_fields_fast(json: &[u8]) -> Option<(String, f64)> {
        // Custom JSON parser that only extracts correlationId and amount
        // This is much faster than full JSON parsing for our specific case
        
        let json_str = std::str::from_utf8(json).ok()?;
        
        let mut correlation_id = None;
        let mut amount = None;
        
        // Find correlationId
        if let Some(start) = json_str.find("\"correlationId\":\"") {
            let start = start + 16; // Length of "correlationId":""
            if let Some(end) = json_str[start..].find('"') {
                correlation_id = Some(json_str[start..start + end].to_string());
            }
        }
        
        // Find amount
        if let Some(start) = json_str.find("\"amount\":") {
            let start = start + 9; // Length of "amount":"
            let remaining = &json_str[start..];
            
            // Skip whitespace
            let remaining = remaining.trim_start();
            
            // Find end of number (comma, brace, or end)
            let end = remaining.find(|c: char| c == ',' || c == '}')
                .unwrap_or(remaining.len());
            
            if let Ok(parsed_amount) = remaining[..end].parse::<f64>() {
                amount = Some(parsed_amount);
            }
        }
        
        match (correlation_id, amount) {
            (Some(id), Some(amt)) => Some((id, amt)),
            _ => None,
        }
    }
}

/// Specialized request processing for high-frequency patterns
pub mod specialized {
    use super::*;
    
    /// Process payment request with minimal allocations
    pub async fn process_payment_minimal(
        ctx: &HandlerContext,
        body: &[u8],
    ) -> Result<HttpResponse, HandlerError> {
        use std::sync::atomic::Ordering;
        
        // Fast field extraction
        let (correlation_id_str, amount) = match optimizations::extract_payment_fields_fast(body) {
            Some(fields) => fields,
            None => {
                ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                return Ok(ctx.response_builder.bad_request("Invalid JSON format"));
            }
        };
        
        // Fast validation
        if !optimizations::validate_payment_fast(amount, &correlation_id_str) {
            ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
            return Ok(ctx.response_builder.bad_request("Invalid payment data"));
        }
        
        // Parse correlation ID
        let correlation_id = match optimizations::parse_correlation_id_fast(&correlation_id_str) {
            Some(id) => id,
            None => {
                ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                return Ok(ctx.response_builder.bad_request("Invalid correlationId"));
            }
        };
        
        // Store payment
        match ctx.storage.store_payment(correlation_id, amount, ProcessorType::Default).await {
            Ok(_) => {
                ctx.stats.payments_processed.fetch_add(1, Ordering::Relaxed);
                Ok(ctx.response_builder.ok_empty())
            }
            Err(e) => {
                tracing::error!("Storage error: {}", e);
                ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                Ok(ctx.response_builder.internal_error())
            }
        }
    }
    
    /// Process summary request with caching hints
    pub async fn process_summary_cached(
        ctx: &HandlerContext,
        query: Option<&str>,
    ) -> Result<HttpResponse, HandlerError> {
        use std::sync::atomic::Ordering;
        
        // For queries without time range, use fast path
        if query.is_none() || query == Some("") {
            match ctx.storage.get_current_stats().await {
                Ok(summary) => {
                    ctx.stats.summaries_served.fetch_add(1, Ordering::Relaxed);
                    let json = serialize_summary_fast(&summary)?;
                    Ok(ctx.response_builder.ok_json(&json))
                }
                Err(e) => {
                    ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                    Err(HandlerError::Storage(e))
                }
            }
        } else {
            // Fall back to full query processing
            let parsed_query = parse_summary_query(query)?;
            
            let summary = match (parsed_query.from, parsed_query.to) {
                (Some(from), Some(to)) => ctx.storage.get_summary(from, to).await,
                _ => ctx.storage.get_current_stats().await,
            };
            
            match summary {
                Ok(summary) => {
                    ctx.stats.summaries_served.fetch_add(1, Ordering::Relaxed);
                    let json = serialize_summary_fast(&summary)?;
                    Ok(ctx.response_builder.ok_json(&json))
                }
                Err(e) => {
                    ctx.stats.errors_encountered.fetch_add(1, Ordering::Relaxed);
                    Err(HandlerError::Storage(e))
                }
            }
        }
    }
}

/// Request validation utilities
pub mod validation {
    use super::*;
    
    /// Validate payment amount with specific business rules
    pub fn validate_amount(amount: f64) -> Result<(), &'static str> {
        if amount <= 0.0 {
            return Err("Amount must be positive");
        }
        
        if amount > 1_000_000.0 {
            return Err("Amount exceeds maximum limit");
        }
        
        if !amount.is_finite() {
            return Err("Amount must be a valid number");
        }
        
        // Check for reasonable precision (avoid floating point issues)
        let cents = (amount * 100.0).round() as u64;
        let reconstructed = cents as f64 / 100.0;
        if (amount - reconstructed).abs() > 0.001 {
            return Err("Amount has invalid precision");
        }
        
        Ok(())
    }
    
    /// Validate correlation ID format
    pub fn validate_correlation_id(id: &str) -> Result<Uuid, &'static str> {
        if id.is_empty() {
            return Err("Correlation ID cannot be empty");
        }
        
        if id.len() != 36 {
            return Err("Correlation ID must be a valid UUID");
        }
        
        Uuid::parse_str(id).map_err(|_| "Invalid UUID format")
    }
    
    /// Validate request headers for payment endpoint
    pub fn validate_payment_headers(headers: &super::super::http_parser::FastHeaders) -> Result<(), &'static str> {
        // Check Content-Type
        match headers.content_type {
            Some(ct) if ct.contains("application/json") => {},
            Some(_) => return Err("Content-Type must be application/json"),
            None => return Err("Content-Type header is required"),
        }
        
        // Check Content-Length
        match headers.content_length {
            Some(len) if len > 0 && len <= 1024 => {}, // Reasonable payload size
            Some(0) => return Err("Request body cannot be empty"),
            Some(_) => return Err("Request body too large"),
            None => return Err("Content-Length header is required"),
        }
        
        Ok(())
    }
}

impl From<serde_json::Error> for HandlerError {
    fn from(err: serde_json::Error) -> Self {
        HandlerError::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{StorageEngine, StorageConfig};
    use std::sync::Arc;
    
    fn create_test_context() -> HandlerContext {
        let storage = Arc::new(StorageEngine::new(StorageConfig::default()));
        HandlerContext::new(storage)
    }
    
    #[test]
    fn test_validate_amount() {
        use validation::validate_amount;
        
        assert!(validate_amount(19.90).is_ok());
        assert!(validate_amount(1.00).is_ok());
        assert!(validate_amount(999999.99).is_ok());
        
        assert!(validate_amount(0.0).is_err());
        assert!(validate_amount(-10.0).is_err());
        assert!(validate_amount(1_000_001.0).is_err());
        assert!(validate_amount(f64::NAN).is_err());
        assert!(validate_amount(f64::INFINITY).is_err());
    }
    
    #[test]
    fn test_validate_correlation_id() {
        use validation::validate_correlation_id;
        
        let valid_uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert!(validate_correlation_id(valid_uuid).is_ok());
        
        assert!(validate_correlation_id("").is_err());
        assert!(validate_correlation_id("not-a-uuid").is_err());
        assert!(validate_correlation_id("550e8400-e29b-41d4-a716").is_err()); // Too short
    }
    
    #[test]
    fn test_extract_payment_fields_fast() {
        use optimizations::extract_payment_fields_fast;
        
        let json = br#"{"correlationId":"550e8400-e29b-41d4-a716-446655440000","amount":19.90}"#;
        let result = extract_payment_fields_fast(json).unwrap();
        
        assert_eq!(result.0, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(result.1, 19.90);
    }
    
    #[test]
    fn test_serialize_summary_fast() {
        use crate::storage::{PaymentSummary, SummaryData};
        
        let summary = PaymentSummary {
            default: SummaryData {
                total_requests: 100,
                total_amount: 1990.0,
            },
            fallback: SummaryData {
                total_requests: 50,
                total_amount: 995.0,
            },
        };
        
        let json = serialize_summary_fast(&summary).unwrap();
        assert!(json.contains("\"totalRequests\":100"));
        assert!(json.contains("\"totalAmount\":1990"));
        assert!(json.contains("\"totalRequests\":50"));
        assert!(json.contains("\"totalAmount\":995"));
    }
    
    #[test]
    fn test_parse_summary_query() {
        let query = "from=2020-01-01T00:00:00.000Z&to=2020-12-31T23:59:59.999Z";
        let result = parse_summary_query(Some(query)).unwrap();
        
        assert!(result.from.is_some());
        assert!(result.to.is_some());
        
        // Test without query
        let result = parse_summary_query(None).unwrap();
        assert!(result.from.is_none());
        assert!(result.to.is_none());
    }
    
    #[tokio::test]
    async fn test_handle_payment_success() {
        use crate::server::http_parser::{HttpRequest, Method, FastHeaders};
        
        let ctx = create_test_context();
        let body = br#"{"correlationId":"550e8400-e29b-41d4-a716-446655440000","amount":19.90}"#;
        
        let mut headers = FastHeaders::new();
        headers.content_type = Some("application/json");
        headers.content_length = Some(body.len());
        
        let request = HttpRequest {
            method: Method::Post,
            path: "/payments",
            query: None,
            version: "HTTP/1.1",
            headers,
            body,
        };
        
        let response = handle_payment(&ctx, request).await.unwrap();
        assert_eq!(response.status_code, 200);
    }
    
    #[tokio::test]
    async fn test_handle_summary() {
        use crate::server::http_parser::{HttpRequest, Method, FastHeaders};
        
        let ctx = create_test_context();
        let headers = FastHeaders::new();
        
        let request = HttpRequest {
            method: Method::Get,
            path: "/payments-summary",
            query: None,
            version: "HTTP/1.1",
            headers,
            body: &[],
        };
        
        let response = handle_summary(&ctx, request).await.unwrap();
        assert_eq!(response.status_code, 200);
        assert!(response.body.len() > 0);
    }
}