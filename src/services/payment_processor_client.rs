use crate::app::config::Config;
use crate::models::payment::{PaymentRequest, Payment};
use reqwest::Client;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use tracing::{info, error, warn};

#[derive(Debug, Clone)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

struct CircuitBreaker {
    state: CircuitBreakerState,
    failure_count: u32,
    last_failure_time: Option<SystemTime>,
    threshold: u32,
    timeout: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, timeout_secs: u64) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            last_failure_time: None,
            threshold,
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    fn can_execute(&mut self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                if let Some(last_failure) = self.last_failure_time {
                    if last_failure.elapsed().unwrap_or(Duration::ZERO) >= self.timeout {
                        self.state = CircuitBreakerState::HalfOpen;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitBreakerState::HalfOpen => true,
        }
    }

    fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitBreakerState::Closed;
        self.last_failure_time = None;
    }

    fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure_time = Some(SystemTime::now());

        if self.failure_count >= self.threshold {
            self.state = CircuitBreakerState::Open;
        }
    }
}

pub struct PaymentProcessorClient {
    client: Client,
    config: Config,
    default_breaker: Arc<Mutex<CircuitBreaker>>,
    fallback_breaker: Arc<Mutex<CircuitBreaker>>,
}

impl PaymentProcessorClient {
    pub fn new(config: &Config) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_millis(5000))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            config: config.clone(),
            default_breaker: Arc::new(Mutex::new(CircuitBreaker::new(
                config.circuit_breaker_threshold,
                config.circuit_breaker_timeout_secs,
            ))),
            fallback_breaker: Arc::new(Mutex::new(CircuitBreaker::new(
                config.circuit_breaker_threshold,
                config.circuit_breaker_timeout_secs,
            ))),
        }
    }

    pub async fn process_payment(&self, request: PaymentRequest) -> Option<Payment> {
        // Try default processor first
        if let Some(payment) = self.try_processor("default", &request).await {
            return Some(payment);
        }

        // Fallback to secondary processor
        if let Some(payment) = self.try_processor("fallback", &request).await {
            return Some(payment);
        }

        error!("Both processors failed for payment {}", request.id);
        None
    }

    async fn try_processor(&self, processor_type: &str, request: &PaymentRequest) -> Option<Payment> {
        let (url, breaker) = match processor_type {
            "default" => (&self.config.default_processor_url, &self.default_breaker),
            "fallback" => (&self.config.fallback_processor_url, &self.fallback_breaker),
            _ => return None,
        };

        // Check circuit breaker
        let can_execute = {
            let mut breaker = breaker.lock().unwrap();
            breaker.can_execute()
        };

        if !can_execute {
            warn!("Circuit breaker open for {} processor", processor_type);
            return None;
        }

        match self.send_request(url, request).await {
            Ok(payment) => {
                let mut breaker = breaker.lock().unwrap();
                breaker.record_success();
                info!("Payment {} processed successfully by {} processor", request.id, processor_type);
                Some(payment)
            }
            Err(e) => {
                let mut breaker = breaker.lock().unwrap();
                breaker.record_failure();
                error!("Failed to process payment {} with {} processor: {}", request.id, processor_type, e);
                None
            }
        }
    }

    async fn send_request(&self, url: &str, request: &PaymentRequest) -> Result<Payment, Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::json!({
            "correlationId": request.id,
            "amount": request.amount,
            "requestedAt": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        });

        let response = self.client
            .post(&format!("{}/payments", url))
            .header("Content-Type", "application/json")
            .header("X-Rinha-Token", &self.config.token)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let processor = if url.contains("default") {
                "default"
            } else {
                "fallback"
            };

            Ok(Payment {
                id: request.id.clone(),
                amount: request.amount,
                processor: processor.to_string(),
                fee: request.amount / 20, // 5% fee
                processed_at: Some(SystemTime::now()),
            })
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP {}", response.status())
            )))
        }
    }

    pub async fn health_check(&self, processor_type: &str) -> bool {
        let url = match processor_type {
            "default" => &self.config.default_processor_url,
            "fallback" => &self.config.fallback_processor_url,
            _ => return false,
        };

        match self.client
            .get(&format!("{}/health", url))
            .timeout(Duration::from_millis(10000))
            .send()
            .await
        {
            Ok(response) => {
                let is_healthy = response.status().is_success();
                if is_healthy {
                    info!("{} processor is healthy", processor_type);
                } else {
                    warn!("{} processor returned status: {}", processor_type, response.status());
                }
                is_healthy
            }
            Err(e) => {
                warn!("{} processor health check failed: {}", processor_type, e);
                false
            }
        }
    }

    pub async fn get_breaker_status(&self, processor_type: &str) -> Option<CircuitBreakerState> {
        let breaker = match processor_type {
            "default" => &self.default_breaker,
            "fallback" => &self.fallback_breaker,
            _ => return None,
        };

        let breaker = breaker.lock().unwrap();
        Some(breaker.state.clone())
    }
}