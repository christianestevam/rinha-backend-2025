use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub server_port: u16,
    pub token: String,
    pub default_processor_url: String,
    pub fallback_processor_url: String,
    pub batch_size: usize,
    pub queue_buffer_size: usize,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_timeout_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            server_port: env::var("PORT")
                .unwrap_or_else(|_| "9999".to_string())
                .parse()
                .unwrap_or(9999),
            token: env::var("TOKEN")
                .unwrap_or_else(|_| "123".to_string()),
            default_processor_url: env::var("DEFAULT_PROCESSOR_URL")
                .unwrap_or_else(|_| "http://payment-processor-default:8080".to_string()),
            fallback_processor_url: env::var("FALLBACK_PROCESSOR_URL")
                .unwrap_or_else(|_| "http://payment-processor-fallback:8080".to_string()),
            batch_size: env::var("BATCH_SIZE")
                .unwrap_or_else(|_| "50".to_string())
                .parse()
                .unwrap_or(50),
            queue_buffer_size: env::var("QUEUE_BUFFER_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            circuit_breaker_threshold: env::var("CIRCUIT_BREAKER_THRESHOLD")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            circuit_breaker_timeout_secs: env::var("CIRCUIT_BREAKER_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        }
    }
}