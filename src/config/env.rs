//! Environment configuration management for the Rinha Backend 2025
//! 
//! This module handles loading and validation of configuration from
//! environment variables, config files, and default values.

use std::time::Duration;
use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};

/// Main application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Server configuration
    pub server: ServerConfig,
    
    /// Processor configuration
    pub processors: ProcessorConfig,
    
    /// Database configuration
    pub database: DatabaseConfig,
    
    /// Metrics configuration
    pub metrics: MetricsConfig,
    
    /// Logging configuration
    pub logging: LoggingConfig,
    
    /// Performance configuration
    pub performance: PerformanceConfig,
    
    /// Security configuration
    pub security: SecurityConfig,
    
    /// Load balancer configuration
    pub load_balancer: LoadBalancerConfig,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server host
    pub host: String,
    
    /// Server port
    pub port: u16,
    
    /// Request timeout
    pub request_timeout: Duration,
    
    /// Keep alive timeout
    pub keep_alive_timeout: Duration,
    
    /// Maximum request size
    pub max_request_size: usize,
    
    /// Enable HTTP/2
    pub enable_http2: bool,
    
    /// Enable compression
    pub enable_compression: bool,
}

/// Payment processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorConfig {
    /// Default processor URL
    pub default_url: String,
    
    /// Fallback processor URL
    pub fallback_url: String,
    
    /// Default processor fee rate
    pub default_fee_rate: f64,
    
    /// Fallback processor fee rate
    pub fallback_fee_rate: f64,
    
    /// Connection timeout
    pub connection_timeout: Duration,
    
    /// Request timeout
    pub request_timeout: Duration,
    
    /// Health check interval
    pub health_check_interval: Duration,
    
    /// Maximum retries
    pub max_retries: u32,
    
    /// Retry delay
    pub retry_delay: Duration,
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Maximum number of stored payments
    pub max_payments: usize,
    
    /// Cleanup interval
    pub cleanup_interval: Duration,
    
    /// Retention period
    pub retention_period: Duration,
    
    /// Enable compression
    pub enable_compression: bool,
    
    /// Batch size for operations
    pub batch_size: usize,
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics collection
    pub enabled: bool,
    
    /// Metrics collection interval
    pub collection_interval: Duration,
    
    /// Metrics retention period
    pub retention_period: Duration,
    
    /// Enable detailed metrics
    pub detailed_metrics: bool,
    
    /// Export port (0 = disabled)
    pub export_port: u16,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: String,
    
    /// Log format
    pub format: String,
    
    /// Enable structured logging
    pub structured: bool,
    
    /// Log file path (empty = stdout)
    pub file_path: String,
    
    /// Maximum log file size
    pub max_file_size: usize,
    
    /// Number of log files to keep
    pub max_files: u32,
}

/// Performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Number of worker threads
    pub worker_threads: usize,
    
    /// Maximum concurrent connections
    pub max_connections: usize,
    
    /// TCP nodelay setting
    pub tcp_nodelay: bool,
    
    /// TCP keepalive setting
    pub tcp_keepalive: bool,
    
    /// Buffer size
    pub buffer_size: usize,
    
    /// Enable SIMD optimizations
    pub enable_simd: bool,
    
    /// Enable arena allocator
    pub enable_arena_allocator: bool,
    
    /// Enable prefetch
    pub prefetch_enabled: bool,
    
    /// Maximum payments in memory
    pub max_payments: usize,
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Rate limiting (requests per second)
    pub rate_limit_rps: u32,
    
    /// Rate limiting burst
    pub rate_limit_burst: u32,
    
    /// Request timeout
    pub request_timeout: Duration,
    
    /// Maximum request size
    pub max_request_size: usize,
    
    /// Enable request validation
    pub enable_validation: bool,
    
    /// Allowed origins for CORS
    pub allowed_origins: Vec<String>,
}

/// Load balancer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerConfig {
    /// Load balancing algorithm
    pub algorithm: String,
    
    /// Upstream servers
    pub upstream_servers: Vec<String>,
    
    /// Health check interval
    pub health_check_interval: Duration,
    
    /// Health check timeout
    pub health_check_timeout: Duration,
    
    /// Failure threshold
    pub failure_threshold: u32,
    
    /// Recovery threshold
    pub recovery_threshold: u32,
}

/// Configuration loading error
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Environment variable error: {0}")]
    EnvVar(String),
    
    #[error("Parsing error: {0}")]
    Parse(String),
    
    #[error("Validation error: {0}")]
    Validation(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("TOML parsing error: {0}")]
    Toml(#[from] toml::de::Error),
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            processors: ProcessorConfig::default(),
            database: DatabaseConfig::default(),
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
            performance: PerformanceConfig::default(),
            security: SecurityConfig::default(),
            load_balancer: LoadBalancerConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9999,
            request_timeout: Duration::from_secs(30),
            keep_alive_timeout: Duration::from_secs(60),
            max_request_size: 1024 * 1024, // 1MB
            enable_http2: true,
            enable_compression: false, // Disabled for performance
        }
    }
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            default_url: "http://payment-processor-default:8080".to_string(),
            fallback_url: "http://payment-processor-fallback:8080".to_string(),
            default_fee_rate: 0.05, // 5%
            fallback_fee_rate: 0.08, // 8%
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_millis(500),
            health_check_interval: Duration::from_secs(5),
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            max_payments: 1_000_000,
            cleanup_interval: Duration::from_secs(300), // 5 minutes
            retention_period: Duration::from_secs(3600), // 1 hour
            enable_compression: true,
            batch_size: 1000,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval: Duration::from_secs(30),
            retention_period: Duration::from_secs(3600),
            detailed_metrics: false,
            export_port: 0, // Disabled
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "pretty".to_string(),
            structured: false,
            file_path: String::new(), // stdout
            max_file_size: 10 * 1024 * 1024, // 10MB
            max_files: 5,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get(),
            max_connections: 10000,
            tcp_nodelay: true,
            tcp_keepalive: true,
            buffer_size: 8192,
            enable_simd: false,
            enable_arena_allocator: false,
            prefetch_enabled: false,
            max_payments: 1_000_000,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            rate_limit_rps: 10000,
            rate_limit_burst: 1000,
            request_timeout: Duration::from_secs(30),
            max_request_size: 1024 * 1024,
            enable_validation: true,
            allowed_origins: vec!["*".to_string()],
        }
    }
}

impl Default for LoadBalancerConfig {
    fn default() -> Self {
        Self {
            algorithm: "adaptive".to_string(),
            upstream_servers: vec![
                "http://api-server-1:9999".to_string(),
                "http://api-server-2:9999".to_string(),
            ],
            health_check_interval: Duration::from_secs(5),
            health_check_timeout: Duration::from_secs(2),
            failure_threshold: 3,
            recovery_threshold: 2,
        }
    }
}

/// Load configuration from environment variables and files
pub fn load_config() -> Result<AppConfig> {
    let mut config = AppConfig::default();
    
    // Load from environment variables
    load_from_env(&mut config)?;
    
    // Load from config file if specified
    if let Ok(config_file) = std::env::var("RINHA_CONFIG_FILE") {
        load_from_file(&mut config, &config_file)?;
    }
    
    // Apply profile-specific overrides
    apply_profile_overrides(&mut config)?;
    
    Ok(config)
}

/// Load configuration from environment variables
fn load_from_env(config: &mut AppConfig) -> Result<()> {
    // Server configuration
    if let Ok(host) = std::env::var("RINHA_HOST") {
        config.server.host = host;
    }
    
    if let Ok(port) = std::env::var("RINHA_PORT") {
        config.server.port = port.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid port: {}", e)))?;
    }
    
    if let Ok(port) = std::env::var("SERVER_PORT") {
        config.server.port = port.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid server port: {}", e)))?;
    }
    
    // Performance configuration
    if let Ok(threads) = std::env::var("WORKER_THREADS") {
        config.performance.worker_threads = threads.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid worker threads: {}", e)))?;
    }
    
    if let Ok(connections) = std::env::var("MAX_CONNECTIONS") {
        config.performance.max_connections = connections.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid max connections: {}", e)))?;
    }
    
    // Processor configuration
    if let Ok(url) = std::env::var("DEFAULT_PROCESSOR_URL") {
        config.processors.default_url = url;
    }
    
    if let Ok(url) = std::env::var("FALLBACK_PROCESSOR_URL") {
        config.processors.fallback_url = url;
    }
    
    if let Ok(fee) = std::env::var("DEFAULT_FEE_RATE") {
        config.processors.default_fee_rate = fee.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid default fee rate: {}", e)))?;
    }
    
    if let Ok(fee) = std::env::var("FALLBACK_FEE_RATE") {
        config.processors.fallback_fee_rate = fee.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid fallback fee rate: {}", e)))?;
    }
    
    // Logging configuration
    if let Ok(level) = std::env::var("RUST_LOG") {
        // Extract just the level part if it's a complex filter
        let level = if level.contains('=') {
            level.split(',').next().unwrap_or("info").split('=').last().unwrap_or("info")
        } else {
            &level
        };
        config.logging.level = level.to_string();
    }
    
    // Metrics configuration
    if let Ok(enabled) = std::env::var("METRICS_ENABLED") {
        config.metrics.enabled = enabled.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid metrics enabled: {}", e)))?;
    }
    
    // Database configuration
    if let Ok(max_payments) = std::env::var("MAX_PAYMENTS") {
        config.database.max_payments = max_payments.parse()
            .map_err(|e| ConfigError::Parse(format!("Invalid max payments: {}", e)))?;
        config.performance.max_payments = config.database.max_payments;
    }
    
    Ok(())
}

/// Load configuration from TOML file
fn load_from_file(config: &mut AppConfig, file_path: &str) -> Result<()> {
    let content = std::fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read config file: {}", file_path))?;
    
    let file_config: AppConfig = toml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", file_path))?;
    
    // Merge file config with current config
    // This is a simplified merge - in production you'd want a more sophisticated approach
    *config = file_config;
    
    Ok(())
}

/// Apply profile-specific configuration overrides
fn apply_profile_overrides(config: &mut AppConfig) -> Result<()> {
    let profile = std::env::var("RINHA_PROFILE").unwrap_or_else(|_| "production".to_string());
    
    match profile.as_str() {
        "development" => {
            config.logging.level = "debug".to_string();
            config.performance.worker_threads = 4;
            config.performance.max_connections = 1000;
            config.metrics.enabled = true;
            config.metrics.detailed_metrics = true;
            config.performance.enable_simd = false;
            config.performance.enable_arena_allocator = false;
        }
        
        "production" => {
            config.logging.level = "info".to_string();
            config.performance.worker_threads = num_cpus::get();
            config.performance.max_connections = 10000;
            config.metrics.enabled = true;
            config.metrics.detailed_metrics = false;
            config.performance.enable_simd = false;
            config.performance.enable_arena_allocator = false;
        }
        
        "benchmark" => {
            config.logging.level = "warn".to_string();
            config.metrics.enabled = false; // Disable for max performance
            config.performance.worker_threads = num_cpus::get() * 2;
            config.performance.max_connections = 50000;
            config.performance.enable_simd = true;
            config.performance.enable_arena_allocator = true;
            config.performance.prefetch_enabled = true;
            config.performance.buffer_size = 16384;
            config.server.enable_compression = false;
        }
        
        "testing" => {
            config.logging.level = "debug".to_string();
            config.server.host = "127.0.0.1".to_string();
            config.server.port = 0; // Let OS choose
            config.performance.worker_threads = 2;
            config.performance.max_connections = 100;
            config.metrics.enabled = false;
            config.database.max_payments = 10000; // Smaller for tests
            config.performance.max_payments = 10000;
        }
        
        _ => {
            return Err(ConfigError::Validation(format!("Unknown profile: {}", profile)).into());
        }
    }
    
    Ok(())
}

/// Validate configuration
pub fn validate_config(config: &AppConfig) -> Result<()> {
    // Validate server configuration
    if config.server.port == 0 && std::env::var("RINHA_PROFILE").unwrap_or_default() != "testing" {
        return Err(ConfigError::Validation("Server port cannot be 0 in non-testing mode".to_string()).into());
    }
    
    // Validate processor URLs
    if config.processors.default_url.is_empty() {
        return Err(ConfigError::Validation("Default processor URL cannot be empty".to_string()).into());
    }
    
    if config.processors.fallback_url.is_empty() {
        return Err(ConfigError::Validation("Fallback processor URL cannot be empty".to_string()).into());
    }
    
    // Validate fee rates
    if config.processors.default_fee_rate < 0.0 || config.processors.default_fee_rate > 1.0 {
        return Err(ConfigError::Validation("Default fee rate must be between 0.0 and 1.0".to_string()).into());
    }
    
    if config.processors.fallback_fee_rate < 0.0 || config.processors.fallback_fee_rate > 1.0 {
        return Err(ConfigError::Validation("Fallback fee rate must be between 0.0 and 1.0".to_string()).into());
    }
    
    if config.processors.fallback_fee_rate <= config.processors.default_fee_rate {
        return Err(ConfigError::Validation("Fallback fee rate must be higher than default fee rate".to_string()).into());
    }
    
    // Validate performance settings
    if config.performance.worker_threads == 0 {
        return Err(ConfigError::Validation("Worker threads cannot be 0".to_string()).into());
    }
    
    if config.performance.max_connections == 0 {
        return Err(ConfigError::Validation("Max connections cannot be 0".to_string()).into());
    }
    
    if config.performance.buffer_size == 0 {
        return Err(ConfigError::Validation("Buffer size cannot be 0".to_string()).into());
    }
    
    // Validate database settings
    if config.database.max_payments == 0 {
        return Err(ConfigError::Validation("Max payments cannot be 0".to_string()).into());
    }
    
    Ok(())
}

/// Get configuration value as string for debugging
pub fn config_to_string(config: &AppConfig) -> String {
    toml::to_string_pretty(config).unwrap_or_else(|_| "Failed to serialize config".to_string())
}

/// Environment variable helpers
pub mod env_helpers {
    use super::*;
    
    /// Get environment variable with default
    pub fn get_env_or_default(key: &str, default: &str) -> String {
        std::env::var(key).unwrap_or_else(|_| default.to_string())
    }
    
    /// Get environment variable as number with default
    pub fn get_env_number<T>(key: &str, default: T) -> Result<T>
    where
        T: std::str::FromStr + Copy,
        T::Err: std::fmt::Display,
    {
        match std::env::var(key) {
            Ok(value) => value.parse()
                .map_err(|e| ConfigError::Parse(format!("Failed to parse {}: {}", key, e)).into()),
            Err(_) => Ok(default),
        }
    }
    
    /// Get environment variable as boolean with default
    pub fn get_env_bool(key: &str, default: bool) -> bool {
        match std::env::var(key).as_deref() {
            Ok("true" | "1" | "yes" | "on") => true,
            Ok("false" | "0" | "no" | "off") => false,
            _ => default,
        }
    }
    
    /// Get environment variable as duration with default
    pub fn get_env_duration(key: &str, default: Duration) -> Result<Duration> {
        match std::env::var(key) {
            Ok(value) => {
                if value.ends_with("ms") {
                    let ms: u64 = value.trim_end_matches("ms").parse()
                        .map_err(|e| ConfigError::Parse(format!("Failed to parse {} as milliseconds: {}", key, e)))?;
                    Ok(Duration::from_millis(ms))
                } else if value.ends_with('s') {
                    let secs: u64 = value.trim_end_matches('s').parse()
                        .map_err(|e| ConfigError::Parse(format!("Failed to parse {} as seconds: {}", key, e)))?;
                    Ok(Duration::from_secs(secs))
                } else {
                    let secs: u64 = value.parse()
                        .map_err(|e| ConfigError::Parse(format!("Failed to parse {} as seconds: {}", key, e)))?;
                    Ok(Duration::from_secs(secs))
                }
            }
            Err(_) => Ok(default),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.processors.default_fee_rate, 0.05);
        assert_eq!(config.processors.fallback_fee_rate, 0.08);
    }

    #[test]
    fn test_config_validation() {
        let config = AppConfig::default();
        assert!(validate_config(&config).is_ok());

        let mut invalid_config = AppConfig::default();
        invalid_config.processors.default_fee_rate = 1.5; // Invalid
        assert!(validate_config(&invalid_config).is_err());
    }

    #[test]
    fn test_env_helpers() {
        use env_helpers::*;
        
        std::env::set_var("TEST_VAR", "test_value");
        assert_eq!(get_env_or_default("TEST_VAR", "default"), "test_value");
        assert_eq!(get_env_or_default("NONEXISTENT", "default"), "default");

        std::env::set_var("TEST_BOOL", "true");
        assert_eq!(get_env_bool("TEST_BOOL", false), true);
        assert_eq!(get_env_bool("NONEXISTENT_BOOL", false), false);

        std::env::remove_var("TEST_VAR");
        std::env::remove_var("TEST_BOOL");
    }

    #[test]
    fn test_profile_overrides() {
        std::env::set_var("RINHA_PROFILE", "benchmark");
        let mut config = AppConfig::default();
        apply_profile_overrides(&mut config).unwrap();
        
        assert_eq!(config.logging.level, "warn");
        assert!(!config.metrics.enabled);
        assert!(config.performance.enable_simd);
        
        std::env::remove_var("RINHA_PROFILE");
    }
}