//! Configuration management system for the Rinha Backend challenge
//! 
//! This module provides comprehensive configuration management with
//! environment variable support, validation, and performance tuning.

pub mod env;

// Re-export configuration types
pub use env::{
    AppConfig,
    ServerConfig,
    ProcessorConfig,
    DatabaseConfig,
    MetricsConfig,
    LoggingConfig,
    PerformanceConfig,
    SecurityConfig,
    load_config,
    validate_config,
    ConfigError
};

use std::sync::OnceLock;
use anyhow::Result;

/// Configuration profile types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigProfile {
    /// Development profile with debug features
    Development,
    
    /// Production profile optimized for performance
    Production,
    
    /// Benchmark profile with maximum performance
    Benchmark,
    
    /// Testing profile with smaller limits
    Testing,
}

impl ConfigProfile {
    /// Convert profile to string
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigProfile::Development => "development",
            ConfigProfile::Production => "production",
            ConfigProfile::Benchmark => "benchmark",
            ConfigProfile::Testing => "testing",
        }
    }
    
    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "development" => Some(ConfigProfile::Development),
            "production" => Some(ConfigProfile::Production),
            "benchmark" => Some(ConfigProfile::Benchmark),
            "testing" => Some(ConfigProfile::Testing),
            _ => None,
        }
    }
}

/// Global configuration instance
static GLOBAL_CONFIG: OnceLock<AppConfig> = OnceLock::new();

/// Initialize global configuration from environment
pub fn init_config() -> Result<&'static AppConfig> {
    GLOBAL_CONFIG.get_or_try_init(|| {
        let config = load_config()?;
        validate_config(&config)?;
        Ok(config)
    })
}

/// Get global configuration (must call init_config first)
pub fn get_config() -> &'static AppConfig {
    GLOBAL_CONFIG.get()
        .expect("Configuration not initialized. Call init_config() first.")
}

/// Configuration builder for testing and custom setups
pub struct ConfigBuilder {
    config: AppConfig,
}

impl ConfigBuilder {
    /// Create new config builder with defaults
    pub fn new() -> Self {
        Self {
            config: AppConfig::default(),
        }
    }

    /// Set server configuration
    pub fn with_server(mut self, server: ServerConfig) -> Self {
        self.config.server = server;
        self
    }

    /// Set processor configuration
    pub fn with_processors(mut self, processors: ProcessorConfig) -> Self {
        self.config.processors = processors;
        self
    }

    /// Set database configuration
    pub fn with_database(mut self, database: DatabaseConfig) -> Self {
        self.config.database = database;
        self
    }

    /// Set metrics configuration
    pub fn with_metrics(mut self, metrics: MetricsConfig) -> Self {
        self.config.metrics = metrics;
        self
    }

    /// Set logging configuration
    pub fn with_logging(mut self, logging: LoggingConfig) -> Self {
        self.config.logging = logging;
        self
    }

    /// Set performance configuration
    pub fn with_performance(mut self, performance: PerformanceConfig) -> Self {
        self.config.performance = performance;
        self
    }

    /// Set security configuration
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.config.security = security;
        self
    }

    /// Enable development mode (relaxed settings)
    pub fn development_mode(mut self) -> Self {
        self.config.server.host = "0.0.0.0".to_string();
        self.config.server.port = 9999;
        self.config.logging.level = "debug".to_string();
        self.config.metrics.enabled = true;
        self.config.performance.worker_threads = 4;
        self.config.performance.max_connections = 1000;
        self
    }

    /// Enable production mode (optimized settings)
    pub fn production_mode(mut self) -> Self {
        self.config.server.host = "0.0.0.0".to_string();
        self.config.server.port = 9999;
        self.config.logging.level = "info".to_string();
        self.config.metrics.enabled = true;
        self.config.performance.worker_threads = num_cpus::get();
        self.config.performance.max_connections = 10000;
        self.config.performance.tcp_nodelay = true;
        self.config.performance.tcp_keepalive = true;
        self.config.performance.buffer_size = 8192;
        self
    }

    /// Enable benchmarking mode (maximum performance)
    pub fn benchmark_mode(mut self) -> Self {
        self.config.server.host = "0.0.0.0".to_string();
        self.config.server.port = 9999;
        self.config.logging.level = "warn".to_string(); // Minimal logging
        self.config.metrics.enabled = false; // Disable metrics for max performance
        self.config.performance.worker_threads = num_cpus::get() * 2;
        self.config.performance.max_connections = 50000;
        self.config.performance.tcp_nodelay = true;
        self.config.performance.tcp_keepalive = true;
        self.config.performance.buffer_size = 16384;
        self.config.performance.enable_simd = true;
        self.config.performance.enable_arena_allocator = true;
        self.config.performance.prefetch_enabled = true;
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<AppConfig> {
        validate_config(&self.config)?;
        Ok(self.config)
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigProfile {
    /// Create configuration for the specified profile
    pub fn create_config(self) -> Result<AppConfig> {
        let builder = ConfigBuilder::new();
        
        let config = match self {
            ConfigProfile::Development => builder.development_mode().build()?,
            ConfigProfile::Testing => builder
                .development_mode()
                .with_server(ServerConfig {
                    host: "127.0.0.1".to_string(),
                    port: 0, // Let OS choose port for testing
                    ..Default::default()
                })
                .build()?,
            ConfigProfile::Production => builder.production_mode().build()?,
            ConfigProfile::Benchmark => builder.benchmark_mode().build()?,
        };

        Ok(config)
    }

    /// Get profile from environment variable
    pub fn from_env() -> Self {
        match std::env::var("RINHA_PROFILE").as_deref() {
            Ok("development" | "dev") => ConfigProfile::Development,
            Ok("testing" | "test") => ConfigProfile::Testing,
            Ok("production" | "prod") => ConfigProfile::Production,
            Ok("benchmark" | "bench") => ConfigProfile::Benchmark,
            _ => ConfigProfile::Production, // Default to production
        }
    }
}

/// Configuration validation utilities
pub mod validation {
    use super::*;

    /// Validate port number
    pub fn validate_port(port: u16) -> Result<()> {
        if port == 0 {
            return Ok(()); // Let OS choose
        }
        
        if port < 1024 {
            return Err(anyhow::anyhow!("Port {} requires root privileges", port));
        }
        
        Ok(())
    }

    /// Validate URL format
    pub fn validate_url(url: &str) -> Result<()> {
        url::Url::parse(url)
            .map_err(|e| anyhow::anyhow!("Invalid URL '{}': {}", url, e))?;
        Ok(())
    }

    /// Validate memory size (in MB)
    pub fn validate_memory_mb(memory_mb: usize) -> Result<()> {
        if memory_mb < 64 {
            return Err(anyhow::anyhow!("Memory too low: {}MB (minimum 64MB)", memory_mb));
        }
        
        if memory_mb > 2048 {
            return Err(anyhow::anyhow!("Memory too high: {}MB (maximum 2048MB)", memory_mb));
        }
        
        Ok(())
    }

    /// Validate thread count
    pub fn validate_thread_count(threads: usize) -> Result<()> {
        if threads == 0 {
            return Err(anyhow::anyhow!("Thread count cannot be zero"));
        }
        
        let max_threads = num_cpus::get() * 4; // Allow up to 4x CPU cores
        if threads > max_threads {
            return Err(anyhow::anyhow!(
                "Thread count too high: {} (maximum {})", 
                threads, 
                max_threads
            ));
        }
        
        Ok(())
    }

    /// Validate connection count
    pub fn validate_connection_count(connections: usize) -> Result<()> {
        if connections == 0 {
            return Err(anyhow::anyhow!("Connection count cannot be zero"));
        }
        
        if connections > 100000 {
            return Err(anyhow::anyhow!(
                "Connection count too high: {} (maximum 100000)", 
                connections
            ));
        }
        
        Ok(())
    }
}

/// Environment configuration helpers
pub mod environment {
    use std::env;

    /// Get environment variable with default
    pub fn get_env_or_default(key: &str, default: &str) -> String {
        env::var(key).unwrap_or_else(|_| default.to_string())
    }

    /// Get environment variable as number with default
    pub fn get_env_number<T>(key: &str, default: T) -> T
    where
        T: std::str::FromStr + Copy,
    {
        env::var(key)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    /// Get environment variable as boolean with default
    pub fn get_env_bool(key: &str, default: bool) -> bool {
        match env::var(key).as_deref() {
            Ok("true" | "1" | "yes" | "on") => true,
            Ok("false" | "0" | "no" | "off") => false,
            _ => default,
        }
    }

    /// Check if running in development mode
    pub fn is_development() -> bool {
        matches!(
            env::var("RINHA_PROFILE").as_deref(),
            Ok("development" | "dev")
        )
    }

    /// Check if running in production mode
    pub fn is_production() -> bool {
        matches!(
            env::var("RINHA_PROFILE").as_deref(),
            Ok("production" | "prod") | Err(_) // Default to production
        )
    }

    /// Check if benchmarking mode is enabled
    pub fn is_benchmark() -> bool {
        matches!(
            env::var("RINHA_PROFILE").as_deref(),
            Ok("benchmark" | "bench")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::new()
            .development_mode()
            .build()
            .expect("Config should build successfully");

        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.logging.level, "debug");
    }

    #[test]
    fn test_config_profiles() {
        let dev_config = ConfigProfile::Development
            .create_config()
            .expect("Dev config should build");
        
        let prod_config = ConfigProfile::Production
            .create_config()
            .expect("Prod config should build");

        // Development should have debug logging
        assert_eq!(dev_config.logging.level, "debug");
        
        // Production should have info logging
        assert_eq!(prod_config.logging.level, "info");
    }

    #[test]
    fn test_validation() {
        use validation::*;

        // Valid cases
        assert!(validate_port(9999).is_ok());
        assert!(validate_port(0).is_ok()); // OS chooses
        assert!(validate_url("http://localhost:8080").is_ok());
        assert!(validate_memory_mb(256).is_ok());
        assert!(validate_thread_count(4).is_ok());
        assert!(validate_connection_count(1000).is_ok());

        // Invalid cases
        assert!(validate_port(80).is_err()); // Requires root
        assert!(validate_url("not-a-url").is_err());
        assert!(validate_memory_mb(32).is_err()); // Too low
        assert!(validate_memory_mb(4096).is_err()); // Too high
        assert!(validate_thread_count(0).is_err()); // Zero threads
        assert!(validate_connection_count(0).is_err()); // Zero connections
    }

    #[test]
    fn test_environment_helpers() {
        use environment::*;

        // Test default values
        assert_eq!(get_env_or_default("NON_EXISTENT_VAR", "default"), "default");
        assert_eq!(get_env_number::<u16>("NON_EXISTENT_PORT", 9999), 9999);
        assert_eq!(get_env_bool("NON_EXISTENT_BOOL", true), true);
    }

    #[test]
    fn test_benchmark_mode() {
        let config = ConfigBuilder::new()
            .benchmark_mode()
            .build()
            .expect("Benchmark config should build");

        assert_eq!(config.logging.level, "warn"); // Minimal logging
        assert!(!config.metrics.enabled); // Metrics disabled for performance
        assert!(config.performance.enable_simd);
        assert!(config.performance.enable_arena_allocator);
    }
}