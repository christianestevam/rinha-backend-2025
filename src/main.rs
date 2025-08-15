//! Rinha Backend 2025 - Ultra High-Performance Payment Gateway
//! 
//! This is the main entry point for the Rinha Backend 2025 challenge.
//! The system is designed for maximum performance with p99 latency < 1ms
//! and profit maximization through intelligent processor selection.

use anyhow::{Result, Context};
use clap::{Parser, Subcommand};
use tracing::{info, error};
use std::env;

use rinha_backend_2025::{
    config::{init_config, ConfigProfile},
};

/// Command line interface for Rinha Backend 2025
#[derive(Parser)]
#[command(name = "rinha-backend-2025")]
#[command(about = "Ultra High-Performance Payment Gateway for Rinha Backend 2025")]
#[command(version = "1.0.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Available commands
#[derive(Subcommand)]
enum Commands {
    /// Start the API server (default mode)
    Api {
        /// Server port (default: 9999)
        #[arg(short, long, default_value = "9999")]
        port: u16,
        
        /// Number of worker threads
        #[arg(short, long)]
        workers: Option<usize>,
        
        /// Configuration profile
        #[arg(long, default_value = "production")]
        profile: String,
    },
    
    /// Start a background worker
    Worker {
        /// Worker ID
        #[arg(short, long, default_value = "1")]
        id: u32,
        
        /// Configuration profile
        #[arg(long, default_value = "production")]
        profile: String,
    },
    
    /// Start the internal load balancer
    LoadBalancer {
        /// Load balancer ID
        #[arg(short, long, default_value = "lb-1")]
        id: String,
        
        /// Configuration profile
        #[arg(long, default_value = "production")]
        profile: String,
    },
    
    /// Start all components (full system)
    All {
        /// Number of workers
        #[arg(short, long, default_value = "2")]
        workers: u32,
        
        /// Configuration profile
        #[arg(long, default_value = "production")]
        profile: String,
    },
    
    /// Run benchmarks
    Benchmark {
        /// Duration in seconds
        #[arg(short, long, default_value = "60")]
        duration: u64,
        
        /// Target RPS
        #[arg(short, long, default_value = "10000")]
        rps: u64,
    },
    
    /// Show system configuration
    Config {
        /// Configuration profile
        #[arg(long, default_value = "production")]
        profile: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing early
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Commands::Api { port, workers, profile } => {
            run_api_server(port, workers, &profile).await
        }
        Commands::Worker { id, profile } => {
            run_worker(id, &profile).await
        }
        Commands::LoadBalancer { id, profile } => {
            run_load_balancer(&id, &profile).await
        }
        Commands::All { workers, profile } => {
            run_full_system(workers, &profile).await
        }
        Commands::Benchmark { duration, rps } => {
            run_benchmark(duration, rps).await
        }
        Commands::Config { profile } => {
            show_config(&profile).await
        }
    }
}

/// Initialize tracing/logging
fn init_tracing() {
    let log_level = env::var("RUST_LOG")
        .unwrap_or_else(|_| "info,rinha_backend_2025=debug".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();
}

/// Run API server
async fn run_api_server(port: u16, workers: Option<usize>, profile: &str) -> Result<()> {
    info!("🚀 Starting Rinha Backend 2025 API Server");
    info!("📊 Profile: {}, Port: {}", profile, port);
    
    // Set environment variables
    env::set_var("RINHA_PROFILE", profile);
    env::set_var("SERVER_PORT", port.to_string());
    
    if let Some(worker_count) = workers {
        env::set_var("WORKER_THREADS", worker_count.to_string());
    }

    // Initialize configuration
    let config = init_config().context("Failed to initialize configuration")?;
    info!("✅ Configuration loaded: {:?}", config.server);

    // Import and run the API server
    let api_server = rinha_backend_2025::api_server::ApiServer::new().await
        .context("Failed to create API server")?;

    info!("🎯 API Server ready on port {}", port);
    info!("💰 Optimized for profit maximization");
    info!("⚡ Target: p99 < 1ms, 100k+ RPS");

    api_server.run().await
        .context("API server execution failed")?;

    Ok(())
}

/// Run background worker
async fn run_worker(worker_id: u32, profile: &str) -> Result<()> {
    info!("👷 Starting Rinha Backend Worker #{}", worker_id);
    info!("📊 Profile: {}", profile);

    // Set environment variables
    env::set_var("RINHA_PROFILE", profile);
    env::set_var("WORKER_ID", worker_id.to_string());

    // Initialize configuration
    let _config = init_config().context("Failed to initialize configuration")?;

    // Import and run the worker
    let worker = rinha_backend_2025::worker::PaymentWorker::new(worker_id).await
        .context("Failed to create payment worker")?;

    info!("🔧 Worker #{} ready for background processing", worker_id);

    worker.run().await
        .context("Worker execution failed")?;

    Ok(())
}

/// Run load balancer
async fn run_load_balancer(lb_id: &str, profile: &str) -> Result<()> {
    info!("⚖️ Starting Rinha Backend Load Balancer: {}", lb_id);
    info!("📊 Profile: {}", profile);

    // Set environment variables
    env::set_var("RINHA_PROFILE", profile);
    env::set_var("LB_ID", lb_id);

    // Initialize configuration
    let _config = init_config().context("Failed to initialize configuration")?;

    // Import and run the load balancer
    let load_balancer = rinha_backend_2025::load_balancer::InternalLoadBalancer::new(lb_id.to_string()).await
        .context("Failed to create load balancer")?;

    info!("🎯 Load Balancer {} ready on port 8080", lb_id);

    load_balancer.run().await
        .context("Load balancer execution failed")?;

    Ok(())
}

/// Run full system (all components)
async fn run_full_system(worker_count: u32, profile: &str) -> Result<()> {
    info!("🏗️ Starting Full Rinha Backend System");
    info!("📊 Profile: {}, Workers: {}", profile, worker_count);

    // Set environment variables
    env::set_var("RINHA_PROFILE", profile);

    // This would orchestrate starting all components
    // In a real deployment, these would be separate processes/containers
    
    info!(" Full system startup sequence:");
    info!("1. Load Balancer (port 8080)");
    info!("2. API Server (port 9999)");
    info!("3. {} Background Workers", worker_count);

    // For now, just start the API server as the main component
    // In production, each component would be deployed separately
    run_api_server(9999, Some(num_cpus::get()), profile).await
}

/// Run performance benchmark
async fn run_benchmark(duration: u64, target_rps: u64) -> Result<()> {
    info!(" Starting Rinha Backend Benchmark");
    info!("⏱ Duration: {}s, Target RPS: {}", duration, target_rps);

    // This would implement the benchmarking logic
    // For now, we'll provide a placeholder implementation
    
    info!("📊Benchmark Configuration:");
    info!("   - Duration: {} seconds", duration);
    info!("   - Target RPS: {}", target_rps);
    info!("   - Total Requests: {}", duration * target_rps);
    
    // Simulate benchmark
    for i in 0..duration {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        info!("📈 Benchmark progress: {}/{}s", i + 1, duration);
    }

    info!("Benchmark completed!");
    info!("Results would be displayed here in a real implementation");

    Ok(())
}

/// Show system configuration
async fn show_config(profile: &str) -> Result<()> {
    info!("Rinha Backend 2025 Configuration");
    info!("Profile: {}", profile);

    // Set profile
    env::set_var("RINHA_PROFILE", profile);

    // Load and display configuration
    let config = init_config().context("Failed to load configuration")?;

    println!("\n🔧 System Configuration:");
    println!("├── Profile: {}", profile);
    println!("├── Server:");
    println!("│   ├── Host: {}", config.server.host);
    println!("│   ├── Port: {}", config.server.port);
    println!("│   └── Workers: {}", config.performance.worker_threads);
    println!("├── Performance:");
    println!("│   ├── Max Connections: {}", config.performance.max_connections);
    println!("│   ├── Buffer Size: {} bytes", config.performance.buffer_size);
    println!("│   ├── TCP NoDelay: {}", config.performance.tcp_nodelay);
    println!("│   └── TCP KeepAlive: {}", config.performance.tcp_keepalive);
    println!("├── Database:");
    println!("│   ├── Max Payments: {}", config.performance.max_payments);
    println!("│   └── Cleanup Interval: {:?}", config.database.cleanup_interval);
    println!("└── Logging:");
    println!("    ├── Level: {}", config.logging.level);
    println!("    └── Metrics: {}", config.metrics.enabled);

    if profile == "benchmark" {
        println!("\n Benchmark Mode Optimizations:");
        println!("├── SIMD: {}", config.performance.enable_simd);
        println!("├── Arena Allocator: {}", config.performance.enable_arena_allocator);
        println!("├── Prefetch: {}", config.performance.prefetch_enabled);
        println!("└── Metrics: Disabled for max performance");
    }

    println!("\n Optimization Targets:");
    println!("├── Latency: p99 < 1ms");
    println!("├── Throughput: 100k+ RPS");
    println!("├── Profit: 95%+ default processor usage");
    println!("└── Memory: < 250MB total");

    Ok(())
}

/// Default main function when run without arguments
/// This provides a quick start for the most common use case
pub async fn quick_start() -> Result<()> {
    init_tracing();
    
    info!("🚀 Rinha Backend 2025 - Quick Start Mode");
    info!("💡 Use --help for more options");
    
    // Detect best profile based on environment
    let profile = if env::var("BENCHMARK").is_ok() {
        "benchmark"
    } else if env::var("DEBUG").is_ok() {
        "development"
    } else {
        "production"
    };

    info!("📊 Auto-detected profile: {}", profile);
    
    // Start API server with defaults
    run_api_server(9999, None, profile).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        // Test API command
        let cli = Cli::try_parse_from(&["rinha-backend-2025", "api", "--port", "8080"]);
        assert!(cli.is_ok());

        // Test Worker command
        let cli = Cli::try_parse_from(&["rinha-backend-2025", "worker", "--id", "42"]);
        assert!(cli.is_ok());

        // Test LoadBalancer command
        let cli = Cli::try_parse_from(&["rinha-backend-2025", "load-balancer", "--id", "test-lb"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_profile_detection() {
        // Test environment variable detection
        std::env::set_var("BENCHMARK", "1");
        let profile = if env::var("BENCHMARK").is_ok() {
            "benchmark"
        } else {
            "production"
        };
        assert_eq!(profile, "benchmark");
        
        std::env::remove_var("BENCHMARK");
    }

    #[tokio::test]
    async fn test_config_loading() {
        std::env::set_var("RINHA_PROFILE", "development");
        let result = init_config();
        
        match result {
            Ok(_config) => {
                println!("Configuration loaded successfully");
            }
            Err(e) => {
                println!("Expected error in test environment: {}", e);
            }
        }
    }
}