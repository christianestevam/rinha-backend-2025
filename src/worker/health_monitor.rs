//! Health monitoring for workers

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use anyhow::Result;
use serde::{Serialize, Deserialize};

/// Health status monitor
#[derive(Debug)]
pub struct HealthMonitor {
    is_healthy: AtomicBool,
    last_check: AtomicU64,
    cpu_usage: AtomicU64,
    memory_usage: AtomicU64,
    active_connections: AtomicU64,
}

impl HealthMonitor {
    /// Create new health monitor
    pub fn new() -> Self {
        Self {
            is_healthy: AtomicBool::new(true),
            last_check: AtomicU64::new(0),
            cpu_usage: AtomicU64::new(0),
            memory_usage: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
        }
    }

    /// Check health status
    pub async fn check_health(&self) -> Result<HealthStatus> {
        let now = Instant::now();
        self.last_check.store(now.elapsed().as_secs(), Ordering::Relaxed);

        // Simulate health checks
        let cpu = self.cpu_usage.load(Ordering::Relaxed);
        let memory = self.memory_usage.load(Ordering::Relaxed);
        let connections = self.active_connections.load(Ordering::Relaxed);

        let is_healthy = cpu < 90 && memory < 85 && connections < 10000;
        self.is_healthy.store(is_healthy, Ordering::Relaxed);

        Ok(HealthStatus {
            is_healthy,
            cpu_usage_percent: cpu,
            memory_usage_percent: memory,
            active_connections: connections,
            last_check_secs: self.last_check.load(Ordering::Relaxed),
        })
    }

    /// Update metrics
    pub fn update_metrics(&self, cpu: u64, memory: u64, connections: u64) {
        self.cpu_usage.store(cpu, Ordering::Relaxed);
        self.memory_usage.store(memory, Ordering::Relaxed);
        self.active_connections.store(connections, Ordering::Relaxed);
    }

    /// Check if healthy
    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Health status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub cpu_usage_percent: u64,
    pub memory_usage_percent: u64,
    pub active_connections: u64,
    pub last_check_secs: u64,
}
