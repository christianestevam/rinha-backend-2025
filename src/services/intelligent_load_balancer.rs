use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct InstanceHealth {
    pub latency_ms: u64,
    pub success_rate: f64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub active_connections: u32,
    pub last_updated: Instant,
}

#[derive(Debug, Clone)]
pub struct ProcessorInstance {
    pub id: String,
    pub url: String,
    pub weight: f64,
    pub health: InstanceHealth,
}

pub struct IntelligentLoadBalancer {
    instances: Arc<RwLock<HashMap<String, InstanceHealth>>>,
    weights: Arc<RwLock<HashMap<String, f64>>>,
}

impl IntelligentLoadBalancer {
    pub fn new() -> Self {
        Self {
            instances: Arc::new(RwLock::new(HashMap::new())),
            weights: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn select_best_instance(&self, available_instances: &[String]) -> Option<String> {
        let instances = self.instances.read().await;
        let weights = self.weights.read().await;

        let mut best_instance: Option<String> = None;
        let mut best_score = 0.0f64;

        for instance_id in available_instances {
            if let Some(health) = instances.get(instance_id) {
                let weight = weights.get(instance_id).copied().unwrap_or(1.0);
                let score = self.calculate_health_score(health) * weight;

                if score > best_score {
                    best_score = score;
                    best_instance = Some(instance_id.clone());
                }
            }
        }

        best_instance
    }

    fn calculate_health_score(&self, health: &InstanceHealth) -> f64 {
        let latency_score = 1.0 / (1.0 + health.latency_ms as f64 / 100.0);
        let success_score = health.success_rate;
        let load_score = 1.0 - (health.active_connections as f64 / 1000.0).min(1.0);
        let cpu_score = 1.0 - health.cpu_usage.min(1.0);

        latency_score * 0.3 + success_score * 0.3 + load_score * 0.2 + cpu_score * 0.2
    }

    pub async fn update_instance_health(&self, instance_id: String, health: InstanceHealth) {
        let mut instances = self.instances.write().await;
        instances.insert(instance_id.clone(), health.clone());
        
        let mut weights = self.weights.write().await;
        let new_weight = self.calculate_health_score(&health);
        weights.insert(instance_id, new_weight);
    }

    pub async fn get_instance_weight(&self, instance_id: &str) -> f64 {
        let weights = self.weights.read().await;
        weights.get(instance_id).copied().unwrap_or(1.0)
    }
}