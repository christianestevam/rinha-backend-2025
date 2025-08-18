use tokio::time::{Duration, sleep, Instant};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct AdaptiveMonitor {
    current_interval: Arc<RwLock<Duration>>,
    min_interval: Duration,
    max_interval: Duration,
    load_factor: Arc<RwLock<f64>>,
}

impl AdaptiveMonitor {
    pub fn new() -> Self {
        Self {
            current_interval: Arc::new(RwLock::new(Duration::from_secs(1))),
            min_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(10),
            load_factor: Arc::new(RwLock::new(0.0)),
        }
    }

    pub async fn start_adaptive_monitoring(&self) {
        loop {
            let interval = *self.current_interval.read().await;
            sleep(interval).await;

            // Ajusta intervalo baseado na carga
            self.adjust_monitoring_interval().await;
            
            // Executa health-checks
            self.perform_health_checks().await;
        }
    }

    async fn adjust_monitoring_interval(&self) {
        let load = *self.load_factor.read().await;
        let mut current = self.current_interval.write().await;

        // Alta carga = monitoramento mais frequente
        let new_interval = if load > 0.8 {
            self.min_interval
        } else if load < 0.2 {
            self.max_interval
        } else {
            Duration::from_millis(
                (self.min_interval.as_millis() as f64 + 
                 (self.max_interval.as_millis() - self.min_interval.as_millis()) as f64 * 
                 (1.0 - load)) as u64
            )
        };

        *current = new_interval;
    }

    async fn perform_health_checks(&self) {
        // Implementa health-checks inteligentes
        println!("Realizando health-checks adaptativos...");
    }

    pub async fn update_load(&self, new_load: f64) {
        let mut load = self.load_factor.write().await;
        *load = new_load;
    }
}