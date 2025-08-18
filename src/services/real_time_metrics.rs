use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use std::collections::{HashMap, VecDeque};

pub struct MetricsCollector {
    latencies: Arc<RwLock<VecDeque<Duration>>>,
    error_rates: Arc<RwLock<VecDeque<f64>>>,
    throughput: Arc<RwLock<VecDeque<u32>>>,
    processor_performance: Arc<RwLock<HashMap<String, ProcessorMetrics>>>,
}

#[derive(Clone)]
struct ProcessorMetrics {
    success_rate: f64,
    avg_latency: Duration,
    fee_efficiency: f64, // Lucro por operação
    last_update: Instant,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            latencies: Arc::new(RwLock::new(VecDeque::new())),
            error_rates: Arc::new(RwLock::new(VecDeque::new())),
            throughput: Arc::new(RwLock::new(VecDeque::new())),
            processor_performance: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn record_request(&self, latency: Duration, success: bool, processor: &str, fee: u64) {
        // Registra latência
        {
            let mut latencies = self.latencies.write().await;
            latencies.push_back(latency);
            if latencies.len() > 1000 { latencies.pop_front(); }
        }

        // Atualiza métricas do processor
        {
            let mut processors = self.processor_performance.write().await;
            let metrics = processors.entry(processor.to_string()).or_insert(ProcessorMetrics {
                success_rate: 1.0,
                avg_latency: latency,
                fee_efficiency: fee as f64,
                last_update: Instant::now(),
            });

            // Média móvel exponencial
            let alpha = 0.1; // Fator de suavização
            metrics.avg_latency = Duration::from_millis(
                ((metrics.avg_latency.as_millis() as f64 * (1.0 - alpha)) + 
                 (latency.as_millis() as f64 * alpha)) as u64
            );
            
            if success {
                metrics.success_rate = metrics.success_rate * (1.0 - alpha) + alpha;
                metrics.fee_efficiency = metrics.fee_efficiency * (1.0 - alpha) + (fee as f64 * alpha);
            } else {
                metrics.success_rate = metrics.success_rate * (1.0 - alpha);
            }
            
            metrics.last_update = Instant::now();
        }
    }

    pub async fn get_p99_latency(&self) -> Duration {
        let latencies = self.latencies.read().await;
        if latencies.is_empty() { return Duration::from_millis(0); }
        
        let mut sorted: Vec<_> = latencies.iter().cloned().collect();
        sorted.sort();
        let p99_index = (sorted.len() as f64 * 0.99) as usize;
        sorted.get(p99_index).cloned().unwrap_or(Duration::from_millis(0))
    }

    pub async fn get_best_processor(&self) -> Option<String> {
        let processors = self.processor_performance.read().await;
        
        let mut best_score = f64::MIN;
        let mut best_processor = None;
        
        for (name, metrics) in processors.iter() {
            // Score = success_rate * fee_efficiency / latency_ms
            let score = metrics.success_rate * metrics.fee_efficiency / 
                       (metrics.avg_latency.as_millis() as f64 + 1.0);
            
            if score > best_score {
                best_score = score;
                best_processor = Some(name.clone());
            }
        }
        
        best_processor
    }
}