use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Clone)]
struct AccessPattern {
    frequency: u32,
    last_access: Instant,
    access_intervals: VecDeque<Duration>,
}

pub struct PredictiveCache {
    cache: Arc<RwLock<HashMap<String, (serde_json::Value, Instant)>>>,
    access_patterns: Arc<RwLock<HashMap<String, AccessPattern>>>,
    max_size: usize,
    ttl: Duration,
}

impl PredictiveCache {
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
            max_size,
            ttl,
        }
    }

    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        // Atualiza padrão de acesso
        self.update_access_pattern(key).await;
        
        let cache = self.cache.read().await;
        if let Some((value, timestamp)) = cache.get(key) {
            if timestamp.elapsed() < self.ttl {
                return Some(value.clone());
            }
        }
        None
    }

    pub async fn set(&self, key: String, value: serde_json::Value) {
        let mut cache = self.cache.write().await;
        
        // Evict usando algoritmo preditivo
        if cache.len() >= self.max_size {
            self.predictive_evict(&mut cache).await;
        }
        
        cache.insert(key.clone(), (value, Instant::now()));
        self.update_access_pattern(&key).await;
    }

    async fn update_access_pattern(&self, key: &str) {
        let mut patterns = self.access_patterns.write().await;
        let now = Instant::now();
        
        let pattern = patterns.entry(key.to_string()).or_insert(AccessPattern {
            frequency: 0,
            last_access: now,
            access_intervals: VecDeque::new(),
        });
        
        // Calcula intervalo desde último acesso
        let interval = now.duration_since(pattern.last_access);
        pattern.access_intervals.push_back(interval);
        
        // Mantém apenas os últimos 10 intervalos
        if pattern.access_intervals.len() > 10 {
            pattern.access_intervals.pop_front();
        }
        
        pattern.frequency += 1;
        pattern.last_access = now;
    }

    async fn predictive_evict(&self, cache: &mut HashMap<String, (serde_json::Value, Instant)>) {
        let patterns = self.access_patterns.read().await;
        
        // Calcula score de cada item (menor = mais provável de ser removido)
        let mut scores: Vec<(String, f64)> = cache.keys()
            .map(|key| {
                let score = if let Some(pattern) = patterns.get(key) {
                    // Score baseado em frequência e previsão de próximo acesso
                    let avg_interval = pattern.access_intervals.iter()
                        .map(|d| d.as_secs_f64())
                        .sum::<f64>() / pattern.access_intervals.len() as f64;
                    
                    let time_since_access = pattern.last_access.elapsed().as_secs_f64();
                    let predicted_next_access = avg_interval - time_since_access;
                    
                    // Maior frequência + acesso previsto em breve = maior score
                    pattern.frequency as f64 + predicted_next_access.max(0.0)
                } else {
                    0.0 // Sem padrão = score baixo
                };
                
                (key.clone(), score)
            })
            .collect();
        
        // Remove o item com menor score
        scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        if let Some((key_to_remove, _)) = scores.first() {
            cache.remove(key_to_remove);
        }
    }
}