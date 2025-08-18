use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

type L1Cache = Arc<RwLock<HashMap<String, (serde_json::Value, Instant)>>>;

type L2Cache = Arc<RwLock<HashMap<String, (serde_json::Value, Instant)>>>;

pub struct MultiLayerCache {
    l1_cache: L1Cache,
    l2_cache: L2Cache,
    l1_ttl: Duration,
    l2_ttl: Duration,
    l1_max_size: usize,
}

impl MultiLayerCache {
    pub fn new() -> Self {
        Self {
            l1_cache: Arc::new(RwLock::new(HashMap::new())),
            l2_cache: Arc::new(RwLock::new(HashMap::new())),
            l1_ttl: Duration::from_millis(100),
            l2_ttl: Duration::from_secs(5),
            l1_max_size: 1000,
        }
    }

    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        // Verifica L1 primeiro
        {
            let l1 = self.l1_cache.read().await;
            if let Some((value, timestamp)) = l1.get(key) {
                if timestamp.elapsed() < self.l1_ttl {
                    return Some(value.clone());
                }
            }
        }

        // Verifica L2
        {
            let l2 = self.l2_cache.read().await;
            if let Some((value, timestamp)) = l2.get(key) {
                if timestamp.elapsed() < self.l2_ttl {
                    // Promove para L1
                    self.promote_to_l1(key, value.clone()).await;
                    return Some(value.clone());
                }
            }
        }

        None
    }

    pub async fn set(&self, key: String, value: serde_json::Value) {
        // Sempre insere no L1
        let mut l1 = self.l1_cache.write().await;
        
        // Eviction policy: remove o mais antigo se exceder tamanho
        if l1.len() >= self.l1_max_size {
            self.evict_oldest_l1(&mut l1).await;
        }
        
        l1.insert(key, (value, Instant::now()));
    }

    async fn promote_to_l1(&self, key: &str, value: serde_json::Value) {
        let mut l1 = self.l1_cache.write().await;
        l1.insert(key.to_string(), (value, Instant::now()));
    }

    async fn evict_oldest_l1(&self, l1: &mut HashMap<String, (serde_json::Value, Instant)>) {
        if let Some(oldest_key) = l1.iter()
            .min_by_key(|(_, (_, timestamp))| timestamp)
            .map(|(key, _)| key.clone()) {
            
            if let Some((value, _)) = l1.remove(&oldest_key) {
                // Move para L2
                let mut l2 = self.l2_cache.write().await;
                l2.insert(oldest_key, (value, Instant::now()));
            }
        }
    }
}