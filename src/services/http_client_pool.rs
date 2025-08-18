use reqwest::Client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub struct HttpClientPool {
    clients: Vec<Client>,
    current_index: AtomicUsize,
}

impl HttpClientPool {
    pub fn new(pool_size: usize) -> Self {
        let mut clients = Vec::with_capacity(pool_size);
        
        for _ in 0..pool_size {
            let client = Client::builder()
                .timeout(Duration::from_millis(1500))
                .pool_max_idle_per_host(20) // Mantém conexões vivas
                .pool_idle_timeout(Duration::from_secs(30))
                .tcp_keepalive(Duration::from_secs(60))
                .build()
                .expect("Failed to create HTTP client");
            
            clients.push(client);
        }

        Self {
            clients,
            current_index: AtomicUsize::new(0),
        }
    }

    pub fn get_client(&self) -> &Client {
        let index = self.current_index.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[index]
    }
}