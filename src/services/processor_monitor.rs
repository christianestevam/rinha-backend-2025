use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

pub async fn monitor_processors(processor_status: Arc<Mutex<String>>) {
    loop {
        // Simula alternância entre processors a cada 10s
        {
            let mut status = processor_status.lock().unwrap();
            *status = if *status == "default" { "fallback".to_string() } else { "default".to_string() };
        }
        sleep(Duration::from_secs(10)).await;
    }
}