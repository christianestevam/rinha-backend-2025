//! Inter-Process Communication module for high-performance messaging
//! 
//! This module provides ultra-fast IPC mechanisms using Unix domain sockets,
//! shared memory, and lock-free message passing for the Rinha Backend challenge.

pub mod message;
pub mod unix_socket;

// Re-export main IPC types
pub use message::{
    Message,
    MessageType,
    MessagePriority,
    MessageHeader,
    MessagePayload,
    PaymentMessage,
    HealthMessage,
    MetricsMessage,
    ControlMessage,
    PaymentRequestPayload,
    PaymentResponsePayload,
    HealthCheckPayload,
    HealthCheckResponsePayload,
    PerformanceStatsPayload,
    WorkerStatusPayload
};

pub use unix_socket::{
    SocketServer,
    SocketClient,
    SocketServerConfig,
    SocketClientConfig,
    ConnectionInfo,
    MessageHandler,
    ConnectionStats,
    ServerStats
};

use std::sync::Arc;
use std::time::Duration;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use anyhow::Result;
use uuid::Uuid;

/// High-performance IPC manager for coordinating multiple processes
pub struct IpcManager {
    /// Server for receiving messages from workers
    server: Option<Arc<SocketServer>>,
    
    /// Client connections to workers
    worker_clients: Vec<Arc<SocketClient>>,
    
    /// Message dispatcher
    dispatcher: MessageDispatcher,
    
    /// Configuration
    config: IpcConfig,
}

/// IPC configuration
#[derive(Debug, Clone)]
pub struct IpcConfig {
    /// Socket path for main server
    pub server_socket_path: String,
    
    /// Socket paths for worker connections
    pub worker_socket_paths: Vec<String>,
    
    /// Message buffer size
    pub buffer_size: usize,
    
    /// Connection timeout
    pub connect_timeout: Duration,
    
    /// Read timeout
    pub read_timeout: Duration,
    
    /// Write timeout
    pub write_timeout: Duration,
    
    /// Enable compression for large messages
    pub enable_compression: bool,
    
    /// Maximum message size
    pub max_message_size: usize,
    
    /// Number of worker threads for message processing
    pub worker_threads: usize,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            server_socket_path: "/tmp/rinha-backend-server.sock".to_string(),
            worker_socket_paths: vec![
                "/tmp/rinha-backend-worker-1.sock".to_string(),
                "/tmp/rinha-backend-worker-2.sock".to_string(),
            ],
            buffer_size: 64 * 1024, // 64KB
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_millis(100),
            write_timeout: Duration::from_millis(100),
            enable_compression: false, // Disabled for lowest latency
            max_message_size: 1024 * 1024, // 1MB
            worker_threads: num_cpus::get(),
        }
    }
}

/// Message dispatcher for routing IPC messages
struct MessageDispatcher {
    /// Sender for payment messages
    payment_sender: mpsc::UnboundedSender<PaymentMessage>,
    
    /// Sender for health messages
    health_sender: mpsc::UnboundedSender<HealthMessage>,
    
    /// Sender for metrics messages
    metrics_sender: mpsc::UnboundedSender<MetricsMessage>,
    
    /// Sender for control messages
    control_sender: mpsc::UnboundedSender<ControlMessage>,
}

impl IpcManager {
    /// Create new IPC manager
    pub async fn new(config: IpcConfig) -> Result<Self> {
        // Create message channels
        let (payment_sender, payment_receiver) = mpsc::unbounded_channel();
        let (health_sender, health_receiver) = mpsc::unbounded_channel();
        let (metrics_sender, metrics_receiver) = mpsc::unbounded_channel();
        let (control_sender, control_receiver) = mpsc::unbounded_channel();

        let dispatcher = MessageDispatcher {
            payment_sender,
            health_sender,
            metrics_sender,
            control_sender,
        };

        // Start message processors
        Self::start_message_processors(
            payment_receiver,
            health_receiver,
            metrics_receiver,
            control_receiver,
        ).await;

        Ok(Self {
            server: None,
            worker_clients: Vec::new(),
            dispatcher,
            config,
        })
    }

    /// Start the IPC server
    pub async fn start_server(&mut self) -> Result<()> {
        let socket_config = SocketServerConfig {
            socket_path: PathBuf::from(&self.config.socket_path),
            max_connections: self.config.max_connections,
            buffer_size: self.config.buffer_size,
            read_timeout: self.config.read_timeout,
            write_timeout: self.config.write_timeout,
            max_message_size: self.config.max_message_size,
        };

        let server = Arc::new(
            UnixSocketServer::bind(&self.config.server_socket_path, socket_config).await?
        );

        // Start accepting connections
        let server_clone = Arc::clone(&server);
        let dispatcher_clone = self.dispatcher.clone();
        
        tokio::spawn(async move {
            Self::handle_server_connections(server_clone, dispatcher_clone).await;
        });

        self.server = Some(server);
        Ok(())
    }

    /// Connect to worker processes
    pub async fn connect_to_workers(&mut self) -> Result<()> {
        for socket_path in &self.config.worker_socket_paths {
            let socket_config = SocketClientConfig {
                socket_path: socket_path.clone(),
                pool_size: 10,
                connection_timeout: Duration::from_secs(5),
                read_timeout: self.config.read_timeout,
                write_timeout: self.config.write_timeout,
                max_message_size: self.config.max_message_size,
                buffer_size: self.config.buffer_size,
            };

            match SocketClient::new(socket_config).await {
                Ok(client) => {
                    self.worker_clients.push(Arc::new(client));
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to worker at {}: {}", socket_path, e);
                }
            }
        }

        if self.worker_clients.is_empty() {
            return Err(anyhow::anyhow!("Failed to connect to any workers"));
        }

        tracing::info!("Connected to {} workers", self.worker_clients.len());
        Ok(())
    }

    /// Send payment message to worker
    pub async fn send_payment_to_worker(
        &self,
        payment: PaymentMessage,
        worker_index: Option<usize>,
    ) -> Result<()> {
        let worker_idx = worker_index.unwrap_or_else(|| {
            // Simple round-robin load balancing
            use std::sync::atomic::{AtomicUsize, Ordering};
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            COUNTER.fetch_add(1, Ordering::Relaxed) % self.worker_clients.len()
        });

        if let Some(client) = self.worker_clients.get(worker_idx) {
            let message = Message::new(
                MessageType::PaymentRequest,
                MessagePriority::High,
                bincode::serialize(&payment).unwrap(),
            ).unwrap();

            client.send_message(&message).await?;
        } else {
            return Err(anyhow::anyhow!("Worker index {} not found", worker_idx));
        }

        Ok(())
    }

    /// Broadcast health check to all workers
    pub async fn broadcast_health_check(&self) -> Result<Vec<HealthMessage>> {
        let mut responses = Vec::new();
        let (sender, mut receiver) = mpsc::channel(self.worker_clients.len());

        // Send health check to all workers
        for (idx, client) in self.worker_clients.iter().enumerate() {
            let client_clone = Arc::clone(client);
            let sender_clone = sender.clone();
            
            tokio::spawn(async move {
                let health_check = HealthMessage {
                    processor_type: crate::storage::ProcessorType::Default, // Will be overridden
                    is_healthy: true, // Request
                    latency_ms: 0,
                    error_message: None,
                };

                let message = IpcMessage {
                    header: MessageHeader {
                        message_type: MessageType::Health,
                        timestamp: chrono::Utc::now().timestamp_micros() as u64,
                        sequence_id: generate_sequence_id(),
                        flags: 0,
                    },
                    payload: MessagePayload::Health(health_check),
                };

                match client_clone.send_message(&message).await {
                    Ok(_) => {
                        // Wait for response
                        if let Ok(response) = client_clone.receive_message().await {
                            if let MessagePayload::Health(health_response) = response.payload {
                                let _ = sender_clone.send((idx, health_response)).await;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to send health check to worker {}: {}", idx, e);
                    }
                }
            });
        }

        drop(sender); // Close sender to end receiving

        // Collect responses with timeout
        let timeout = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some((_idx, health_response)) = receiver.recv().await {
                responses.push(health_response);
            }
        });

        let _ = timeout.await; // Ignore timeout errors
        Ok(responses)
    }

    /// Get IPC performance metrics
    pub async fn get_metrics(&self) -> IpcMetrics {
        let mut total_messages_sent = 0;
        let mut total_messages_received = 0;
        let mut total_bytes_sent = 0;
        let mut total_bytes_received = 0;

        // Aggregate metrics from all clients
        for client in &self.worker_clients {
            if let Ok(metrics) = client.get_metrics().await {
                total_messages_sent += metrics.messages_sent;
                total_messages_received += metrics.messages_received;
                total_bytes_sent += metrics.bytes_sent;
                total_bytes_received += metrics.bytes_received;
            }
        }

        IpcMetrics {
            total_messages_sent,
            total_messages_received,
            total_bytes_sent,
            total_bytes_received,
            active_connections: self.worker_clients.len(),
            server_running: self.server.is_some(),
        }
    }

    /// Shutdown IPC manager
    pub async fn shutdown(&mut self) -> Result<()> {
        // Shutdown all worker clients
        for client in &self.worker_clients {
            let _ = client.disconnect().await;
        }
        self.worker_clients.clear();

        // Shutdown server
        if let Some(server) = &self.server {
            server.shutdown().await?;
        }
        self.server = None;

        // Cleanup socket files
        let _ = std::fs::remove_file(&self.config.server_socket_path);
        for socket_path in &self.config.worker_socket_paths {
            let _ = std::fs::remove_file(socket_path);
        }

        Ok(())
    }

    /// Handle incoming server connections
    async fn handle_server_connections(
        server: Arc<UnixSocketServer>,
        dispatcher: MessageDispatcher,
    ) {
        loop {
            match server.accept().await {
                Ok(connection) => {
                    let dispatcher_clone = dispatcher.clone();
                    tokio::spawn(async move {
                        Self::handle_client_connection(connection, dispatcher_clone).await;
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to accept connection: {}", e);
                    break;
                }
            }
        }
    }

    /// Handle individual client connection
    async fn handle_client_connection(
        mut connection: ConnectionHandle,
        dispatcher: MessageDispatcher,
    ) {
        loop {
            match connection.receive_message().await {
                Ok(message) => {
                    if let Err(e) = Self::dispatch_message(message, &dispatcher).await {
                        tracing::error!("Failed to dispatch message: {}", e);
                    }
                }
                Err(e) => {
                    tracing::debug!("Connection closed: {}", e);
                    break;
                }
            }
        }
    }

    /// Dispatch message to appropriate handler
    async fn dispatch_message(
        message: IpcMessage,
        dispatcher: &MessageDispatcher,
    ) -> Result<()> {
        match message.payload {
            MessagePayload::Payment(payment) => {
                dispatcher.payment_sender.send(payment)?;
            }
            MessagePayload::Health(health) => {
                dispatcher.health_sender.send(health)?;
            }
            MessagePayload::Metrics(metrics) => {
                dispatcher.metrics_sender.send(metrics)?;
            }
            MessagePayload::Control(control) => {
                dispatcher.control_sender.send(control)?;
            }
        }
        Ok(())
    }

    /// Start message processor tasks
    async fn start_message_processors(
        mut payment_receiver: mpsc::UnboundedReceiver<PaymentMessage>,
        mut health_receiver: mpsc::UnboundedReceiver<HealthMessage>,
        mut metrics_receiver: mpsc::UnboundedReceiver<MetricsMessage>,
        mut control_receiver: mpsc::UnboundedReceiver<ControlMessage>,
    ) {
        // Payment message processor
        tokio::spawn(async move {
            while let Some(payment) = payment_receiver.recv().await {
                // Process payment message
                tracing::debug!("Processing payment message: {:?}", payment.correlation_id);
                // TODO: Integrate with payment processing logic
            }
        });

        // Health message processor
        tokio::spawn(async move {
            while let Some(health) = health_receiver.recv().await {
                tracing::debug!("Processing health message for {:?}", health.processor_type);
                // TODO: Integrate with health monitoring
            }
        });

        // Metrics message processor
        tokio::spawn(async move {
            while let Some(metrics) = metrics_receiver.recv().await {
                tracing::debug!("Processing metrics message");
                // TODO: Integrate with metrics collection
            }
        });

        // Control message processor
        tokio::spawn(async move {
            while let Some(control) = control_receiver.recv().await {
                tracing::debug!("Processing control message: {:?}", control.command);
                // TODO: Integrate with control logic
            }
        });
    }
}

impl MessageDispatcher {
    fn clone(&self) -> Self {
        Self {
            payment_sender: self.payment_sender.clone(),
            health_sender: self.health_sender.clone(),
            metrics_sender: self.metrics_sender.clone(),
            control_sender: self.control_sender.clone(),
        }
    }
}

/// IPC performance metrics
#[derive(Debug, Clone)]
pub struct IpcMetrics {
    pub total_messages_sent: u64,
    pub total_messages_received: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_received: u64,
    pub active_connections: usize,
    pub server_running: bool,
}

/// Generate unique sequence ID
fn generate_sequence_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(1);
    SEQUENCE_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// IPC factory for creating managers with different configurations
pub struct IpcFactory;

impl IpcFactory {
    /// Create IPC manager for API server
    pub async fn create_api_server() -> Result<IpcManager> {
        let config = IpcConfig {
            server_socket_path: "/tmp/rinha-api-server.sock".to_string(),
            worker_socket_paths: vec![
                "/tmp/rinha-worker-1.sock".to_string(),
                "/tmp/rinha-worker-2.sock".to_string(),
            ],
            ..Default::default()
        };

        IpcManager::new(config).await
    }

    /// Create IPC manager for load balancer
    pub async fn create_load_balancer() -> Result<IpcManager> {
        let config = IpcConfig {
            server_socket_path: "/tmp/rinha-lb-server.sock".to_string(),
            worker_socket_paths: vec![
                "/tmp/rinha-api-1.sock".to_string(),
                "/tmp/rinha-api-2.sock".to_string(),
            ],
            ..Default::default()
        };

        IpcManager::new(config).await
    }

    /// Create IPC manager for worker
    pub async fn create_worker(worker_id: u32) -> Result<IpcManager> {
        let config = IpcConfig {
            server_socket_path: format!("/tmp/rinha-worker-{}.sock", worker_id),
            worker_socket_paths: vec![], // Workers don't connect to other workers
            worker_threads: 2, // Fewer threads for workers
            ..Default::default()
        };

        IpcManager::new(config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ipc_manager_creation() {
        let config = IpcConfig::default();
        let manager = IpcManager::new(config).await;
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_ipc_factory() {
        let api_manager = IpcFactory::create_api_server().await;
        assert!(api_manager.is_ok());

        let lb_manager = IpcFactory::create_load_balancer().await;
        assert!(lb_manager.is_ok());

        let worker_manager = IpcFactory::create_worker(1).await;
        assert!(worker_manager.is_ok());
    }

    #[test]
    fn test_sequence_id_generation() {
        let id1 = generate_sequence_id();
        let id2 = generate_sequence_id();
        assert!(id2 > id1);
    }

    #[test]
    fn test_ipc_config_defaults() {
        let config = IpcConfig::default();
        assert_eq!(config.buffer_size, 64 * 1024);
        assert_eq!(config.worker_threads, num_cpus::get());
        assert!(!config.enable_compression); // Disabled for performance
    }
}