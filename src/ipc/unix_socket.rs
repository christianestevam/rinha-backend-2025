//! Ultra-low latency Unix domain socket implementation for the Rinha Backend 2025
//! 
//! This module provides high-performance IPC communication using Unix domain sockets
//! with zero-copy optimizations and connection pooling.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, Duration, Instant};
use anyhow::{Result, anyhow, Context};
use parking_lot::{Mutex, RwLock};
use serde::{Serialize, Deserialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::timeout;
use tracing::{info, warn, error, debug, trace};

use crate::config::AppConfig;
use crate::ipc::message::{Message, MessageType, MAX_MESSAGE_SIZE, MESSAGE_HEADER_SIZE};

// Para desenvolvimento no Windows, usar TCP ao invés de Unix sockets
#[cfg(unix)]
use tokio::net::unix::{UnixStream, UnixListener, WriteHalf, ReadHalf};
#[cfg(not(unix))]
use tokio::net::{TcpStream as UnixStream, TcpListener as UnixListener};
#[cfg(not(unix))]
use tokio::io::{WriteHalf, ReadHalf};

/// Default socket path prefix
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/rinha-backend";

/// Maximum number of concurrent connections
pub const MAX_CONNECTIONS: usize = 1000;

/// Connection timeout
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Read timeout
pub const READ_TIMEOUT: Duration = Duration::from_millis(100);

/// Write timeout
pub const WRITE_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum pending messages per connection
pub const MAX_PENDING_MESSAGES: usize = 1000;

/// Socket server configuration
#[derive(Debug, Clone)]
pub struct SocketServerConfig {
    /// Socket path
    pub socket_path: PathBuf,
    
    /// Maximum concurrent connections
    pub max_connections: usize,
    
    /// Connection timeout
    pub connection_timeout: Duration,
    
    /// Read timeout
    pub read_timeout: Duration,
    
    /// Write timeout
    pub write_timeout: Duration,
    
    /// Maximum pending messages per connection
    pub max_pending_messages: usize,
    
    /// Enable message compression
    pub enable_compression: bool,
    
    /// Buffer size for socket operations
    pub buffer_size: usize,
}

impl Default for SocketServerConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from(format!("{}-server.sock", DEFAULT_SOCKET_PATH)),
            max_connections: MAX_CONNECTIONS,
            connection_timeout: CONNECTION_TIMEOUT,
            read_timeout: READ_TIMEOUT,
            write_timeout: WRITE_TIMEOUT,
            max_pending_messages: MAX_PENDING_MESSAGES,
            enable_compression: false, // Disabled for performance
            buffer_size: 64 * 1024, // 64KB
        }
    }
}

/// Socket client configuration
#[derive(Debug, Clone)]
pub struct SocketClientConfig {
    /// Socket path to connect to
    pub socket_path: PathBuf,
    
    /// Connection timeout
    pub connection_timeout: Duration,
    
    /// Read timeout
    pub read_timeout: Duration,
    
    /// Write timeout
    pub write_timeout: Duration,
    
    /// Maximum retries on connection failure
    pub max_retries: u32,
    
    /// Retry delay
    pub retry_delay: Duration,
    
    /// Enable connection pooling
    pub enable_pooling: bool,
    
    /// Pool size
    pub pool_size: usize,
    
    /// Buffer size for socket operations
    pub buffer_size: usize,
}

impl Default for SocketClientConfig {
    fn default() -> Self {
        Self {
            socket_path: PathBuf::from(format!("{}-server.sock", DEFAULT_SOCKET_PATH)),
            connection_timeout: Duration::from_secs(5),
            read_timeout: READ_TIMEOUT,
            write_timeout: WRITE_TIMEOUT,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            enable_pooling: true,
            pool_size: 10,
            buffer_size: 64 * 1024,
        }
    }
}

/// Connection statistics
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// Connection created timestamp
    pub created_at: SystemTime,
    
    /// Last activity timestamp
    pub last_activity: SystemTime,
    
    /// Total messages sent
    pub messages_sent: u64,
    
    /// Total messages received
    pub messages_received: u64,
    
    /// Total bytes sent
    pub bytes_sent: u64,
    
    /// Total bytes received
    pub bytes_received: u64,
    
    /// Connection errors
    pub errors: u64,
    
    /// Average message latency
    pub avg_latency_us: u64,
}

/// Connection information
#[derive(Debug)]
pub struct ConnectionInfo {
    /// Connection ID
    pub id: String,
    
    /// Connection statistics
    pub stats: ConnectionStats,
    
    /// Connection state
    pub state: ConnectionState,
    
    /// Message sender
    pub sender: mpsc::UnboundedSender<Message>,
}

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is connecting
    Connecting,
    
    /// Connection is active
    Active,
    
    /// Connection is idle
    Idle,
    
    /// Connection is closing
    Closing,
    
    /// Connection is closed
    Closed,
    
    /// Connection has errors
    Error,
}

/// Message handler trait
#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    /// Handle incoming message
    async fn handle_message(&self, message: Message, connection_id: &str) -> Result<Option<Message>>;
    
    /// Handle connection established
    async fn on_connection_established(&self, connection_id: &str) {
        info!("Connection established: {}", connection_id);
    }
    
    /// Handle connection closed
    async fn on_connection_closed(&self, connection_id: &str) {
        info!("Connection closed: {}", connection_id);
    }
    
    /// Handle connection error
    async fn on_connection_error(&self, connection_id: &str, error: &anyhow::Error) {
        error!("Connection error for {}: {}", connection_id, error);
    }
}

/// Unix domain socket server
pub struct SocketServer {
    /// Server configuration
    config: SocketServerConfig,
    
    /// Active connections
    connections: Arc<RwLock<HashMap<String, ConnectionInfo>>>,
    
    /// Connection semaphore for limiting concurrent connections
    connection_semaphore: Arc<Semaphore>,
    
    /// Message handler
    handler: Arc<dyn MessageHandler>,
    
    /// Server statistics
    stats: Arc<Mutex<ServerStats>>,
    
    /// Shutdown signal
    shutdown_tx: Option<mpsc::UnboundedSender<()>>,
}

/// Server statistics
#[derive(Debug)]
pub struct ServerStats {
    /// Server start time
    pub start_time: SystemTime,
    
    /// Total connections accepted
    pub total_connections: u64,
    
    /// Current active connections
    pub active_connections: u64,
    
    /// Total messages processed
    pub total_messages: u64,
    
    /// Total bytes processed
    pub total_bytes: u64,
    
    /// Total errors
    pub total_errors: u64,
    
    /// Average message processing time
    pub avg_processing_time_us: u64,
}

impl SocketServer {
    /// Create a new socket server
    pub fn new(config: SocketServerConfig, handler: Arc<dyn MessageHandler>) -> Self {
        let connection_semaphore = Arc::new(Semaphore::new(config.max_connections));
        
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            connection_semaphore,
            handler,
            stats: Arc::new(Mutex::new(ServerStats {
                start_time: SystemTime::now(),
                total_connections: 0,
                active_connections: 0,
                total_messages: 0,
                total_bytes: 0,
                total_errors: 0,
                avg_processing_time_us: 0,
            })),
            shutdown_tx: None,
        }
    }
    
    /// Start the socket server
    pub async fn start(&mut self) -> Result<()> {
        // Remove existing socket file
        if self.config.socket_path.exists() {
            std::fs::remove_file(&self.config.socket_path)
                .context("Failed to remove existing socket file")?;
        }
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = self.config.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await
                .context("Failed to create socket directory")?;
        }
        
        // Bind to socket
        let listener = UnixListener::bind(&self.config.socket_path)
            .context("Failed to bind to Unix socket")?;
        
        info!("Socket server listening on: {:?}", self.config.socket_path);
        
        // Setup shutdown channel
        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
        self.shutdown_tx = Some(shutdown_tx);
        
        // Accept connections
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            self.handle_connection(stream).await;
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping server");
                    break;
                }
            }
        }
        
        // Cleanup
        self.cleanup().await;
        
        Ok(())
    }
    
    /// Handle a new connection
    async fn handle_connection(&self, stream: UnixStream) {
        // Acquire connection permit
        let permit = match self.connection_semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!("Maximum connections reached, rejecting connection");
                return;
            }
        };
        
        let connection_id = uuid::Uuid::new_v4().to_string();
        let config = self.config.clone();
        let connections = self.connections.clone();
        let handler = self.handler.clone();
        let stats = self.stats.clone();
        
        // Update stats
        {
            let mut server_stats = stats.lock().await;
            server_stats.total_connections += 1;
            server_stats.active_connections += 1;
        }
        
        // Spawn connection handler
        tokio::spawn(async move {
            let _permit = permit; // Keep permit alive
            let result = Self::handle_connection_inner(
                connection_id.clone(),
                stream,
                config,
                connections.clone(),
                handler,
                stats.clone(),
            ).await;
            
            if let Err(e) = result {
                error!("Connection {} error: {}", connection_id, e);
            }
            
            // Update stats
            {
                let mut server_stats = stats.lock().await;
                server_stats.active_connections = server_stats.active_connections.saturating_sub(1);
            }
            
            // Remove connection
            connections.write().await.remove(&connection_id);
        });
    }
    
    /// Handle connection inner logic
    async fn handle_connection_inner(
        connection_id: String,
        mut stream: UnixStream,
        config: SocketServerConfig,
        connections: Arc<RwLock<HashMap<String, ConnectionInfo>>>,
        handler: Arc<dyn MessageHandler>,
        stats: Arc<Mutex<ServerStats>>,
    ) -> Result<()> {
        // Create message channel
        let (tx, mut rx) = mpsc::unbounded_channel();
        
        // Create connection info
        let connection_info = ConnectionInfo {
            id: connection_id.clone(),
            stats: ConnectionStats {
                created_at: SystemTime::now(),
                last_activity: SystemTime::now(),
                ..Default::default()
            },
            state: ConnectionState::Active,
            sender: tx,
        };
        
        // Add to connections
        connections.write().await.insert(connection_id.clone(), connection_info);
        
        // Notify handler
        handler.on_connection_established(&connection_id).await;
        
        // Split stream
        let (mut reader, mut writer) = stream.split();
        
        // Spawn message sender task
        let sender_task = {
            let connection_id = connection_id.clone();
            let connections = connections.clone();
            let config = config.clone();
            
            tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    let start_time = Instant::now();
                    
                    match timeout(config.write_timeout, Self::send_message(&mut writer, &message)).await {
                        Ok(Ok(_)) => {
                            // Update connection stats
                            if let Some(mut conn) = connections.write().await.get_mut(&connection_id) {
                                conn.stats.messages_sent += 1;
                                conn.stats.bytes_sent += message.size() as u64;
                                conn.stats.last_activity = SystemTime::now();
                                
                                let latency = start_time.elapsed().as_micros() as u64;
                                conn.stats.avg_latency_us = (conn.stats.avg_latency_us + latency) / 2;
                            }
                        }
                        Ok(Err(e)) => {
                            error!("Failed to send message to {}: {}", connection_id, e);
                            break;
                        }
                        Err(_) => {
                            error!("Send timeout for connection {}", connection_id);
                            break;
                        }
                    }
                }
            })
        };
        
        // Main message receiving loop
        let mut buffer = vec![0u8; config.buffer_size];
        let receiver_task = async {
            loop {
                match timeout(config.read_timeout, Self::receive_message(&mut reader, &mut buffer)).await {
                    Ok(Ok(message)) => {
                        let start_time = Instant::now();
                        
                        // Update connection stats
                        if let Some(mut conn) = connections.write().await.get_mut(&connection_id) {
                            conn.stats.messages_received += 1;
                            conn.stats.bytes_received += message.size() as u64;
                            conn.stats.last_activity = SystemTime::now();
                        }
                        
                        // Handle message
                        match handler.handle_message(message, &connection_id).await {
                            Ok(Some(response)) => {
                                // Send response
                                if let Some(conn) = connections.read().await.get(&connection_id) {
                                    if let Err(e) = conn.sender.send(response) {
                                        error!("Failed to queue response for {}: {}", connection_id, e);
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {
                                // No response needed
                            }
                            Err(e) => {
                                error!("Message handling error for {}: {}", connection_id, e);
                                handler.on_connection_error(&connection_id, &e).await;
                                
                                // Update error stats
                                if let Some(mut conn) = connections.write().await.get_mut(&connection_id) {
                                    conn.stats.errors += 1;
                                }
                            }
                        }
                        
                        // Update server stats
                        {
                            let mut server_stats = stats.lock().await;
                            server_stats.total_messages += 1;
                            
                            let processing_time = start_time.elapsed().as_micros() as u64;
                            server_stats.avg_processing_time_us = 
                                (server_stats.avg_processing_time_us + processing_time) / 2;
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Receive error for {}: {}", connection_id, e);
                        break;
                    }
                    Err(_) => {
                        // Timeout is normal, continue
                        continue;
                    }
                }
            }
        };
        
        // Wait for either task to complete
        tokio::select! {
            _ = sender_task => {
                debug!("Sender task completed for {}", connection_id);
            }
            _ = receiver_task => {
                debug!("Receiver task completed for {}", connection_id);
            }
        }
        
        // Notify handler
        handler.on_connection_closed(&connection_id).await;
        
        Ok(())
    }
    
    /// Send a message over the stream
    async fn send_message(
        writer: &mut WriteHalf<UnixStream>,
        message: &Message,
    ) -> Result<()> {
        let bytes = message.to_bytes();
        writer.write_all(&bytes).await
            .context("Failed to write message")?;
        writer.flush().await
            .context("Failed to flush writer")?;
        
        trace!("Sent message: {} bytes", bytes.len());
        Ok(())
    }
    
    /// Receive a message from the stream
    async fn receive_message(
        reader: &mut ReadHalf<UnixStream>,
        buffer: &mut [u8],
    ) -> Result<Message> {
        // Read message header first
        reader.read_exact(&mut buffer[..MESSAGE_HEADER_SIZE]).await
            .context("Failed to read message header")?;
        
        let header_bytes: [u8; MESSAGE_HEADER_SIZE] = buffer[..MESSAGE_HEADER_SIZE]
            .try_into()
            .map_err(|_| anyhow!("Invalid header size"))?;
        
        let header = crate::ipc::message::MessageHeader::from_bytes(&header_bytes);
        
        if !header.is_valid() {
            return Err(anyhow!("Invalid message header"));
        }
        
        let payload_size = header.payload_size as usize;
        if payload_size > buffer.len() - MESSAGE_HEADER_SIZE {
            return Err(anyhow!("Message too large: {} bytes", payload_size));
        }
        
        // Read payload
        if payload_size > 0 {
            reader.read_exact(&mut buffer[MESSAGE_HEADER_SIZE..MESSAGE_HEADER_SIZE + payload_size]).await
                .context("Failed to read message payload")?;
        }
        
        // Construct complete message bytes
        let total_size = MESSAGE_HEADER_SIZE + payload_size;
        let message_bytes = &buffer[..total_size];
        
        let message = Message::from_bytes(message_bytes)
            .context("Failed to deserialize message")?;
        
        trace!("Received message: {} bytes", total_size);
        Ok(message)
    }
    
    /// Send a message to a specific connection
    pub async fn send_to_connection(&self, connection_id: &str, message: Message) -> Result<()> {
        let connections = self.connections.read().await;
        if let Some(conn) = connections.get(connection_id) {
            conn.sender.send(message)
                .map_err(|e| anyhow!("Failed to send message to {}: {}", connection_id, e))?;
            Ok(())
        } else {
            Err(anyhow!("Connection not found: {}", connection_id))
        }
    }
    
    /// Broadcast a message to all connections
    pub async fn broadcast(&self, message: Message) -> Result<()> {
        let connections = self.connections.read().await;
        let mut errors = Vec::new();
        
        for (id, conn) in connections.iter() {
            if let Err(e) = conn.sender.send(message.clone()) {
                errors.push(format!("Failed to send to {}: {}", id, e));
            }
        }
        
        if !errors.is_empty() {
            return Err(anyhow!("Broadcast errors: {}", errors.join(", ")));
        }
        
        Ok(())
    }
    
    /// Get server statistics
    pub async fn get_stats(&self) -> ServerStats {
        self.stats.lock().await.clone()
    }
    
    /// Get connection statistics
    pub async fn get_connection_stats(&self) -> HashMap<String, ConnectionStats> {
        self.connections.read().await
            .iter()
            .map(|(id, conn)| (id.clone(), conn.stats.clone()))
            .collect()
    }
    
    /// Shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        if let Some(tx) = &self.shutdown_tx {
            tx.send(()).map_err(|_| anyhow!("Failed to send shutdown signal"))?;
        }
        Ok(())
    }
    
    /// Cleanup resources
    async fn cleanup(&self) {
        // Close all connections
        let connections = self.connections.read().await;
        for (id, _) in connections.iter() {
            debug!("Closing connection: {}", id);
        }
        
        // Remove socket file
        if self.config.socket_path.exists() {
            if let Err(e) = std::fs::remove_file(&self.config.socket_path) {
                error!("Failed to remove socket file: {}", e);
            }
        }
    }
}

/// Unix domain socket client with connection pooling
pub struct SocketClient {
    /// Client configuration
    config: SocketClientConfig,
    
    /// Connection pool
    pool: Arc<Mutex<VecDeque<UnixStream>>>,
    
    /// Pool semaphore
    pool_semaphore: Arc<Semaphore>,
    
    /// Client statistics
    stats: Arc<Mutex<ClientStats>>,
}

/// Client statistics
#[derive(Debug, Default)]
pub struct ClientStats {
    /// Total connections created
    pub total_connections: u64,
    
    /// Total messages sent
    pub total_messages_sent: u64,
    
    /// Total messages received
    pub total_messages_received: u64,
    
    /// Total bytes sent
    pub total_bytes_sent: u64,
    
    /// Total bytes received
    pub total_bytes_received: u64,
    
    /// Total errors
    pub total_errors: u64,
    
    /// Average round-trip time
    pub avg_rtt_us: u64,
    
    /// Connection pool hits
    pub pool_hits: u64,
    
    /// Connection pool misses
    pub pool_misses: u64,
}

impl SocketClient {
    /// Create a new socket client
    pub fn new(config: SocketClientConfig) -> Self {
        let pool_semaphore = if config.enable_pooling {
            Arc::new(Semaphore::new(config.pool_size))
        } else {
            Arc::new(Semaphore::new(1))
        };
        
        Self {
            config,
            pool: Arc::new(Mutex::new(VecDeque::new())),
            pool_semaphore,
            stats: Arc::new(Mutex::new(ClientStats::default())),
        }
    }
    
    /// Send a message and wait for response
    pub async fn send_message(&self, message: Message) -> Result<Option<Message>> {
        let start_time = Instant::now();
        let mut stream = self.get_connection().await?;
        
        // Send message
        timeout(self.config.write_timeout, Self::send_message_to_stream(&mut stream, &message)).await
            .context("Send timeout")?
            .context("Failed to send message")?;
        
        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.total_messages_sent += 1;
            stats.total_bytes_sent += message.size() as u64;
        }
        
        // Receive response (if expected)
        let response = if message.header.message_type() != MessageType::Shutdown {
            let mut buffer = vec![0u8; self.config.buffer_size];
            match timeout(self.config.read_timeout, Self::receive_message_from_stream(&mut stream, &mut buffer)).await {
                Ok(Ok(response)) => {
                    // Update stats
                    {
                        let mut stats = self.stats.lock().await;
                        stats.total_messages_received += 1;
                        stats.total_bytes_received += response.size() as u64;
                        
                        let rtt = start_time.elapsed().as_micros() as u64;
                        stats.avg_rtt_us = (stats.avg_rtt_us + rtt) / 2;
                    }
                    
                    Some(response)
                }
                Ok(Err(e)) => {
                    error!("Failed to receive response: {}", e);
                    self.stats.lock().await.total_errors += 1;
                    return Err(e);
                }
                Err(_) => {
                    warn!("Receive timeout");
                    None
                }
            }
        } else {
            None
        };
        
        // Return connection to pool
        self.return_connection(stream).await;
        
        Ok(response)
    }
    
    /// Get a connection from the pool or create a new one
    async fn get_connection(&self) -> Result<UnixStream> {
        // Try to get from pool first
        if self.config.enable_pooling {
            let mut pool = self.pool.lock().await;
            if let Some(stream) = pool.pop_front() {
                self.stats.lock().await.pool_hits += 1;
                return Ok(stream);
            }
        }
        
        self.stats.lock().await.pool_misses += 1;
        
        // Create new connection
        self.create_connection().await
    }
    
    /// Create a new connection with retries
    async fn create_connection(&self) -> Result<UnixStream> {
        let mut last_error = None;
        
        for attempt in 0..=self.config.max_retries {
            match timeout(
                self.config.connection_timeout,
                UnixStream::connect(&self.config.socket_path)
            ).await {
                Ok(Ok(stream)) => {
                    self.stats.lock().await.total_connections += 1;
                    debug!("Connected to {:?}", self.config.socket_path);
                    return Ok(stream);
                }
                Ok(Err(e)) => {
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        warn!("Connection attempt {} failed, retrying in {:?}: {}", 
                              attempt + 1, self.config.retry_delay, last_error.as_ref().unwrap());
                        tokio::time::sleep(self.config.retry_delay).await;
                    }
                }
                Err(_) => {
                    last_error = Some(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Connection timeout"
                    ));
                    if attempt < self.config.max_retries {
                        warn!("Connection timeout on attempt {}, retrying", attempt + 1);
                        tokio::time::sleep(self.config.retry_delay).await;
                    }
                }
            }
        }
        
        self.stats.lock().await.total_errors += 1;
        Err(anyhow!("Failed to connect after {} attempts: {}", 
                    self.config.max_retries + 1, 
                    last_error.unwrap()))
    }
    
    /// Return a connection to the pool
    async fn return_connection(&self, stream: UnixStream) {
        if self.config.enable_pooling {
            let mut pool = self.pool.lock().await;
            if pool.len() < self.config.pool_size {
                pool.push_back(stream);
            }
            // If pool is full, just drop the connection
        }
        // If pooling is disabled, just drop the connection
    }
    
    /// Send message to stream
    async fn send_message_to_stream(stream: &mut UnixStream, message: &Message) -> Result<()> {
        let bytes = message.to_bytes();
        stream.write_all(&bytes).await
            .context("Failed to write message")?;
        stream.flush().await
            .context("Failed to flush stream")?;
        
        trace!("Sent message: {} bytes", bytes.len());
        Ok(())
    }
    
    /// Receive message from stream
    async fn receive_message_from_stream(
        stream: &mut UnixStream,
        buffer: &mut [u8],
    ) -> Result<Message> {
        // Read message header first
        stream.read_exact(&mut buffer[..MESSAGE_HEADER_SIZE]).await
            .context("Failed to read message header")?;
        
        let header_bytes: [u8; MESSAGE_HEADER_SIZE] = buffer[..MESSAGE_HEADER_SIZE]
            .try_into()
            .map_err(|_| anyhow!("Invalid header size"))?;
        
        let header = crate::ipc::message::MessageHeader::from_bytes(&header_bytes);
        
        if !header.is_valid() {
            return Err(anyhow!("Invalid message header"));
        }
        
        let payload_size = header.payload_size as usize;
        if payload_size > buffer.len() - MESSAGE_HEADER_SIZE {
            return Err(anyhow!("Message too large: {} bytes", payload_size));
        }
        
        // Read payload
        if payload_size > 0 {
            stream.read_exact(&mut buffer[MESSAGE_HEADER_SIZE..MESSAGE_HEADER_SIZE + payload_size]).await
                .context("Failed to read message payload")?;
        }
        
        // Construct complete message bytes
        let total_size = MESSAGE_HEADER_SIZE + payload_size;
        let message_bytes = &buffer[..total_size];
        
        let message = Message::from_bytes(message_bytes)
            .context("Failed to deserialize message")?;
        
        trace!("Received message: {} bytes", total_size);
        Ok(message)
    }
    
    /// Get client statistics
    pub async fn get_stats(&self) -> ClientStats {
        self.stats.lock().await.clone()
    }
    
    /// Clear connection pool
    pub async fn clear_pool(&self) {
        self.pool.lock().await.clear();
    }
}

/// Helper functions for socket path management
pub mod socket_utils {
    use super::*;
    
    /// Generate socket path for a service
    pub fn service_socket_path(service_name: &str) -> PathBuf {
        PathBuf::from(format!("{}-{}.sock", DEFAULT_SOCKET_PATH, service_name))
    }
    
    /// Generate socket path with custom directory
    pub fn custom_socket_path(dir: &Path, service_name: &str) -> PathBuf {
        dir.join(format!("{}.sock", service_name))
    }
    
    /// Check if socket path exists and is accessible
    pub fn check_socket_path(path: &Path) -> Result<()> {
        if !path.exists() {
            return Err(anyhow!("Socket path does not exist: {:?}", path));
        }
        
        // Try to connect to verify it's a valid socket
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| anyhow!("No tokio runtime found"))?;
        
        rt.block_on(async {
            match tokio::time::timeout(
                Duration::from_millis(100),
                UnixStream::connect(path)
            ).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(anyhow!("Cannot connect to socket: {}", e)),
                Err(_) => Err(anyhow!("Socket connection timeout")),
            }
        })
    }
    
    /// Clean up old socket files
    pub fn cleanup_old_sockets(dir: &Path, max_age: Duration) -> Result<()> {
        let entries = std::fs::read_dir(dir)
            .with_context(|| format!("Failed to read directory: {:?}", dir))?;
        
        for entry in entries {
            let entry = entry.context("Failed to read directory entry")?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("sock") {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = modified.elapsed() {
                            if age > max_age {
                                if let Err(e) = std::fs::remove_file(&path) {
                                    warn!("Failed to remove old socket file {:?}: {}", path, e);
                                } else {
                                    info!("Removed old socket file: {:?}", path);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::message::{MessageBuilder, HealthCheckPayload};
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestMessageHandler;

    #[async_trait::async_trait]
    impl MessageHandler for TestMessageHandler {
        async fn handle_message(&self, message: Message, _connection_id: &str) -> Result<Option<Message>> {
            // Echo the message back
            Ok(Some(message))
        }
    }

    #[tokio::test]
    async fn test_socket_server_client() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        // Start server
        let server_config = SocketServerConfig {
            socket_path: socket_path.clone(),
            max_connections: 10,
            ..Default::default()
        };

        let handler = Arc::new(TestMessageHandler);
        let mut server = SocketServer::new(server_config, handler);

        // Start server in background
        let server_handle = {
            tokio::spawn(async move {
                server.start().await.unwrap();
            })
        };

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client
        let client_config = SocketClientConfig {
            socket_path: socket_path.clone(),
            enable_pooling: false,
            ..Default::default()
        };

        let client = SocketClient::new(client_config);

        // Test message
        let payload = HealthCheckPayload {
            service_name: "test".to_string(),
            version: "1.0.0".to_string(),
            timestamp: SystemTime::now(),
            metadata: HashMap::new(),
        };

        let message = MessageBuilder::new(MessageType::HealthCheck)
            .health_check(payload.clone())
            .unwrap();

        // Send message
        let response = client.send_message(message).await.unwrap();
        assert!(response.is_some());

        let response = response.unwrap();
        assert_eq!(response.header.message_type(), MessageType::HealthCheck);

        let response_payload: HealthCheckPayload = response.to_data().unwrap();
        assert_eq!(response_payload.service_name, payload.service_name);

        // Get stats
        let stats = client.get_stats().await;
        assert_eq!(stats.total_messages_sent, 1);
        assert_eq!(stats.total_messages_received, 1);

        // Cleanup
        server_handle.abort();
    }

    #[test]
    fn test_socket_utils() {
        let path = socket_utils::service_socket_path("test");
        assert!(path.to_string_lossy().contains("test"));

        let temp_dir = TempDir::new().unwrap();
        let custom_path = socket_utils::custom_socket_path(temp_dir.path(), "custom");
        assert!(custom_path.to_string_lossy().contains("custom"));
    }
}