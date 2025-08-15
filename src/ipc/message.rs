//! High-performance IPC message system for the Rinha Backend 2025
//! 
//! This module provides zero-copy message serialization and deserialization
//! optimized for ultra-low latency inter-process communication.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow};
use uuid::Uuid;

/// Maximum message size (16KB for performance)
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024;

/// Message header size in bytes
pub const MESSAGE_HEADER_SIZE: usize = 32;

/// Message magic number for validation
pub const MESSAGE_MAGIC: u32 = 0x52494E48; // "RINH" in hex

/// IPC message types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    /// Payment processing request
    PaymentRequest = 1,
    
    /// Payment processing response
    PaymentResponse = 2,
    
    /// Health check request
    HealthCheck = 3,
    
    /// Health check response
    HealthCheckResponse = 4,
    
    /// Metrics report
    MetricsReport = 5,
    
    /// Load balancer notification
    LoadBalancerUpdate = 6,
    
    /// Worker status update
    WorkerStatus = 7,
    
    /// Shutdown signal
    Shutdown = 8,
    
    /// Configuration update
    ConfigUpdate = 9,
    
    /// Performance stats
    PerformanceStats = 10,
    
    /// Error notification
    Error = 255,
}

impl From<u8> for MessageType {
    fn from(value: u8) -> Self {
        match value {
            1 => MessageType::PaymentRequest,
            2 => MessageType::PaymentResponse,
            3 => MessageType::HealthCheck,
            4 => MessageType::HealthCheckResponse,
            5 => MessageType::MetricsReport,
            6 => MessageType::LoadBalancerUpdate,
            7 => MessageType::WorkerStatus,
            8 => MessageType::Shutdown,
            9 => MessageType::ConfigUpdate,
            10 => MessageType::PerformanceStats,
            255 => MessageType::Error,
            _ => MessageType::Error,
        }
    }
}

/// Message priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessagePriority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl From<u8> for MessagePriority {
    fn from(value: u8) -> Self {
        match value {
            1 => MessagePriority::Low,
            2 => MessagePriority::Normal,
            3 => MessagePriority::High,
            4 => MessagePriority::Critical,
            _ => MessagePriority::Normal,
        }
    }
}

/// Fixed-size message header for zero-copy operations
#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct MessageHeader {
    /// Magic number for validation
    pub magic: u32,
    
    /// Message type
    pub msg_type: u8,
    
    /// Message priority
    pub priority: u8,
    
    /// Reserved flags
    pub flags: u16,
    
    /// Message ID (first 8 bytes of UUID)
    pub message_id: u64,
    
    /// Payload size
    pub payload_size: u32,
    
    /// Checksum (CRC32)
    pub checksum: u32,
    
    /// Timestamp (nanoseconds since UNIX epoch)
    pub timestamp: u64,
    
    /// Sender ID
    pub sender_id: u32,
    
    /// Reserved for future use
    pub reserved: u32,
}

impl Default for MessageHeader {
    fn default() -> Self {
        Self {
            magic: MESSAGE_MAGIC,
            msg_type: MessageType::Error as u8,
            priority: MessagePriority::Normal as u8,
            flags: 0,
            message_id: 0,
            payload_size: 0,
            checksum: 0,
            timestamp: 0,
            sender_id: 0,
            reserved: 0,
        }
    }
}

impl MessageHeader {
    /// Create a new message header
    pub fn new(msg_type: MessageType, priority: MessagePriority, payload_size: u32) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let message_id = Uuid::new_v4().as_u128() as u64; // Use first 8 bytes
        
        Self {
            magic: MESSAGE_MAGIC,
            msg_type: msg_type as u8,
            priority: priority as u8,
            flags: 0,
            message_id,
            payload_size,
            checksum: 0, // Will be calculated later
            timestamp,
            sender_id: std::process::id(),
            reserved: 0,
        }
    }
    
    /// Validate message header
    pub fn is_valid(&self) -> bool {
        self.magic == MESSAGE_MAGIC && 
        self.payload_size <= (MAX_MESSAGE_SIZE - MESSAGE_HEADER_SIZE) as u32
    }
    
    /// Get message type
    pub fn message_type(&self) -> MessageType {
        MessageType::from(self.msg_type)
    }
    
    /// Get message priority
    pub fn priority(&self) -> MessagePriority {
        MessagePriority::from(self.priority)
    }
    
    /// Get timestamp as Duration since UNIX epoch
    pub fn timestamp(&self) -> Duration {
        Duration::from_nanos(self.timestamp)
    }
    
    /// Convert to byte array
    pub fn to_bytes(&self) -> [u8; MESSAGE_HEADER_SIZE] {
        unsafe { std::mem::transmute(*self) }
    }
    
    /// Create from byte array
    pub fn from_bytes(bytes: &[u8; MESSAGE_HEADER_SIZE]) -> Self {
        unsafe { std::mem::transmute(*bytes) }
    }
}

/// IPC message with zero-copy serialization
#[derive(Debug, Clone)]
pub struct Message {
    /// Message header
    pub header: MessageHeader,
    
    /// Message payload
    pub payload: Vec<u8>,
}

impl Message {
    /// Create a new message
    pub fn new(msg_type: MessageType, priority: MessagePriority, payload: Vec<u8>) -> Result<Self> {
        if payload.len() > MAX_MESSAGE_SIZE - MESSAGE_HEADER_SIZE {
            return Err(anyhow!("Payload too large: {} bytes", payload.len()));
        }
        
        let mut header = MessageHeader::new(msg_type, priority, payload.len() as u32);
        header.checksum = Self::calculate_checksum(&payload);
        
        Ok(Self { header, payload })
    }
    
    /// Create from serializable data
    pub fn from_data<T: Serialize>(
        msg_type: MessageType,
        priority: MessagePriority,
        data: &T,
    ) -> Result<Self> {
        let payload = bincode::serialize(data)
            .map_err(|e| anyhow!("Serialization failed: {}", e))?;
        
        Self::new(msg_type, priority, payload)
    }
    
    /// Deserialize payload to type T
    pub fn to_data<T: for<'a> Deserialize<'a>>(&self) -> Result<T> {
        bincode::deserialize(&self.payload)
            .map_err(|e| anyhow!("Deserialization failed: {}", e))
    }
    
    /// Serialize message to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MESSAGE_HEADER_SIZE + self.payload.len());
        bytes.extend_from_slice(&self.header.to_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }
    
    /// Deserialize message from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < MESSAGE_HEADER_SIZE {
            return Err(anyhow!("Message too short: {} bytes", bytes.len()));
        }
        
        let header_bytes: [u8; MESSAGE_HEADER_SIZE] = bytes[..MESSAGE_HEADER_SIZE]
            .try_into()
            .map_err(|_| anyhow!("Invalid header size"))?;
        
        let header = MessageHeader::from_bytes(&header_bytes);
        
        if !header.is_valid() {
            return Err(anyhow!("Invalid message header"));
        }
        
        let payload_start = MESSAGE_HEADER_SIZE;
        let payload_end = payload_start + header.payload_size as usize;
        
        if bytes.len() < payload_end {
            return Err(anyhow!("Incomplete message: expected {} bytes, got {}", payload_end, bytes.len()));
        }
        
        let payload = bytes[payload_start..payload_end].to_vec();
        
        // Verify checksum
        let calculated_checksum = Self::calculate_checksum(&payload);
        if calculated_checksum != header.checksum {
            return Err(anyhow!("Checksum mismatch: expected {}, got {}", header.checksum, calculated_checksum));
        }
        
        Ok(Self { header, payload })
    }
    
    /// Calculate CRC32 checksum
    fn calculate_checksum(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }
    
    /// Get message age
    pub fn age(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        Duration::from_nanos(now.saturating_sub(self.header.timestamp))
    }
    
    /// Check if message is expired
    pub fn is_expired(&self, max_age: Duration) -> bool {
        self.age() > max_age
    }
    
    /// Get total message size
    pub fn size(&self) -> usize {
        MESSAGE_HEADER_SIZE + self.payload.len()
    }
}

/// Payment request message payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentRequestPayload {
    /// Payment ID
    pub payment_id: String,
    
    /// Payment amount in cents
    pub amount: i64,
    
    /// Payment description
    pub description: String,
    
    /// Customer information
    pub customer: CustomerInfo,
    
    /// Preferred processor
    pub preferred_processor: Option<String>,
    
    /// Maximum processing time
    pub max_processing_time: Duration,
    
    /// Request timestamp
    pub timestamp: SystemTime,
}

/// Payment response message payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentResponsePayload {
    /// Payment ID
    pub payment_id: String,
    
    /// Processing result
    pub result: PaymentResult,
    
    /// Processing time
    pub processing_time: Duration,
    
    /// Processor used
    pub processor_used: String,
    
    /// Fee charged
    pub fee_charged: i64,
    
    /// Error message if failed
    pub error_message: Option<String>,
    
    /// Response timestamp
    pub timestamp: SystemTime,
}

/// Customer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomerInfo {
    /// Customer ID
    pub id: String,
    
    /// Customer name
    pub name: String,
    
    /// Customer email
    pub email: String,
}

/// Payment processing result
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentResult {
    /// Payment processed successfully
    Success,
    
    /// Payment failed
    Failed,
    
    /// Payment processing timeout
    Timeout,
    
    /// Insufficient funds
    InsufficientFunds,
    
    /// Invalid payment data
    InvalidData,
    
    /// Processor unavailable
    ProcessorUnavailable,
    
    /// Internal error
    InternalError,
}

/// Health check payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckPayload {
    /// Service name
    pub service_name: String,
    
    /// Service version
    pub version: String,
    
    /// Check timestamp
    pub timestamp: SystemTime,
    
    /// Additional metadata
    pub metadata: std::collections::HashMap<String, String>,
}

/// Health check response payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponsePayload {
    /// Service status
    pub status: ServiceStatus,
    
    /// Service uptime
    pub uptime: Duration,
    
    /// Memory usage in bytes
    pub memory_usage: u64,
    
    /// CPU usage percentage
    pub cpu_usage: f64,
    
    /// Active connections
    pub active_connections: u32,
    
    /// Response timestamp
    pub timestamp: SystemTime,
    
    /// Additional metrics
    pub metrics: std::collections::HashMap<String, f64>,
}

/// Service status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceStatus {
    /// Service is healthy
    Healthy,
    
    /// Service is degraded but operational
    Degraded,
    
    /// Service is unhealthy
    Unhealthy,
    
    /// Service is shutting down
    ShuttingDown,
}

/// Performance statistics payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStatsPayload {
    /// Requests per second
    pub requests_per_second: f64,
    
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    
    /// P95 latency in microseconds
    pub p95_latency_us: u64,
    
    /// P99 latency in microseconds
    pub p99_latency_us: u64,
    
    /// Success rate (0.0 to 1.0)
    pub success_rate: f64,
    
    /// Error rate (0.0 to 1.0)
    pub error_rate: f64,
    
    /// Memory usage in bytes
    pub memory_usage: u64,
    
    /// CPU usage percentage
    pub cpu_usage: f64,
    
    /// Network bytes sent
    pub network_bytes_sent: u64,
    
    /// Network bytes received
    pub network_bytes_received: u64,
    
    /// Timestamp of the statistics
    pub timestamp: SystemTime,
}

/// Worker status payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatusPayload {
    /// Worker ID
    pub worker_id: String,
    
    /// Worker type
    pub worker_type: String,
    
    /// Worker status
    pub status: WorkerStatus,
    
    /// Currently processing payments
    pub active_payments: u32,
    
    /// Total payments processed
    pub total_payments_processed: u64,
    
    /// Worker load (0.0 to 1.0)
    pub load: f64,
    
    /// Last heartbeat
    pub last_heartbeat: SystemTime,
    
    /// Performance metrics
    pub performance: PerformanceStatsPayload,
}

/// Worker status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerStatus {
    /// Worker is starting up
    Starting,
    
    /// Worker is idle
    Idle,
    
    /// Worker is processing
    Processing,
    
    /// Worker is overloaded
    Overloaded,
    
    /// Worker is shutting down
    ShuttingDown,
    
    /// Worker has crashed
    Crashed,
}

/// Load balancer update payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerUpdatePayload {
    /// Update type
    pub update_type: LoadBalancerUpdateType,
    
    /// Server information
    pub server_info: ServerInfo,
    
    /// Update timestamp
    pub timestamp: SystemTime,
}

/// Load balancer update type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoadBalancerUpdateType {
    /// Server added to pool
    ServerAdded,
    
    /// Server removed from pool
    ServerRemoved,
    
    /// Server marked as healthy
    ServerHealthy,
    
    /// Server marked as unhealthy
    ServerUnhealthy,
    
    /// Server load updated
    LoadUpdated,
    
    /// Configuration updated
    ConfigUpdated,
}

/// Server information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Server ID
    pub id: String,
    
    /// Server address
    pub address: String,
    
    /// Server port
    pub port: u16,
    
    /// Server weight
    pub weight: f64,
    
    /// Server health status
    pub health_status: ServiceStatus,
    
    /// Current load (0.0 to 1.0)
    pub load: f64,
    
    /// Response time in microseconds
    pub response_time_us: u64,
    
    /// Active connections
    pub active_connections: u32,
}

/// Error message payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    /// Error code
    pub code: String,
    
    /// Error message
    pub message: String,
    
    /// Error details
    pub details: Option<String>,
    
    /// Service that generated the error
    pub service: String,
    
    /// Error timestamp
    pub timestamp: SystemTime,
    
    /// Request ID if available
    pub request_id: Option<String>,
}

/// Message builder for convenient message creation
pub struct MessageBuilder {
    msg_type: MessageType,
    priority: MessagePriority,
}

impl MessageBuilder {
    /// Create a new message builder
    pub fn new(msg_type: MessageType) -> Self {
        Self {
            msg_type,
            priority: MessagePriority::Normal,
        }
    }
    
    /// Set message priority
    pub fn priority(mut self, priority: MessagePriority) -> Self {
        self.priority = priority;
        self
    }
    
    /// Build payment request message
    pub fn payment_request(self, payload: PaymentRequestPayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build payment response message
    pub fn payment_response(self, payload: PaymentResponsePayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build health check message
    pub fn health_check(self, payload: HealthCheckPayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build health check response message
    pub fn health_check_response(self, payload: HealthCheckResponsePayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build performance stats message
    pub fn performance_stats(self, payload: PerformanceStatsPayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build worker status message
    pub fn worker_status(self, payload: WorkerStatusPayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build load balancer update message
    pub fn load_balancer_update(self, payload: LoadBalancerUpdatePayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build error message
    pub fn error(self, payload: ErrorPayload) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, &payload)
    }
    
    /// Build with custom payload
    pub fn build<T: Serialize>(self, payload: &T) -> Result<Message> {
        Message::from_data(self.msg_type, self.priority, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_header() {
        let header = MessageHeader::new(MessageType::PaymentRequest, MessagePriority::High, 100);
        assert_eq!(header.magic, MESSAGE_MAGIC);
        assert_eq!(header.message_type(), MessageType::PaymentRequest);
        assert_eq!(header.priority(), MessagePriority::High);
        assert_eq!(header.payload_size, 100);
        assert!(header.is_valid());
    }

    #[test]
    fn test_message_serialization() {
        let payload = PaymentRequestPayload {
            payment_id: "test-123".to_string(),
            amount: 10000,
            description: "Test payment".to_string(),
            customer: CustomerInfo {
                id: "customer-1".to_string(),
                name: "John Doe".to_string(),
                email: "john@example.com".to_string(),
            },
            preferred_processor: None,
            max_processing_time: Duration::from_secs(30),
            timestamp: SystemTime::now(),
        };

        let msg = Message::from_data(MessageType::PaymentRequest, MessagePriority::Normal, &payload).unwrap();
        let bytes = msg.to_bytes();
        let restored_msg = Message::from_bytes(&bytes).unwrap();
        
        assert_eq!(msg.header.message_type(), restored_msg.header.message_type());
        assert_eq!(msg.header.priority(), restored_msg.header.priority());
        assert_eq!(msg.payload, restored_msg.payload);
        
        let restored_payload: PaymentRequestPayload = restored_msg.to_data().unwrap();
        assert_eq!(payload.payment_id, restored_payload.payment_id);
        assert_eq!(payload.amount, restored_payload.amount);
    }

    #[test]
    fn test_message_builder() {
        let payload = HealthCheckPayload {
            service_name: "api-server".to_string(),
            version: "1.0.0".to_string(),
            timestamp: SystemTime::now(),
            metadata: std::collections::HashMap::new(),
        };

        let msg = MessageBuilder::new(MessageType::HealthCheck)
            .priority(MessagePriority::Low)
            .health_check(payload)
            .unwrap();

        assert_eq!(msg.header.message_type(), MessageType::HealthCheck);
        assert_eq!(msg.header.priority(), MessagePriority::Low);
    }

    #[test]
    fn test_message_checksum() {
        let msg = Message::new(MessageType::HealthCheck, MessagePriority::Normal, b"test".to_vec()).unwrap();
        let bytes = msg.to_bytes();
        let restored_msg = Message::from_bytes(&bytes).unwrap();
        assert_eq!(msg.header.checksum, restored_msg.header.checksum);
        
        // Test corrupted data
        let mut corrupted_bytes = bytes;
        corrupted_bytes[MESSAGE_HEADER_SIZE] = 0xFF; // Corrupt first payload byte
        assert!(Message::from_bytes(&corrupted_bytes).is_err());
    }

    #[test]
    fn test_message_expiration() {
        let msg = Message::new(MessageType::HealthCheck, MessagePriority::Normal, vec![]).unwrap();
        assert!(!msg.is_expired(Duration::from_secs(1)));
        
        std::thread::sleep(Duration::from_millis(10));
        assert!(msg.age() >= Duration::from_millis(10));
    }
}

// Tipos adicionais para compatibilidade com IpcManager
pub type PaymentMessage = Message;
pub type HealthMessage = Message;
pub type MetricsMessage = Message;
pub type ControlMessage = Message;

/// Message payload variants for pattern matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePayload {
    Payment(PaymentRequestPayload),
    Health(HealthCheckPayload),
    Metrics(PerformanceStatsPayload),
    Control(String), // Simple control commands
}

impl Message {
    /// Get the payload as a typed enum
    pub fn get_payload(&self) -> Result<MessagePayload> {
        match self.header.message_type() {
            MessageType::PaymentRequest => {
                let payload: PaymentRequestPayload = bincode::deserialize(&self.payload)?;
                Ok(MessagePayload::Payment(payload))
            },
            MessageType::HealthCheck => {
                let payload: HealthCheckPayload = bincode::deserialize(&self.payload)?;
                Ok(MessagePayload::Health(payload))
            },
            MessageType::MetricsReport => {
                let payload: PerformanceStatsPayload = bincode::deserialize(&self.payload)?;
                Ok(MessagePayload::Metrics(payload))
            },
            MessageType::Shutdown | MessageType::ConfigUpdate => {
                let command = String::from_utf8(self.payload.clone())?;
                Ok(MessagePayload::Control(command))
            },
            _ => Err(anyhow!("Unsupported message type for payload extraction")),
        }
    }
}