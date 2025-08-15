//! High-performance HTTP server implementation for Rinha Backend 2025
//! 
//! This module provides an ultra-fast HTTP server optimized for the specific
//! requirements of the Rinha Backend challenge with manual parsing and
//! zero-allocation response building.

pub mod http_parser;
pub mod handler;
pub mod response;

use handler::{HandlerContext, handle_request};
use http_parser::HttpParser;
use response::ResponseBuilder;
use crate::storage::StorageEngine;

use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn, error, debug};

/// High-performance HTTP server
pub struct HttpServer {
    /// Handler context with storage and response builder
    context: Arc<HandlerContext>,
    /// Server configuration
    config: ServerConfig,
    /// Performance metrics
    metrics: ServerMetrics,
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Port to bind to
    pub port: u16,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Maximum request size in bytes
    pub max_request_size: usize,
    /// Enable keep-alive connections
    pub keep_alive: bool,
    /// Connection buffer size
    pub buffer_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            max_connections: 10000,
            request_timeout: 30,
            max_request_size: 8192,  // 8KB should be enough for our payloads
            keep_alive: true,
            buffer_size: 4096,
        }
    }
}

/// Server performance metrics
#[derive(Default)]
struct ServerMetrics {
    total_connections: std::sync::atomic::AtomicU64,
    active_connections: std::sync::atomic::AtomicU64,
    total_requests: std::sync::atomic::AtomicU64,
    requests_per_second: std::sync::atomic::AtomicU64,
    avg_response_time_micros: std::sync::atomic::AtomicU64,
}

impl HttpServer {
    /// Create new HTTP server
    pub fn new(storage: Arc<StorageEngine>, config: ServerConfig) -> Self {
        let context = Arc::new(HandlerContext::new(storage));
        
        Self {
            context,
            config,
            metrics: ServerMetrics::default(),
        }
    }
    
    /// Start the server
    pub async fn start(&self) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        
        info!("🚀 HTTP server starting on {}", addr);
        info!("📊 Config: max_connections={}, buffer_size={}, keep_alive={}", 
              self.config.max_connections, self.config.buffer_size, self.config.keep_alive);
        
        // Start metrics reporting task
        let metrics = Arc::new(&self.metrics);
        let metrics_task = tokio::spawn(Self::metrics_reporter(metrics.clone()));
        
        // Connection semaphore for limiting concurrent connections
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_connections));
        
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    // Acquire connection permit
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            warn!("Connection limit reached, dropping connection from {}", addr);
                            continue;
                        }
                    };
                    
                    // Update connection metrics
                    self.metrics.total_connections
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.metrics.active_connections
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    
                    // Handle connection
                    let context = self.context.clone();
                    let config = self.config.clone();
                    let metrics = Arc::new(&self.metrics);
                    
                    tokio::spawn(async move {
                        let _permit = permit; // Keep permit alive for connection duration
                        
                        if let Err(e) = Self::handle_connection(context, stream, config, metrics).await {
                            debug!("Connection error from {}: {}", addr, e);
                        }
                        
                        // Connection closed, update metrics
                        metrics.active_connections
                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
    
    /// Handle individual TCP connection
    async fn handle_connection(
        context: Arc<HandlerContext>,
        mut stream: TcpStream,
        config: ServerConfig,
        metrics: Arc<&ServerMetrics>,
    ) -> Result<()> {
        let mut buffer = vec![0u8; config.buffer_size];
        let mut request_buffer = Vec::with_capacity(config.max_request_size);
        
        loop {
            // Set timeout for request
            let timeout_duration = tokio::time::Duration::from_secs(config.request_timeout);
            
            let request_result = tokio::time::timeout(
                timeout_duration,
                Self::read_request(&mut stream, &mut buffer, &mut request_buffer, config.max_request_size)
            ).await;
            
            match request_result {
                Ok(Ok(request_size)) => {
                    if request_size == 0 {
                        // Connection closed by client
                        debug!("Client closed connection");
                        break;
                    }
                    
                    // Process request
                    let start_time = std::time::Instant::now();
                    
                    match Self::process_request(
                        &context,
                        &request_buffer[..request_size],
                        &mut stream,
                    ).await {
                        Ok(_) => {
                            // Update metrics
                            let response_time = start_time.elapsed().as_micros() as u64;
                            metrics.total_requests
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            
                            // Update average response time (simple moving average)
                            let current_avg = metrics.avg_response_time_micros
                                .load(std::sync::atomic::Ordering::Relaxed);
                            let new_avg = if current_avg == 0 {
                                response_time
                            } else {
                                (current_avg * 7 + response_time) / 8 // Exponential moving average
                            };
                            metrics.avg_response_time_micros
                                .store(new_avg, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => {
                            warn!("Request processing error: {}", e);
                            break;
                        }
                    }
                    
                    // Clear request buffer for next request
                    request_buffer.clear();
                    
                    // Check if we should keep the connection alive
                    if !config.keep_alive {
                        break;
                    }
                }
                Ok(Err(e)) => {
                    warn!("Request read error: {}", e);
                    break;
                }
                Err(_) => {
                    warn!("Request timeout");
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Read complete HTTP request from stream
    async fn read_request(
        stream: &mut TcpStream,
        buffer: &mut [u8],
        request_buffer: &mut Vec<u8>,
        max_size: usize,
    ) -> Result<usize> {
        let mut total_read = 0;
        let mut headers_complete = false;
        let mut content_length = 0;
        let mut body_bytes_needed = 0;
        
        loop {
            let bytes_read = stream.read(buffer).await?;
            
            if bytes_read == 0 {
                return Ok(total_read); // Connection closed
            }
            
            // Check size limit
            if total_read + bytes_read > max_size {
                return Err(anyhow::anyhow!("Request too large"));
            }
            
            // Append to request buffer
            request_buffer.extend_from_slice(&buffer[..bytes_read]);
            total_read += bytes_read;
            
            if !headers_complete {
                // Look for end of headers (\r\n\r\n)
                if let Some(headers_end) = find_headers_end(request_buffer) {
                    headers_complete = true;
                    
                    // Parse Content-Length header
                    let headers_part = &request_buffer[..headers_end];
                    content_length = parse_content_length(headers_part);
                    
                    let body_start = headers_end + 4; // Skip \r\n\r\n
                    let body_bytes_received = total_read.saturating_sub(body_start);
                    body_bytes_needed = content_length.saturating_sub(body_bytes_received);
                    
                    if body_bytes_needed == 0 {
                        // Request is complete
                        return Ok(total_read);
                    }
                }
            } else {
                // Headers are complete, check if we have full body
                body_bytes_needed = body_bytes_needed.saturating_sub(bytes_read);
                
                if body_bytes_needed == 0 {
                    // Request is complete
                    return Ok(total_read);
                }
            }
            
            // Prevent infinite loops for malformed requests
            if !headers_complete && total_read > 4096 {
                return Err(anyhow::anyhow!("Headers too large"));
            }
        }
    }
    
    /// Process HTTP request and send response
    async fn process_request(
        context: &HandlerContext,
        request_data: &[u8],
        stream: &mut TcpStream,
    ) -> Result<()> {
        // Parse HTTP request
        let request = HttpParser::parse_request(request_data)
            .map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;
        
        // Handle request
        let response = handle_request(context, request).await
            .map_err(|e| anyhow::anyhow!("Handler error: {}", e))?;
        
        // Send response
        match response.body.is_empty() {
            true => {
                // Use raw template for empty responses (faster)
                let template = match response.status_code {
                    200 => response::raw_templates::OK_EMPTY,
                    204 => response::raw_templates::NO_CONTENT,
                    400 => response::raw_templates::BAD_REQUEST,
                    404 => response::raw_templates::NOT_FOUND,
                    500 => response::raw_templates::INTERNAL_ERROR,
                    _ => response::raw_templates::OK_EMPTY,
                };
                stream.write_all(template).await?;
            }
            false => {
                // Send full response
                stream.write_all(&response.body).await?;
            }
        }
        
        stream.flush().await?;
        Ok(())
    }
    
    /// Metrics reporting task
    async fn metrics_reporter(metrics: Arc<&ServerMetrics>) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        let mut last_requests = 0u64;
        
        loop {
            interval.tick().await;
            
            let total_conn = metrics.total_connections.load(std::sync::atomic::Ordering::Relaxed);
            let active_conn = metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed);
            let total_req = metrics.total_requests.load(std::sync::atomic::Ordering::Relaxed);
            let avg_response = metrics.avg_response_time_micros.load(std::sync::atomic::Ordering::Relaxed);
            
            let rps = (total_req - last_requests) / 10; // Requests per second
            last_requests = total_req;
            
            metrics.requests_per_second.store(rps, std::sync::atomic::Ordering::Relaxed);
            
            info!(
                "📊 Server metrics: connections={}/{}, requests={}, rps={}, avg_response={}μs",
                active_conn, total_conn, total_req, rps, avg_response
            );
        }
    }
    
    /// Get current server metrics
    pub fn get_metrics(&self) -> (u64, u64, u64, u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.metrics.total_connections.load(Ordering::Relaxed),
            self.metrics.active_connections.load(Ordering::Relaxed),
            self.metrics.total_requests.load(Ordering::Relaxed),
            self.metrics.requests_per_second.load(Ordering::Relaxed),
            self.metrics.avg_response_time_micros.load(Ordering::Relaxed),
        )
    }
}

/// Find end of HTTP headers (\r\n\r\n)
fn find_headers_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Parse Content-Length header from headers
fn parse_content_length(headers: &[u8]) -> usize {
    let headers_str = match std::str::from_utf8(headers) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    
    for line in headers_str.lines() {
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(value) = line.split(':').nth(1) {
                if let Ok(length) = value.trim().parse::<usize>() {
                    return length;
                }
            }
        }
    }
    
    0
}

/// Public API functions
pub async fn start_api(port: u16, _socket_path: Option<String>) -> Result<()> {
    let storage = Arc::new(crate::storage::StorageEngine::with_default_config());
    
    let config = ServerConfig {
        port,
        ..Default::default()
    };
    
    let server = HttpServer::new(storage, config);
    server.start().await
}

pub async fn start_load_balancer(port: u16) -> Result<()> {
    // For now, we'll implement this as a simple proxy
    // In a real implementation, this would distribute load across multiple API instances
    info!("🔄 Load balancer starting on port {}", port);
    
    // For the contest, we can just start a regular API server
    start_api(port, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_find_headers_end() {
        let data = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\nBody";
        let end = find_headers_end(data).unwrap();
        assert_eq!(end, 29); // Position of first \r in \r\n\r\n
    }
    
    #[test]
    fn test_parse_content_length() {
        let headers = b"POST / HTTP/1.1\r\nContent-Length: 42\r\nHost: localhost\r\n\r\n";
        let length = parse_content_length(headers);
        assert_eq!(length, 42);
        
        let headers_no_cl = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let length = parse_content_length(headers_no_cl);
        assert_eq!(length, 0);
    }
    
    #[tokio::test]
    async fn test_server_creation() {
        let storage = Arc::new(crate::storage::StorageEngine::with_default_config());
        let config = ServerConfig::default();
        let _server = HttpServer::new(storage, config);
        
        // Just test that we can create a server without panicking
    }
}