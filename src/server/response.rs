//! Ultra-fast HTTP response building with pre-compiled templates
//! 
//! This module provides zero-allocation response building using pre-compiled
//! byte arrays and efficient string concatenation for maximum performance.

use std::fmt::Write;

/// HTTP response structure optimized for our use cases
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Create new response
    pub fn new(status_code: u16) -> Self {
        Self {
            status_code,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
    
    /// Add header
    pub fn header(mut self, name: String, value: String) -> Self {
        self.headers.push((name, value));
        self
    }
    
    /// Set body
    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
    
    /// Convert to raw HTTP bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut response = Vec::with_capacity(512);
        
        // Status line
        write!(&mut response, "HTTP/1.1 {} {}\r\n", self.status_code, status_text(self.status_code))
            .unwrap();
        
        // Headers
        for (name, value) in &self.headers {
            write!(&mut response, "{}: {}\r\n", name, value).unwrap();
        }
        
        // Content-Length if not already set
        if !self.headers.iter().any(|(name, _)| name.eq_ignore_ascii_case("content-length")) {
            write!(&mut response, "Content-Length: {}\r\n", self.body.len()).unwrap();
        }
        
        // End of headers
        response.extend_from_slice(b"\r\n");
        
        // Body
        response.extend_from_slice(&self.body);
        
        response
    }
}

/// Ultra-fast response builder using pre-compiled templates
pub struct ResponseBuilder {
    /// Pre-compiled response templates for common cases
    templates: ResponseTemplates,
}

/// Pre-compiled response templates for maximum performance
struct ResponseTemplates {
    /// 200 OK with empty body
    ok_empty: &'static [u8],
    /// 204 No Content
    no_content: &'static [u8],
    /// 400 Bad Request
    bad_request: &'static [u8],
    /// 404 Not Found
    not_found: &'static [u8],
    /// 500 Internal Server Error
    internal_error: &'static [u8],
    /// OPTIONS response for CORS
    options: &'static [u8],
}

impl ResponseTemplates {
    fn new() -> Self {
        Self {
            ok_empty: b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            no_content: b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            bad_request: b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            not_found: b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            internal_error: b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
            options: b"HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type\r\nContent-Length: 0\r\n\r\n",
        }
    }
}

impl ResponseBuilder {
    /// Create new response builder
    pub fn new() -> Self {
        Self {
            templates: ResponseTemplates::new(),
        }
    }
    
    /// 200 OK with empty body (most common for /payments)
    pub fn ok_empty(&self) -> HttpResponse {
        HttpResponse {
            status_code: 200,
            headers: vec![],
            body: self.templates.ok_empty.to_vec(),
        }
    }
    
    /// 200 OK with JSON body
    pub fn ok_json(&self, json: &str) -> HttpResponse {
        let mut response = Vec::with_capacity(128 + json.len());
        
        // Pre-compiled header
        response.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
        response.extend_from_slice(b"Content-Type: application/json\r\n");
        response.extend_from_slice(b"Connection: keep-alive\r\n");
        response.extend_from_slice(b"Content-Length: ");
        response.extend_from_slice(json.len().to_string().as_bytes());
        response.extend_from_slice(b"\r\n\r\n");
        response.extend_from_slice(json.as_bytes());
        
        HttpResponse {
            status_code: 200,
            headers: vec![],
            body: response,
        }
    }
    
    /// 204 No Content (for purge endpoint)
    pub fn no_content(&self) -> HttpResponse {
        HttpResponse {
            status_code: 204,
            headers: vec![],
            body: self.templates.no_content.to_vec(),
        }
    }
    
    /// 400 Bad Request
    pub fn bad_request(&self, message: &str) -> HttpResponse {
        if message.is_empty() {
            // Use pre-compiled template for empty message
            HttpResponse {
                status_code: 400,
                headers: vec![],
                body: self.templates.bad_request.to_vec(),
            }
        } else {
            // Build response with error message
            let error_json = format!(r#"{{"error":"{}"}}"#, escape_json(message));
            self.error_json(400, &error_json)
        }
    }
    
    /// 404 Not Found
    pub fn not_found(&self) -> HttpResponse {
        HttpResponse {
            status_code: 404,
            headers: vec![],
            body: self.templates.not_found.to_vec(),
        }
    }
    
    /// 500 Internal Server Error
    pub fn internal_error(&self) -> HttpResponse {
        HttpResponse {
            status_code: 500,
            headers: vec![],
            body: self.templates.internal_error.to_vec(),
        }
    }
    
    /// OPTIONS response for CORS
    pub fn options(&self) -> HttpResponse {
        HttpResponse {
            status_code: 200,
            headers: vec![],
            body: self.templates.options.to_vec(),
        }
    }
    
    /// Generic JSON error response
    fn error_json(&self, status_code: u16, json: &str) -> HttpResponse {
        let mut response = Vec::with_capacity(128 + json.len());
        
        write!(&mut response, "HTTP/1.1 {} {}\r\n", status_code, status_text(status_code)).unwrap();
        response.extend_from_slice(b"Content-Type: application/json\r\n");
        response.extend_from_slice(b"Connection: keep-alive\r\n");
        response.extend_from_slice(b"Content-Length: ");
        response.extend_from_slice(json.len().to_string().as_bytes());
        response.extend_from_slice(b"\r\n\r\n");
        response.extend_from_slice(json.as_bytes());
        
        HttpResponse {
            status_code,
            headers: vec![],
            body: response,
        }
    }
}

impl Default for ResponseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Get status text for HTTP status code
fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

/// Escape JSON string (minimal implementation for error messages)
fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

/// Raw response templates for maximum performance
pub mod raw_templates {
    /// Pre-compiled responses as byte arrays for direct sending
    
    pub const OK_EMPTY: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    
    pub const NO_CONTENT: &[u8] = b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    
    pub const BAD_REQUEST: &[u8] = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    
    pub const NOT_FOUND: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    
    pub const INTERNAL_ERROR: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    
    pub const OPTIONS: &[u8] = b"HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type\r\nContent-Length: 0\r\n\r\n";
    
    /// Build JSON response with pre-allocated buffer
    pub fn build_json_response(json: &str, buffer: &mut Vec<u8>) {
        buffer.clear();
        buffer.reserve(128 + json.len());
        
        buffer.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
        buffer.extend_from_slice(b"Content-Type: application/json\r\n");
        buffer.extend_from_slice(b"Connection: keep-alive\r\n");
        buffer.extend_from_slice(b"Content-Length: ");
        buffer.extend_from_slice(json.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n\r\n");
        buffer.extend_from_slice(json.as_bytes());
    }
}

/// Optimized response builders for specific use cases
pub mod optimized {
    use super::*;
    
    /// Thread-local response buffer to avoid allocations
    thread_local! {
        static RESPONSE_BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(1024));
    }
    
    /// Build JSON response using thread-local buffer
    pub fn build_json_response_tl(json: &str) -> Vec<u8> {
        RESPONSE_BUFFER.with(|buffer| {
            let mut buf = buffer.borrow_mut();
            raw_templates::build_json_response(json, &mut buf);
            buf.clone()
        })
    }
    
    /// Fast payment summary response builder
    pub fn build_summary_response(
        default_requests: u64,
        default_amount: f64,
        fallback_requests: u64,
        fallback_amount: f64,
    ) -> Vec<u8> {
        RESPONSE_BUFFER.with(|buffer| {
            let mut buf = buffer.borrow_mut();
            buf.clear();
            
            // Build JSON manually for maximum speed
            let json_capacity = 150; // Estimated JSON size
            let mut json = String::with_capacity(json_capacity);
            
            json.push('{');
            json.push_str("\"default\":{\"totalRequests\":");
            json.push_str(&default_requests.to_string());
            json.push_str(",\"totalAmount\":");
            json.push_str(&default_amount.to_string());
            json.push_str("},\"fallback\":{\"totalRequests\":");
            json.push_str(&fallback_requests.to_string());
            json.push_str(",\"totalAmount\":");
            json.push_str(&fallback_amount.to_string());
            json.push_str("}}");
            
            raw_templates::build_json_response(&json, &mut buf);
            buf.clone()
        })
    }
}

/// Response streaming utilities for large responses
pub mod streaming {
    use std::io::{self, Write};
    
    /// Streaming response writer
    pub struct StreamingResponse<W: Write> {
        writer: W,
        headers_sent: bool,
    }
    
    impl<W: Write> StreamingResponse<W> {
        pub fn new(writer: W) -> Self {
            Self {
                writer,
                headers_sent: false,
            }
        }
        
        /// Send headers
        pub fn send_headers(&mut self, status_code: u16, content_type: &str) -> io::Result<()> {
            if self.headers_sent {
                return Ok(());
            }
            
            write!(
                self.writer,
                "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nTransfer-Encoding: chunked\r\n\r\n",
                status_code,
                super::status_text(status_code),
                content_type
            )?;
            
            self.headers_sent = true;
            Ok(())
        }
        
        /// Send chunk of data
        pub fn send_chunk(&mut self, data: &[u8]) -> io::Result<()> {
            if !self.headers_sent {
                self.send_headers(200, "application/json")?;
            }
            
            // Write chunk size in hex
            write!(self.writer, "{:x}\r\n", data.len())?;
            
            // Write chunk data
            self.writer.write_all(data)?;
            self.writer.write_all(b"\r\n")?;
            
            Ok(())
        }
        
        /// End the response
        pub fn end(&mut self) -> io::Result<()> {
            if !self.headers_sent {
                self.send_headers(200, "application/json")?;
            }
            
            // Send final chunk (size 0)
            self.writer.write_all(b"0\r\n\r\n")?;
            self.writer.flush()?;
            
            Ok(())
        }
    }
}

/// Benchmark utilities for response building performance
#[cfg(feature = "bench")]
pub mod bench_utils {
    use super::*;
    use std::time::Instant;
    
    /// Benchmark response building performance
    pub fn benchmark_response_building(iterations: usize) {
        let builder = ResponseBuilder::new();
        
        // Benchmark empty OK responses
        let start = Instant::now();
        for _ in 0..iterations {
            let _response = builder.ok_empty();
        }
        let empty_duration = start.elapsed();
        
        // Benchmark JSON responses
        let json = r#"{"default":{"totalRequests":1000,"totalAmount":19900.0},"fallback":{"totalRequests":500,"totalAmount":9950.0}}"#;
        let start = Instant::now();
        for _ in 0..iterations {
            let _response = builder.ok_json(json);
        }
        let json_duration = start.elapsed();
        
        // Benchmark optimized summary responses
        let start = Instant::now();
        for _ in 0..iterations {
            let _response = optimized::build_summary_response(1000, 19900.0, 500, 9950.0);
        }
        let optimized_duration = start.elapsed();
        
        println!("Response building benchmarks ({} iterations):", iterations);
        println!("  Empty responses: {:?} ({:.2} ns/op)", empty_duration, empty_duration.as_nanos() as f64 / iterations as f64);
        println!("  JSON responses: {:?} ({:.2} ns/op)", json_duration, json_duration.as_nanos() as f64 / iterations as f64);
        println!("  Optimized summary: {:?} ({:.2} ns/op)", optimized_duration, optimized_duration.as_nanos() as f64 / iterations as f64);
    }
}

/// HTTP/2 response utilities (for future optimization)
#[cfg(feature = "http2")]
pub mod http2_utils {
    use super::*;
    
    /// HTTP/2 frame types
    #[derive(Debug, Clone, Copy)]
    pub enum FrameType {
        Data = 0x0,
        Headers = 0x1,
        Priority = 0x2,
        RstStream = 0x3,
        Settings = 0x4,
        PushPromise = 0x5,
        Ping = 0x6,
        GoAway = 0x7,
        WindowUpdate = 0x8,
        Continuation = 0x9,
    }
    
    /// Build HTTP/2 DATA frame
    pub fn build_data_frame(stream_id: u32, data: &[u8], end_stream: bool) -> Vec<u8> {
        let mut frame = Vec::with_capacity(9 + data.len());
        
        // Length (24 bits)
        let length = data.len() as u32;
        frame.push((length >> 16) as u8);
        frame.push((length >> 8) as u8);
        frame.push(length as u8);
        
        // Type (8 bits)
        frame.push(FrameType::Data as u8);
        
        // Flags (8 bits)
        let flags = if end_stream { 0x1 } else { 0x0 };
        frame.push(flags);
        
        // Stream ID (31 bits + 1 reserved bit)
        frame.extend_from_slice(&(stream_id & 0x7FFFFFFF).to_be_bytes());
        
        // Payload
        frame.extend_from_slice(data);
        
        frame
    }
    
    /// Build HTTP/2 HEADERS frame with pseudo-headers
    pub fn build_headers_frame(stream_id: u32, status: u16, content_type: &str, content_length: usize) -> Vec<u8> {
        // This is a simplified implementation
        // In reality, we'd need HPACK compression
        
        let mut headers = Vec::new();
        
        // :status pseudo-header (simplified encoding)
        headers.extend_from_slice(format!(":status: {}\r\n", status).as_bytes());
        headers.extend_from_slice(format!("content-type: {}\r\n", content_type).as_bytes());
        headers.extend_from_slice(format!("content-length: {}\r\n", content_length).as_bytes());
        
        build_data_frame(stream_id, &headers, false)
    }
}

/// Response compression utilities
pub mod compression {
    use std::io::Write;
    
    /// Gzip compression for response bodies
    #[cfg(feature = "compression")]
    pub fn gzip_compress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        use flate2::{Compression, write::GzEncoder};
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(data)?;
        encoder.finish()
    }
    
    /// Check if client accepts gzip encoding
    pub fn accepts_gzip(accept_encoding: Option<&str>) -> bool {
        accept_encoding
            .map(|ae| ae.contains("gzip"))
            .unwrap_or(false)
    }
    
    /// Build compressed response
    #[cfg(feature = "compression")]
    pub fn build_compressed_response(
        status_code: u16,
        content_type: &str,
        data: &[u8],
        accept_encoding: Option<&str>,
    ) -> Vec<u8> {
        if accepts_gzip(accept_encoding) && data.len() > 1024 {
            // Only compress larger responses
            if let Ok(compressed) = gzip_compress(data) {
                if compressed.len() < data.len() {
                    // Compression was beneficial
                    let mut response = Vec::with_capacity(128 + compressed.len());
                    
                    write!(&mut response, "HTTP/1.1 {} {}\r\n", status_code, super::status_text(status_code)).unwrap();
                    write!(&mut response, "Content-Type: {}\r\n", content_type).unwrap();
                    write!(&mut response, "Content-Encoding: gzip\r\n").unwrap();
                    write!(&mut response, "Content-Length: {}\r\n", compressed.len()).unwrap();
                    response.extend_from_slice(b"\r\n");
                    response.extend_from_slice(&compressed);
                    
                    return response;
                }
            }
        }
        
        // Fallback to uncompressed
        let mut response = Vec::with_capacity(128 + data.len());
        
        write!(&mut response, "HTTP/1.1 {} {}\r\n", status_code, super::status_text(status_code)).unwrap();
        write!(&mut response, "Content-Type: {}\r\n", content_type).unwrap();
        write!(&mut response, "Content-Length: {}\r\n", data.len()).unwrap();
        response.extend_from_slice(b"\r\n");
        response.extend_from_slice(data);
        
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_response_builder_ok_empty() {
        let builder = ResponseBuilder::new();
        let response = builder.ok_empty();
        
        assert_eq!(response.status_code, 200);
        assert!(response.body.starts_with(b"HTTP/1.1 200 OK"));
        assert!(response.body.ends_with(b"\r\n\r\n"));
    }
    
    #[test]
    fn test_response_builder_json() {
        let builder = ResponseBuilder::new();
        let json = r#"{"test":true}"#;
        let response = builder.ok_json(json);
        
        assert_eq!(response.status_code, 200);
        assert!(response.body.ends_with(json.as_bytes()));
        
        let response_str = String::from_utf8(response.body).unwrap();
        assert!(response_str.contains("Content-Type: application/json"));
        assert!(response_str.contains(&format!("Content-Length: {}", json.len())));
    }
    
    #[test]
    fn test_response_builder_bad_request() {
        let builder = ResponseBuilder::new();
        
        // Test with empty message
        let response = builder.bad_request("");
        assert_eq!(response.status_code, 400);
        
        // Test with custom message
        let response = builder.bad_request("Invalid data");
        assert_eq!(response.status_code, 400);
        let response_str = String::from_utf8(response.body).unwrap();
        assert!(response_str.contains("Invalid data"));
    }
    
    #[test]
    fn test_raw_templates() {
        use raw_templates::*;
        
        assert!(OK_EMPTY.starts_with(b"HTTP/1.1 200 OK"));
        assert!(BAD_REQUEST.starts_with(b"HTTP/1.1 400 Bad Request"));
        assert!(NOT_FOUND.starts_with(b"HTTP/1.1 404 Not Found"));
        assert!(INTERNAL_ERROR.starts_with(b"HTTP/1.1 500 Internal Server Error"));
        assert!(OPTIONS.contains(b"Access-Control-Allow-Origin"));
    }
    
    #[test]
    fn test_optimized_json_response() {
        use optimized::*;
        
        let json = r#"{"test":"value"}"#;
        let response = build_json_response_tl(json);
        
        let response_str = String::from_utf8(response).unwrap();
        assert!(response_str.contains("HTTP/1.1 200 OK"));
        assert!(response_str.contains("Content-Type: application/json"));
        assert!(response_str.ends_with(json));
    }
    
    #[test]
    fn test_summary_response_builder() {
        use optimized::*;
        
        let response = build_summary_response(100, 1990.0, 50, 995.0);
        let response_str = String::from_utf8(response).unwrap();
        
        assert!(response_str.contains("\"totalRequests\":100"));
        assert!(response_str.contains("\"totalAmount\":1990"));
        assert!(response_str.contains("\"totalRequests\":50"));
        assert!(response_str.contains("\"totalAmount\":995"));
    }
    
    #[test]
    fn test_escape_json() {
        assert_eq!(escape_json("hello"), "hello");
        assert_eq!(escape_json("hello\"world"), "hello\\\"world");
        assert_eq!(escape_json("line1\nline2"), "line1\\nline2");
        assert_eq!(escape_json("tab\there"), "tab\\there");
    }
    
    #[test]
    fn test_status_text() {
        assert_eq!(status_text(200), "OK");
        assert_eq!(status_text(400), "Bad Request");
        assert_eq!(status_text(404), "Not Found");
        assert_eq!(status_text(500), "Internal Server Error");
        assert_eq!(status_text(999), "Unknown");
    }
    
    #[test]
    fn test_response_to_bytes() {
        let response = HttpResponse::new(200)
            .header("Content-Type".to_string(), "application/json".to_string())
            .body(b"test".to_vec());
        
        let bytes = response.to_bytes();
        let response_str = String::from_utf8(bytes).unwrap();
        
        assert!(response_str.starts_with("HTTP/1.1 200 OK"));
        assert!(response_str.contains("Content-Type: application/json"));
        assert!(response_str.contains("Content-Length: 4"));
        assert!(response_str.ends_with("test"));
    }
    
    #[test]
    fn test_streaming_response() {
        use streaming::*;
        use std::io::Cursor;
        
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let mut streaming = StreamingResponse::new(cursor);
        
        streaming.send_headers(200, "application/json").unwrap();
        streaming.send_chunk(b"chunk1").unwrap();
        streaming.send_chunk(b"chunk2").unwrap();
        streaming.end().unwrap();
        
        let response_str = String::from_utf8(buffer).unwrap();
        assert!(response_str.contains("Transfer-Encoding: chunked"));
        assert!(response_str.contains("chunk1"));
        assert!(response_str.contains("chunk2"));
        assert!(response_str.ends_with("0\r\n\r\n"));
    }
}