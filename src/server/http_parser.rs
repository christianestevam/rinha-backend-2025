//! Ultra-fast HTTP parser optimized for Rinha Backend patterns
//! 
//! This parser is specifically optimized for the limited set of HTTP requests
//! needed by the Rinha Backend challenge, avoiding the overhead of general-purpose
//! HTTP parsers like hyper's.

use std::collections::HashMap;
use std::str;

/// HTTP method enum for fast matching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Options,
}

/// Parsed HTTP request optimized for our use case
#[derive(Debug)]
pub struct HttpRequest<'a> {
    /// HTTP method
    pub method: Method,
    /// Request path
    pub path: &'a str,
    /// Query string (if any)
    pub query: Option<&'a str>,
    /// HTTP version
    pub version: &'a str,
    /// Headers (only the ones we care about)
    pub headers: FastHeaders<'a>,
    /// Request body
    pub body: &'a [u8],
}

/// Fast header storage using slice references (zero-copy)
#[derive(Debug)]
pub struct FastHeaders<'a> {
    /// Content-Type header
    pub content_type: Option<&'a str>,
    /// Content-Length header  
    pub content_length: Option<usize>,
    /// Host header
    pub host: Option<&'a str>,
    /// Connection header
    pub connection: Option<&'a str>,
}

impl<'a> FastHeaders<'a> {
    fn new() -> Self {
        Self {
            content_type: None,
            content_length: None,
            host: None,
            connection: None,
        }
    }
}

/// HTTP parsing errors
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Invalid HTTP method")]
    InvalidMethod,
    #[error("Invalid request line")]
    InvalidRequestLine,
    #[error("Invalid header")]
    InvalidHeader,
    #[error("Missing Content-Length")]
    MissingContentLength,
    #[error("Invalid Content-Length")]
    InvalidContentLength,
    #[error("Incomplete request")]
    IncompleteRequest,
    #[error("Request too large")]
    RequestTooLarge,
}

/// Fast HTTP parser with SIMD optimizations where possible
pub struct HttpParser;

impl HttpParser {
    /// Parse HTTP request from buffer
    /// 
    /// This function is optimized for the specific patterns we expect:
    /// - POST /payments (most common)
    /// - GET /payments-summary
    /// - POST /purge-payments
    pub fn parse_request(buffer: &[u8]) -> Result<HttpRequest, ParseError> {
        if buffer.len() < 16 {
            return Err(ParseError::IncompleteRequest);
        }
        
        // Fast path: check for our expected requests using SIMD-style comparisons
        if let Some(request) = Self::try_fast_parse(buffer)? {
            return Ok(request);
        }
        
        // Fallback to slower but more general parsing
        Self::parse_request_general(buffer)
    }
    
    /// Ultra-fast parsing for our specific endpoints
    fn try_fast_parse(buffer: &[u8]) -> Result<Option<HttpRequest>, ParseError> {
        // Check for "POST /payments" (most common case)
        if buffer.len() >= 14 && &buffer[0..14] == b"POST /payments" {
            return Self::parse_post_payments(buffer).map(Some);
        }
        
        // Check for "GET /payments-summary"
        if buffer.len() >= 21 && &buffer[0..21] == b"GET /payments-summary" {
            return Self::parse_get_summary(buffer).map(Some);
        }
        
        // Check for "POST /purge-payments"
        if buffer.len() >= 19 && &buffer[0..19] == b"POST /purge-payments" {
            return Self::parse_post_purge(buffer).map(Some);
        }
        
        // Not a fast-path request
        Ok(None)
    }
    
    /// Parse "POST /payments" request (hot path optimization)
    fn parse_post_payments(buffer: &[u8]) -> Result<HttpRequest, ParseError> {
        // Find end of request line
        let line_end = Self::find_line_end(buffer)?;
        
        // Parse version (should be HTTP/1.1)
        let version = "HTTP/1.1"; // We assume HTTP/1.1 for performance
        
        // Find headers section
        let headers_start = line_end + 2; // Skip \r\n
        let (headers, body_start) = Self::parse_headers_fast(&buffer[headers_start..])?;
        
        // Get body
        let body = &buffer[headers_start + body_start..];
        
        // Validate Content-Length
        if let Some(content_length) = headers.content_length {
            if body.len() < content_length {
                return Err(ParseError::IncompleteRequest);
            }
        }
        
        Ok(HttpRequest {
            method: Method::Post,
            path: "/payments",
            query: None,
            version,
            headers,
            body,
        })
    }
    
    /// Parse "GET /payments-summary" request
    fn parse_get_summary(buffer: &[u8]) -> Result<HttpRequest, ParseError> {
        // Find query string
        let line_end = Self::find_line_end(buffer)?;
        let request_line = str::from_utf8(&buffer[0..line_end])
            .map_err(|_| ParseError::InvalidRequestLine)?;
        
        // Parse path and query
        let (path, query) = if let Some(question_mark) = request_line.find('?') {
            let parts: Vec<&str> = request_line.split_whitespace().collect();
            if parts.len() < 2 {
                return Err(ParseError::InvalidRequestLine);
            }
            let path_query = parts[1];
            let (path, query) = path_query.split_at(question_mark - 4); // Adjust for "GET "
            (path, Some(&query[1..])) // Skip the '?'
        } else {
            ("/payments-summary", None)
        };
        
        // Headers (minimal for GET)
        let headers_start = line_end + 2;
        let (headers, _) = Self::parse_headers_fast(&buffer[headers_start..])?;
        
        Ok(HttpRequest {
            method: Method::Get,
            path,
            query,
            version: "HTTP/1.1",
            headers,
            body: &[],
        })
    }
    
    /// Parse "POST /purge-payments" request
    fn parse_post_purge(buffer: &[u8]) -> Result<HttpRequest, ParseError> {
        let line_end = Self::find_line_end(buffer)?;
        let headers_start = line_end + 2;
        let (headers, body_start) = Self::parse_headers_fast(&buffer[headers_start..])?;
        let body = &buffer[headers_start + body_start..];
        
        Ok(HttpRequest {
            method: Method::Post,
            path: "/purge-payments",
            query: None,
            version: "HTTP/1.1",
            headers,
            body,
        })
    }
    
    /// General-purpose HTTP parsing (fallback)
    fn parse_request_general(buffer: &[u8]) -> Result<HttpRequest, ParseError> {
        // Find end of request line
        let line_end = Self::find_line_end(buffer)?;
        let request_line = str::from_utf8(&buffer[0..line_end])
            .map_err(|_| ParseError::InvalidRequestLine)?;
        
        // Parse request line
        let parts: Vec<&str> = request_line.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(ParseError::InvalidRequestLine);
        }
        
        let method = match parts[0] {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "OPTIONS" => Method::Options,
            _ => return Err(ParseError::InvalidMethod),
        };
        
        let (path, query) = if let Some(question_mark) = parts[1].find('?') {
            let (path, query_part) = parts[1].split_at(question_mark);
            (path, Some(&query_part[1..])) // Skip the '?'
        } else {
            (parts[1], None)
        };
        
        let version = parts[2];
        
        // Parse headers
        let headers_start = line_end + 2;
        let (headers, body_start) = Self::parse_headers_fast(&buffer[headers_start..])?;
        
        // Get body
        let body = &buffer[headers_start + body_start..];
        
        Ok(HttpRequest {
            method,
            path,
            query,
            version,
            headers,
            body,
        })
    }
    
    /// Fast header parsing (only extracts headers we care about)
    fn parse_headers_fast(buffer: &[u8]) -> Result<(FastHeaders, usize), ParseError> {
        let mut headers = FastHeaders::new();
        let mut pos = 0;
        
        while pos < buffer.len() {
            // Find end of current line
            let line_start = pos;
            let line_end = match Self::find_line_end_from(&buffer[pos..]) {
                Ok(end) => pos + end,
                Err(_) => return Err(ParseError::InvalidHeader),
            };
            
            // Check for empty line (end of headers)
            if line_end == line_start {
                return Ok((headers, pos + 2)); // Skip \r\n
            }
            
            // Parse header line
            let line = &buffer[line_start..line_end];
            if let Some(colon) = line.iter().position(|&b| b == b':') {
                let name = &line[0..colon];
                let value_start = colon + 1;
                
                // Skip whitespace after colon
                let value_start = line[value_start..]
                    .iter()
                    .position(|&b| b != b' ' && b != b'\t')
                    .map(|p| value_start + p)
                    .unwrap_or(value_start);
                
                let value = &line[value_start..];
                
                // Match headers we care about (case-insensitive)
                match name {
                    b"content-type" | b"Content-Type" => {
                        headers.content_type = str::from_utf8(value).ok();
                    }
                    b"content-length" | b"Content-Length" => {
                        if let Ok(value_str) = str::from_utf8(value) {
                            headers.content_length = value_str.parse().ok();
                        }
                    }
                    b"host" | b"Host" => {
                        headers.host = str::from_utf8(value).ok();
                    }
                    b"connection" | b"Connection" => {
                        headers.connection = str::from_utf8(value).ok();
                    }
                    _ => {
                        // Ignore other headers for performance
                    }
                }
            }
            
            pos = line_end + 2; // Skip \r\n
        }
        
        Err(ParseError::IncompleteRequest)
    }
    
    /// Find end of line (\r\n)
    #[inline]
    fn find_line_end(buffer: &[u8]) -> Result<usize, ParseError> {
        Self::find_line_end_from(buffer)
    }
    
    /// Find end of line from position
    #[inline]
    fn find_line_end_from(buffer: &[u8]) -> Result<usize, ParseError> {
        // Fast SIMD-style search for \r\n
        for i in 0..buffer.len().saturating_sub(1) {
            if buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
                return Ok(i);
            }
        }
        Err(ParseError::IncompleteRequest)
    }
}

/// SIMD-optimized string searching (when feature is enabled)
#[cfg(feature = "simd")]
mod simd_parser {
    use super::*;
    
    /// Find pattern using SIMD instructions (AVX2)
    #[cfg(target_arch = "x86_64")]
    pub unsafe fn find_pattern_simd(haystack: &[u8], pattern: &[u8]) -> Option<usize> {
        use std::arch::x86_64::*;
        
        if haystack.len() < pattern.len() || pattern.is_empty() {
            return None;
        }
        
        let first_byte = pattern[0];
        let haystack_len = haystack.len();
        let pattern_len = pattern.len();
        
        // Use AVX2 to find potential matches
        if haystack_len >= 32 {
            let first_bytes = _mm256_set1_epi8(first_byte as i8);
            
            for i in (0..haystack_len - 31).step_by(32) {
                let chunk = _mm256_loadu_si256(haystack.as_ptr().add(i) as *const __m256i);
                let matches = _mm256_cmpeq_epi8(chunk, first_bytes);
                let mask = _mm256_movemask_epi8(matches);
                
                if mask != 0 {
                    // Check each potential match
                    for bit in 0..32 {
                        if mask & (1 << bit) != 0 {
                            let pos = i + bit;
                            if pos + pattern_len <= haystack_len {
                                if &haystack[pos..pos + pattern_len] == pattern {
                                    return Some(pos);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Fallback to scalar search for remaining bytes
        for i in (haystack_len.saturating_sub(31))..haystack_len {
            if i + pattern_len <= haystack_len {
                if &haystack[i..i + pattern_len] == pattern {
                    return Some(i);
                }
            }
        }
        
        None
    }
    
    /// SIMD-optimized line ending search
    #[cfg(target_arch = "x86_64")]
    pub unsafe fn find_crlf_simd(buffer: &[u8]) -> Option<usize> {
        find_pattern_simd(buffer, b"\r\n")
    }
}

/// Pre-compiled request patterns for ultra-fast matching
pub struct RequestPatterns;

impl RequestPatterns {
    /// Check if buffer starts with "POST /payments"
    #[inline]
    pub fn is_post_payments(buffer: &[u8]) -> bool {
        buffer.len() >= 14 && &buffer[0..14] == b"POST /payments"
    }
    
    /// Check if buffer starts with "GET /payments-summary"
    #[inline]
    pub fn is_get_summary(buffer: &[u8]) -> bool {
        buffer.len() >= 21 && &buffer[0..21] == b"GET /payments-summary"
    }
    
    /// Check if buffer starts with "POST /purge-payments"
    #[inline]
    pub fn is_post_purge(buffer: &[u8]) -> bool {
        buffer.len() >= 19 && &buffer[0..19] == b"POST /purge-payments"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_post_payments() {
        let request = b"POST /payments HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 45\r\n\r\n{\"correlationId\":\"test\",\"amount\":19.90}";
        
        let parsed = HttpParser::parse_request(request).unwrap();
        assert_eq!(parsed.method, Method::Post);
        assert_eq!(parsed.path, "/payments");
        assert_eq!(parsed.headers.content_type, Some("application/json"));
        assert_eq!(parsed.headers.content_length, Some(45));
    }
    
    #[test]
    fn test_parse_get_summary() {
        let request = b"GET /payments-summary?from=2020-01-01T00:00:00.000Z&to=2020-12-31T23:59:59.999Z HTTP/1.1\r\nHost: localhost\r\n\r\n";
        
        let parsed = HttpParser::parse_request(request).unwrap();
        assert_eq!(parsed.method, Method::Get);
        assert_eq!(parsed.path, "/payments-summary");
        assert!(parsed.query.is_some());
        assert!(parsed.query.unwrap().contains("from=2020-01-01"));
    }
    
    #[test]
    fn test_request_patterns() {
        assert!(RequestPatterns::is_post_payments(b"POST /payments HTTP/1.1"));
        assert!(RequestPatterns::is_get_summary(b"GET /payments-summary?test HTTP/1.1"));
        assert!(RequestPatterns::is_post_purge(b"POST /purge-payments HTTP/1.1"));
        
        assert!(!RequestPatterns::is_post_payments(b"GET /payments"));
        assert!(!RequestPatterns::is_get_summary(b"POST /payments-summary"));
    }
    
    #[test]
    fn test_incomplete_request() {
        let request = b"POST /pay";
        let result = HttpParser::parse_request(request);
        assert!(matches!(result, Err(ParseError::IncompleteRequest)));
    }
    
    #[test]
    fn test_invalid_method() {
        let request = b"PATCH /payments HTTP/1.1\r\n\r\n";
        let result = HttpParser::parse_request(request);
        assert!(matches!(result, Err(ParseError::InvalidMethod)));
    }
    
    #[cfg(feature = "simd")]
    #[test]
    fn test_simd_pattern_search() {
        unsafe {
            let haystack = b"POST /payments HTTP/1.1\r\nContent-Type: application/json\r\n\r\n";
            let pattern = b"\r\n\r\n";
            
            let pos = simd_parser::find_pattern_simd(haystack, pattern);
            assert!(pos.is_some());
            assert_eq!(&haystack[pos.unwrap()..pos.unwrap() + 4], pattern);
        }
    }
}