// Copyright 2024-2025 Golem Cloud
//
// Licensed under the Golem Source License v1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://license.golem.cloud/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

/// Result type for gRPC operations
pub type GrpcResult<T> = Result<T, GrpcError>;

/// Errors that can occur during gRPC integration
#[derive(Debug, Clone)]
pub enum GrpcError {
    /// Error parsing protobuf definition
    ParseError(String),
    
    /// Error generating WIT from protobuf
    WitGenerationError(String),
    
    /// Type mapping error
    TypeMappingError { 
        proto_type: String, 
        message: String 
    },
    
    /// Name resolution error (conflicts, invalid names, etc.)
    NameResolutionError {
        name: String,
        message: String,
    },
    
    /// Invalid protobuf structure
    InvalidProtobuf(String),
    
    /// Unsupported protobuf feature
    UnsupportedFeature(String),
    
    /// IO error during file operations
    IoError(String),
    
    /// Invalid configuration
    InvalidConfiguration(String),
    
    /// Generic error
    Other(String),
}

impl fmt::Display for GrpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrpcError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            GrpcError::WitGenerationError(msg) => write!(f, "WIT generation error: {}", msg),
            GrpcError::TypeMappingError { proto_type, message } => {
                write!(f, "Type mapping error for '{}': {}", proto_type, message)
            }
            GrpcError::NameResolutionError { name, message } => {
                write!(f, "Name resolution error for '{}': {}", name, message)
            }
            GrpcError::InvalidProtobuf(msg) => write!(f, "Invalid protobuf: {}", msg),
            GrpcError::UnsupportedFeature(feature) => {
                write!(f, "Unsupported protobuf feature: {}", feature)
            }
            GrpcError::IoError(msg) => write!(f, "IO error: {}", msg),
            GrpcError::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {}", msg),
            GrpcError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for GrpcError {}

impl From<std::io::Error> for GrpcError {
    fn from(error: std::io::Error) -> Self {
        GrpcError::IoError(error.to_string())
    }
}

// Note: protobuf_parse error conversion will be added when we determine the correct error type

/// Standard gRPC error codes that map to WIT error variants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcStatusCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl GrpcStatusCode {
    /// Convert to WIT-idiomatic kebab-case name
    pub fn to_wit_variant_name(&self) -> &'static str {
        match self {
            GrpcStatusCode::Ok => "ok",
            GrpcStatusCode::Cancelled => "cancelled",
            GrpcStatusCode::Unknown => "unknown",
            GrpcStatusCode::InvalidArgument => "invalid-argument",
            GrpcStatusCode::DeadlineExceeded => "deadline-exceeded",
            GrpcStatusCode::NotFound => "not-found",
            GrpcStatusCode::AlreadyExists => "already-exists",
            GrpcStatusCode::PermissionDenied => "permission-denied",
            GrpcStatusCode::ResourceExhausted => "resource-exhausted",
            GrpcStatusCode::FailedPrecondition => "failed-precondition",
            GrpcStatusCode::Aborted => "aborted",
            GrpcStatusCode::OutOfRange => "out-of-range",
            GrpcStatusCode::Unimplemented => "unimplemented",
            GrpcStatusCode::Internal => "internal",
            GrpcStatusCode::Unavailable => "unavailable",
            GrpcStatusCode::DataLoss => "data-loss",
            GrpcStatusCode::Unauthenticated => "unauthenticated",
        }
    }

    /// Generate the standard gRPC error variant definition for WIT
    pub fn generate_wit_error_variant() -> String {
        let mut variants = Vec::new();
        
        for code in [
            GrpcStatusCode::Cancelled,
            GrpcStatusCode::Unknown,
            GrpcStatusCode::InvalidArgument,
            GrpcStatusCode::DeadlineExceeded,
            GrpcStatusCode::NotFound,
            GrpcStatusCode::AlreadyExists,
            GrpcStatusCode::PermissionDenied,
            GrpcStatusCode::ResourceExhausted,
            GrpcStatusCode::FailedPrecondition,
            GrpcStatusCode::Aborted,
            GrpcStatusCode::OutOfRange,
            GrpcStatusCode::Unimplemented,
            GrpcStatusCode::Internal,
            GrpcStatusCode::Unavailable,
            GrpcStatusCode::DataLoss,
            GrpcStatusCode::Unauthenticated,
        ] {
            match code {
                GrpcStatusCode::InvalidArgument 
                | GrpcStatusCode::FailedPrecondition 
                | GrpcStatusCode::OutOfRange 
                | GrpcStatusCode::Internal => {
                    variants.push(format!("  {}(string),", code.to_wit_variant_name()));
                }
                _ => {
                    variants.push(format!("  {},", code.to_wit_variant_name()));
                }
            }
        }
        
        format!(
            "variant grpc-error {{\n{}\n}}",
            variants.join("\n")
        )
    }
}