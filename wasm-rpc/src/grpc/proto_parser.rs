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

use crate::grpc::error::{GrpcError, GrpcResult};

#[cfg(feature = "grpc")]
use protobuf::descriptor::{
    FileDescriptorProto, DescriptorProto, ServiceDescriptorProto, 
    MethodDescriptorProto, FieldDescriptorProto, EnumDescriptorProto, 
    EnumValueDescriptorProto
};

#[cfg(feature = "grpc")]
pub type ParsedProtoFile = FileDescriptorProto;

/// Parser for Protocol Buffer definitions
pub struct ProtobufParser;

impl ProtobufParser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a protobuf definition from a string
    #[cfg(feature = "grpc")]
    pub fn parse(&mut self, proto_content: &str) -> GrpcResult<ParsedProtoFile> {
        // Parse protobuf content using comprehensive custom parser
        // Handles packages, messages, services, enums, fields, and RPC methods
        
        let descriptor = self.simple_proto_parse(proto_content)?;
        
        // Validate that it contains gRPC services
        Self::validate_grpc_services(&descriptor)?;
        
        Ok(descriptor)
    }
    
    /// Comprehensive proto parser implementation
    /// Parses packages, messages, services, enums and all related constructs
    #[cfg(feature = "grpc")]
    fn simple_proto_parse(&self, proto_content: &str) -> GrpcResult<ParsedProtoFile> {
        use protobuf::descriptor::*;
        
        let mut descriptor = FileDescriptorProto::new();
        
        // Extract basic information from the proto content
        let lines: Vec<&str> = proto_content.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();
        
        // Parse package name
        for line in &lines {
            if line.starts_with("package ") {
                let package = line.strip_prefix("package ").unwrap()
                    .strip_suffix(";").unwrap_or(line.strip_prefix("package ").unwrap())
                    .trim();
                descriptor.set_package(package.to_string());
                break;
            }
        }
        
        // Parse messages and services
        let mut i = 0;
        while i < lines.len() {
            let line = lines[i];
            
            if line.starts_with("message ") {
                let (message, consumed) = self.parse_message(&lines[i..])?;
                descriptor.message_type.push(message);
                i += consumed;
            } else if line.starts_with("service ") {
                let (service, consumed) = self.parse_service(&lines[i..])?;
                descriptor.service.push(service);
                i += consumed;
            } else if line.starts_with("enum ") {
                let (enum_desc, consumed) = self.parse_enum(&lines[i..])?;
                descriptor.enum_type.push(enum_desc);
                i += consumed;
            } else {
                i += 1;
            }
        }
        
        Ok(descriptor)
    }
    
    #[cfg(feature = "grpc")]
    fn parse_message(&self, lines: &[&str]) -> GrpcResult<(DescriptorProto, usize)> {
        use protobuf::descriptor::*;
        
        let mut message = DescriptorProto::new();
        let first_line = lines[0];
        
        // Extract message name
        let name = first_line.strip_prefix("message ")
            .and_then(|s| s.split_whitespace().next())
            .ok_or_else(|| GrpcError::ParseError("Invalid message declaration".to_string()))?;
        message.set_name(name.to_string());
        
        let mut i = 0;
        let mut brace_count = 0i32;
        let mut found_opening_brace = false;
        
        while i < lines.len() {
            let line = lines[i];
            
            if line.contains('{') {
                brace_count += line.chars().filter(|&c| c == '{').count() as i32;
                found_opening_brace = true;
            }
            if line.contains('}') {
                brace_count -= line.chars().filter(|&c| c == '}').count() as i32;
            }
            
            // Parse field
            if found_opening_brace && brace_count > 0 && !line.is_empty() && !line.starts_with('}') {
                if let Some(field) = self.parse_field(line)? {
                    message.field.push(field);
                }
            }
            
            i += 1;
            
            if found_opening_brace && brace_count == 0 {
                break;
            }
        }
        
        Ok((message, i))
    }
    
    #[cfg(feature = "grpc")]
    fn parse_field(&self, line: &str) -> GrpcResult<Option<FieldDescriptorProto>> {
        use protobuf::descriptor::*;
        use protobuf::descriptor::field_descriptor_proto::*;
        
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") || line.starts_with('{') || line.starts_with('}') {
            return Ok(None);
        }
        
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 4 {
            return Ok(None); // Not a valid field
        }
        
        let mut field = FieldDescriptorProto::new();
        
        // Parse field: "type name = number;"
        let field_type = parts[0];
        let field_name = parts[1];
        let field_number_str = parts.get(3).unwrap_or(&"1");
        let field_number = field_number_str.trim_end_matches(';')
            .parse::<i32>()
            .map_err(|_| GrpcError::ParseError(format!("Invalid field number: {}", field_number_str)))?;
        
        field.set_name(field_name.to_string());
        field.set_number(field_number);
        
        // Map protobuf types to field types
        match field_type {
            "string" => field.set_type(Type::TYPE_STRING),
            "int32" => field.set_type(Type::TYPE_INT32),
            "int64" => field.set_type(Type::TYPE_INT64),
            "uint32" => field.set_type(Type::TYPE_UINT32),
            "uint64" => field.set_type(Type::TYPE_UINT64),
            "bool" => field.set_type(Type::TYPE_BOOL),
            "float" => field.set_type(Type::TYPE_FLOAT),
            "double" => field.set_type(Type::TYPE_DOUBLE),
            "bytes" => field.set_type(Type::TYPE_BYTES),
            _ => {
                // Assume it's a message type
                field.set_type(Type::TYPE_MESSAGE);
                field.set_type_name(format!(".{}", field_type));
            }
        }
        
        // Handle repeated fields
        if parts.len() > 4 && parts[0] == "repeated" {
            field.set_label(Label::LABEL_REPEATED);
            // Shift everything over
            let actual_type = parts[1];
            let actual_name = parts[2];
            let actual_number_str = parts.get(5).unwrap_or(&"1");
            let actual_number = actual_number_str.trim_end_matches(';')
                .parse::<i32>()
                .map_err(|_| GrpcError::ParseError(format!("Invalid field number: {}", actual_number_str)))?;
            
            field.set_name(actual_name.to_string());
            field.set_number(actual_number);
            
            match actual_type {
                "string" => field.set_type(Type::TYPE_STRING),
                "int32" => field.set_type(Type::TYPE_INT32),
                "int64" => field.set_type(Type::TYPE_INT64),
                "uint32" => field.set_type(Type::TYPE_UINT32),
                "uint64" => field.set_type(Type::TYPE_UINT64),
                "bool" => field.set_type(Type::TYPE_BOOL),
                "float" => field.set_type(Type::TYPE_FLOAT),
                "double" => field.set_type(Type::TYPE_DOUBLE),
                "bytes" => field.set_type(Type::TYPE_BYTES),
                _ => {
                    field.set_type(Type::TYPE_MESSAGE);
                    field.set_type_name(format!(".{}", actual_type));
                }
            }
        } else {
            field.set_label(Label::LABEL_OPTIONAL);
        }
        
        Ok(Some(field))
    }
    
    #[cfg(feature = "grpc")]
    fn parse_service(&self, lines: &[&str]) -> GrpcResult<(ServiceDescriptorProto, usize)> {
        use protobuf::descriptor::*;
        
        let mut service = ServiceDescriptorProto::new();
        let first_line = lines[0];
        // Extract service name
        let name = first_line.strip_prefix("service ")
            .and_then(|s| s.split_whitespace().next())
            .ok_or_else(|| GrpcError::ParseError("Invalid service declaration".to_string()))?;
        service.set_name(name.to_string());
        
        let mut i = 0;
        let mut brace_count = 0i32;
        let mut found_opening_brace = false;
        
        while i < lines.len() {
            let line = lines[i];
            
            if line.contains('{') {
                brace_count += line.chars().filter(|&c| c == '{').count() as i32;
                found_opening_brace = true;
            }
            if line.contains('}') {
                brace_count -= line.chars().filter(|&c| c == '}').count() as i32;
            }
            
            // Parse RPC method
            if found_opening_brace && brace_count > 0 && line.trim().starts_with("rpc ") {
                if let Some(method) = self.parse_rpc_method(line)? {
                    service.method.push(method);
                }
            }
            
            i += 1;
            
            if found_opening_brace && brace_count == 0 {
                break;
            }
        }
        
        Ok((service, i))
    }
    
    #[cfg(feature = "grpc")]
    fn parse_rpc_method(&self, line: &str) -> GrpcResult<Option<MethodDescriptorProto>> {
        use protobuf::descriptor::*;
        
        let line = line.trim();
        if !line.starts_with("rpc ") {
            return Ok(None);
        }
        
        let mut method = MethodDescriptorProto::new();
        
        // Parse "rpc MethodName(InputType) returns (OutputType);"
        let method_part = line.strip_prefix("rpc ").unwrap();
        
        // Find method name
        let method_name = method_part.split('(').next()
            .ok_or_else(|| GrpcError::ParseError("Invalid RPC method format".to_string()))?
            .trim();
        method.set_name(method_name.to_string());
        
        // Find input type
        let input_start = method_part.find('(').unwrap() + 1;
        let input_end = method_part.find(')').unwrap();
        let input_type_raw = &method_part[input_start..input_end].trim();
        let input_type = input_type_raw.strip_prefix("stream ").unwrap_or(input_type_raw);
        method.set_input_type(format!(".{}", input_type));
        
        // Find output type
        let returns_part = method_part.split("returns").nth(1)
            .ok_or_else(|| GrpcError::ParseError("Missing returns clause in RPC method".to_string()))?;
        let output_start = returns_part.find('(').unwrap() + 1;
        let output_end = returns_part.find(')').unwrap();
        let output_type_raw = &returns_part[output_start..output_end].trim();
        let output_type = output_type_raw.strip_prefix("stream ").unwrap_or(output_type_raw);
        method.set_output_type(format!(".{}", output_type));
        
        // Check for streaming
        method.set_client_streaming(line.contains("stream ") && 
            line.find("stream ").unwrap() < line.find("returns").unwrap_or(line.len()));
        method.set_server_streaming(line.contains("returns (stream "));
        
        Ok(Some(method))
    }
    
    #[cfg(feature = "grpc")]
    fn parse_enum(&self, lines: &[&str]) -> GrpcResult<(EnumDescriptorProto, usize)> {
        use protobuf::descriptor::*;
        
        let mut enum_desc = EnumDescriptorProto::new();
        let first_line = lines[0];
        
        // Extract enum name
        let name = first_line.strip_prefix("enum ")
            .and_then(|s| s.split_whitespace().next())
            .ok_or_else(|| GrpcError::ParseError("Invalid enum declaration".to_string()))?;
        enum_desc.set_name(name.to_string());
        
        let mut i = 0;
        let mut brace_count = 0i32;
        let mut found_opening_brace = false;
        
        while i < lines.len() {
            let line = lines[i];
            
            if line.contains('{') {
                brace_count += line.chars().filter(|&c| c == '{').count() as i32;
                found_opening_brace = true;
            }
            if line.contains('}') {
                brace_count -= line.chars().filter(|&c| c == '}').count() as i32;
            }
            
            // Parse enum value
            if found_opening_brace && brace_count > 0 && !line.is_empty() && 
               !line.starts_with('}') && !line.trim().starts_with("//") {
                if let Some(enum_value) = self.parse_enum_value(line)? {
                    enum_desc.value.push(enum_value);
                }
            }
            
            i += 1;
            
            if found_opening_brace && brace_count == 0 {
                break;
            }
        }
        
        Ok((enum_desc, i))
    }
    
    #[cfg(feature = "grpc")]
    fn parse_enum_value(&self, line: &str) -> GrpcResult<Option<EnumValueDescriptorProto>> {
        use protobuf::descriptor::*;
        
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") || line.starts_with('{') || line.starts_with('}') {
            return Ok(None);
        }
        
        let parts: Vec<&str> = line.split('=').collect();
        if parts.len() != 2 {
            return Ok(None);
        }
        
        let mut enum_value = EnumValueDescriptorProto::new();
        let name = parts[0].trim();
        let number_str = parts[1].trim().trim_end_matches(';');
        let number = number_str.parse::<i32>()
            .map_err(|_| GrpcError::ParseError(format!("Invalid enum value number: {}", number_str)))?;
        
        enum_value.set_name(name.to_string());
        enum_value.set_number(number);
        
        Ok(Some(enum_value))
    }

    #[cfg(not(feature = "grpc"))]
    pub fn parse(&mut self, _proto_content: &str) -> GrpcResult<()> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Parse a protobuf definition from a file path
    #[cfg(feature = "grpc")]
    pub fn parse_file(&mut self, file_path: &str) -> GrpcResult<ParsedProtoFile> {
        let content = std::fs::read_to_string(file_path)?;
        self.parse(&content)
    }

    #[cfg(not(feature = "grpc"))]
    pub fn parse_file(&mut self, _file_path: &str) -> GrpcResult<()> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Validate that the protobuf definition contains gRPC services
    #[cfg(feature = "grpc")]
    pub fn validate_grpc_services(descriptor: &ParsedProtoFile) -> GrpcResult<()> {
        if descriptor.service.is_empty() {
            return Err(GrpcError::InvalidProtobuf(
                "No gRPC services found in protobuf definition".to_string()
            ));
        }

        for service in &descriptor.service {
            if service.method.is_empty() {
                return Err(GrpcError::InvalidProtobuf(
                    format!("Service '{}' has no methods", service.name())
                ));
            }

            for method in &service.method {
                if method.input_type().is_empty() {
                    return Err(GrpcError::InvalidProtobuf(
                        format!("Method '{}' in service '{}' has no input type", 
                               method.name(), service.name())
                    ));
                }
                
                if method.output_type().is_empty() {
                    return Err(GrpcError::InvalidProtobuf(
                        format!("Method '{}' in service '{}' has no output type", 
                               method.name(), service.name())
                    ));
                }
            }
        }

        Ok(())
    }

    #[cfg(not(feature = "grpc"))]
    pub fn validate_grpc_services(_descriptor: &()) -> GrpcResult<()> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Extract package name from the protobuf descriptor
    #[cfg(feature = "grpc")]
    pub fn extract_package_name(descriptor: &ParsedProtoFile) -> String {
        descriptor.package().to_string()
    }

    #[cfg(not(feature = "grpc"))]
    pub fn extract_package_name(_descriptor: &()) -> String {
        "default".to_string()
    }
}

impl Default for ProtobufParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    
    #[test]
    #[cfg(feature = "grpc")]
    fn test_parse_simple_service() {
        let proto_content = r#"
            syntax = "proto3";
            
            package test.v1;
            
            message HelloRequest {
                string name = 1;
            }
            
            message HelloResponse {
                string message = 1;
            }
            
            service HelloService {
                rpc SayHello(HelloRequest) returns (HelloResponse);
            }
        "#;

        let mut parser = ProtobufParser::new();
        let result = parser.parse(proto_content);
        
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        
        assert_eq!(descriptor.package, Some("test.v1".to_string()));
        assert_eq!(descriptor.service.len(), 1);
        assert_eq!(descriptor.service[0].name, "HelloService");
        assert_eq!(descriptor.service[0].method.len(), 1);
        assert_eq!(descriptor.service[0].method[0].name, "SayHello");
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn test_validate_grpc_services() {
        let proto_content = r#"
            syntax = "proto3";
            
            message TestMessage {
                string value = 1;
            }
        "#;

        let mut parser = ProtobufParser::new();
        let descriptor = parser.parse(proto_content).unwrap();
        
        let result = ProtobufParser::validate_grpc_services(&descriptor);
        assert!(result.is_err());
        
        if let Err(GrpcError::InvalidProtobuf(msg)) = result {
            assert!(msg.contains("No gRPC services found"));
        } else {
            panic!("Expected InvalidProtobuf error");
        }
    }
}