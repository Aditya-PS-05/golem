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

use crate::grpc::{
    error::{GrpcError, GrpcResult, GrpcStatusCode},
    type_mapper::TypeMapper,
    name_resolver::NameResolver,
};
use std::collections::HashSet;

#[cfg(feature = "grpc")]
use protobuf::descriptor::FileDescriptorProto;

/// Generates WIT packages from protobuf descriptors
pub struct WitGenerator {
    /// Tracks generated interfaces to avoid duplicates
    generated_interfaces: HashSet<String>,
}

impl WitGenerator {
    pub fn new() -> Self {
        Self {
            generated_interfaces: HashSet::new(),
        }
    }

    /// Generate a complete WIT package from a protobuf file descriptor
    #[cfg(feature = "grpc")]
    pub fn generate_wit_package(
        &mut self,
        descriptor: &FileDescriptorProto,
        package_name: &str,
        version: &str,
        type_mapper: &mut TypeMapper,
        name_resolver: &NameResolver,
    ) -> GrpcResult<String> {
        let mut sections = Vec::new();

        // Generate package declaration
        let wit_package_name = name_resolver.generate_package_name(package_name, version);
        sections.push(format!("package {};", wit_package_name));
        sections.push(String::new()); // Empty line

        // Generate types interface (messages and enums)
        if !descriptor.message_type.is_empty() || !descriptor.enum_type.is_empty() {
            let types_interface = self.generate_types_interface(descriptor, type_mapper, name_resolver)?;
            sections.push(types_interface);
            sections.push(String::new()); // Empty line
        }

        // Generate service interfaces
        for service in &descriptor.service {
            let service_interface = self.generate_service_interface(service, type_mapper, name_resolver)?;
            sections.push(service_interface);
            sections.push(String::new()); // Empty line
        }

        // Generate world that exports the services
        if !descriptor.service.is_empty() {
            let world = self.generate_world(descriptor, name_resolver)?;
            sections.push(world);
        }

        Ok(sections.join("\n"))
    }

    #[cfg(not(feature = "grpc"))]
    pub fn generate_wit_package(
        &mut self,
        _descriptor: &(),
        _package_name: &str,
        _version: &str,
        _type_mapper: &mut TypeMapper,
        _name_resolver: &NameResolver,
    ) -> GrpcResult<String> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Generate the types interface containing all message and enum definitions
    #[cfg(feature = "grpc")]
    fn generate_types_interface(
        &mut self,
        descriptor: &FileDescriptorProto,
        type_mapper: &mut TypeMapper,
        name_resolver: &NameResolver,
    ) -> GrpcResult<String> {
        let mut definitions = Vec::new();

        // Generate enum definitions first (they might be used by messages)
        for enum_desc in &descriptor.enum_type {
            let enum_def = type_mapper.generate_wit_enum(enum_desc, name_resolver)?;
            definitions.push(format!("  {}", enum_def.replace('\n', "\n  ")));
        }

        // Generate message definitions
        for message in &descriptor.message_type {
            // Handle nested enums
            for nested_enum in &message.enum_type {
                let enum_def = type_mapper.generate_wit_enum(nested_enum, name_resolver)?;
                definitions.push(format!("  {}", enum_def.replace('\n', "\n  ")));
            }

            // Handle oneof fields as variants
            for oneof in &message.oneof_decl {
                let oneof_fields: Vec<_> = message.field.iter()
                    .filter(|f| f.oneof_index.is_some() && 
                             f.oneof_index.unwrap() as usize == 
                             message.oneof_decl.iter().position(|o| o == oneof).unwrap())
                    .collect();
                
                if !oneof_fields.is_empty() {
                    let variant_def = type_mapper.generate_wit_variant_for_oneof(
                        oneof.name(), 
                        &oneof_fields, 
                        name_resolver
                    )?;
                    definitions.push(format!("  {}", variant_def.replace('\n', "\n  ")));
                }
            }

            // Generate the main message record
            let record_def = type_mapper.generate_wit_record(message, name_resolver)?;
            definitions.push(format!("  {}", record_def.replace('\n', "\n  ")));
        }

        // Generate the standard gRPC error type
        let grpc_error = GrpcStatusCode::generate_wit_error_variant();
        definitions.push(format!("  {}", grpc_error.replace('\n', "\n  ")));

        let interface = format!(
            "interface types {{\n{}\n}}",
            definitions.join("\n\n")
        );

        Ok(interface)
    }

    /// Generate a service interface from a protobuf service definition
    #[cfg(feature = "grpc")]
    fn generate_service_interface(
        &mut self,
        service: &protobuf::descriptor::ServiceDescriptorProto,
        _type_mapper: &TypeMapper,
        name_resolver: &NameResolver,
    ) -> GrpcResult<String> {
        let interface_name = name_resolver.proto_to_wit_interface_name(service.name());
        
        if self.generated_interfaces.contains(&interface_name) {
            return Err(GrpcError::NameResolutionError {
                name: interface_name,
                message: "Interface name already generated".to_string(),
            });
        }

        let mut functions = Vec::new();
        let mut use_statements = Vec::new();

        // Collect all types used in this service
        let mut used_types = HashSet::new();
        for method in &service.method {
            used_types.insert(name_resolver.proto_to_wit_type_name(method.input_type()));
            used_types.insert(name_resolver.proto_to_wit_type_name(method.output_type()));
        }

        // Generate use statement for types
        if !used_types.is_empty() {
            let mut type_list: Vec<_> = used_types.into_iter().collect();
            type_list.sort();
            type_list.push("grpc-error".to_string()); // Always include error type
            
            use_statements.push(format!("  use types.{{{}}};", type_list.join(", ")));
        }

        // Generate function definitions for each method
        for method in &service.method {
            let function = self.generate_grpc_method_function(method, name_resolver)?;
            functions.push(format!("  {}", function));
        }

        let mut interface_parts = Vec::new();
        interface_parts.push(format!("interface {} {{", interface_name));
        
        if !use_statements.is_empty() {
            interface_parts.extend(use_statements);
            interface_parts.push(String::new()); // Empty line
        }
        
        interface_parts.extend(functions);
        interface_parts.push("}".to_string());

        self.generated_interfaces.insert(interface_name);
        
        Ok(interface_parts.join("\n"))
    }

    /// Generate a WIT function definition for a gRPC method
    #[cfg(feature = "grpc")]
    fn generate_grpc_method_function(
        &self,
        method: &protobuf::descriptor::MethodDescriptorProto,
        name_resolver: &NameResolver,
    ) -> GrpcResult<String> {
        let function_name = name_resolver.proto_to_wit_method_name(method.name());
        let input_type = name_resolver.proto_to_wit_type_name(method.input_type());
        let output_type = name_resolver.proto_to_wit_type_name(method.output_type());

        let client_streaming = method.client_streaming();
        let server_streaming = method.server_streaming();

        let function_def = match (client_streaming, server_streaming) {
            (false, false) => {
                // Unary RPC
                format!(
                    "{}: func(request: {}) -> result<{}, grpc-error>;",
                    function_name, input_type, output_type
                )
            }
            (true, false) => {
                // Client streaming RPC
                format!(
                    "{}: func(requests: list<{}>) -> result<{}, grpc-error>;",
                    function_name, input_type, output_type
                )
            }
            (false, true) => {
                // Server streaming RPC
                format!(
                    "{}: func(request: {}) -> result<list<{}>, grpc-error>;",
                    function_name, input_type, output_type
                )
            }
            (true, true) => {
                // Bidirectional streaming RPC
                format!(
                    "{}: func(requests: list<{}>) -> result<list<{}>, grpc-error>;",
                    function_name, input_type, output_type
                )
            }
        };

        Ok(function_def)
    }

    /// Generate a world that exports all the service interfaces
    #[cfg(feature = "grpc")]
    fn generate_world(
        &self,
        descriptor: &FileDescriptorProto,
        name_resolver: &NameResolver,
    ) -> GrpcResult<String> {
        let world_name = if descriptor.service.len() == 1 {
            format!("{}-api", name_resolver.proto_to_wit_interface_name(descriptor.service[0].name()))
        } else {
            "grpc-api".to_string()
        };

        let mut exports = Vec::new();
        
        for service in &descriptor.service {
            let interface_name = name_resolver.proto_to_wit_interface_name(service.name());
            exports.push(format!("  export {};", interface_name));
        }

        let world = format!(
            "world {} {{\n{}\n}}",
            world_name,
            exports.join("\n")
        );

        Ok(world)
    }

    /// Clear the generated interfaces cache (useful for testing)
    pub fn clear_cache(&mut self) {
        self.generated_interfaces.clear();
    }
}

impl Default for WitGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::{ProtobufParser, TypeMapper, NameResolver};

    #[test]
    #[cfg(feature = "grpc")]
    fn test_generate_simple_service() {
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
        let descriptor = parser.parse(proto_content).unwrap();
        
        let mut generator = WitGenerator::new();
        let mut type_mapper = TypeMapper::new();
        let name_resolver = NameResolver::new();
        
        let wit_package = generator.generate_wit_package(
            &descriptor,
            "test.v1",
            "1.0.0",
            &mut type_mapper,
            &name_resolver,
        ).unwrap();

        assert!(wit_package.contains("package test:v1@1.0.0;"));
        assert!(wit_package.contains("interface types"));
        assert!(wit_package.contains("record hello-request"));
        assert!(wit_package.contains("record hello-response"));
        assert!(wit_package.contains("interface hello-service"));
        assert!(wit_package.contains("say-hello: func"));
        assert!(wit_package.contains("result<hello-response, grpc-error>"));
        assert!(wit_package.contains("world hello-service-api"));
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn test_streaming_methods() {
        let proto_content = r#"
            syntax = "proto3";
            
            package stream.v1;
            
            message StreamRequest {
                string data = 1;
            }
            
            message StreamResponse {
                string result = 1;
            }
            
            service StreamService {
                rpc ClientStreaming(stream StreamRequest) returns (StreamResponse);
                rpc ServerStreaming(StreamRequest) returns (stream StreamResponse);
                rpc BidirectionalStreaming(stream StreamRequest) returns (stream StreamResponse);
            }
        "#;

        let mut parser = ProtobufParser::new();
        let descriptor = parser.parse(proto_content).unwrap();
        
        let mut generator = WitGenerator::new();
        let mut type_mapper = TypeMapper::new();
        let name_resolver = NameResolver::new();
        
        let wit_package = generator.generate_wit_package(
            &descriptor,
            "stream.v1",
            "1.0.0",
            &mut type_mapper,
            &name_resolver,
        ).unwrap();

        // Check that streaming methods are properly generated
        assert!(wit_package.contains("client-streaming: func(requests: list<stream-request>)"));
        assert!(wit_package.contains("server-streaming: func(request: stream-request) -> result<list<stream-response>"));
        assert!(wit_package.contains("bidirectional-streaming: func(requests: list<stream-request>) -> result<list<stream-response>"));
    }
}