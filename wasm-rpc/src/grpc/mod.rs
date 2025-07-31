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

//! gRPC integration module for Golem
//!
//! This module provides functionality to convert Protocol Buffers v3 (proto3)
//! with gRPC service definitions to idiomatic WIT (WebAssembly Interface Types).

pub mod error;
pub mod name_resolver;
pub mod proto_parser;
pub mod type_mapper;
pub mod wit_generator;

#[cfg(feature = "cli")]
pub mod cli;

pub use error::{GrpcError, GrpcResult};
pub use name_resolver::NameResolver;
pub use proto_parser::ProtobufParser;
pub use type_mapper::TypeMapper;
pub use wit_generator::WitGenerator;

#[cfg(feature = "cli")]
pub use cli::{GolemYamlConfig, GrpcAuthConfig, GrpcCliManager, GrpcDependencyConfig};

use std::collections::HashMap;

#[cfg(feature = "grpc")]
use protobuf::descriptor::{field_descriptor_proto::Label, FileDescriptorProto};

/// Main entry point for converting protobuf definitions to WIT
pub struct ProtoToWitConverter {
    parser: ProtobufParser,
    generator: WitGenerator,
    type_mapper: TypeMapper,
    name_resolver: NameResolver,
}

impl ProtoToWitConverter {
    pub fn new() -> Self {
        Self {
            parser: ProtobufParser::new(),
            generator: WitGenerator::new(),
            type_mapper: TypeMapper::new(),
            name_resolver: NameResolver::new(),
        }
    }

    /// Convert a protobuf file to WIT package
    pub fn convert_proto_to_wit(
        &mut self,
        proto_content: &str,
        package_name: &str,
        version: &str,
    ) -> GrpcResult<String> {
        // Parse the protobuf definition
        let proto_desc = self.parser.parse(proto_content)?;

        // Generate WIT from protobuf descriptor
        let wit_package = self.generator.generate_wit_package(
            &proto_desc,
            package_name,
            version,
            &mut self.type_mapper,
            &self.name_resolver,
        )?;

        Ok(wit_package)
    }

    /// Extract gRPC service metadata for runtime stub generation
    pub fn extract_service_metadata(&mut self, proto_content: &str) -> GrpcResult<ServiceMetadata> {
        let proto_desc = self.parser.parse(proto_content)?;
        Ok(ServiceMetadata::from_descriptor(&proto_desc))
    }
}

/// Metadata about gRPC services for runtime stub generation
#[derive(Debug, Clone)]
pub struct ServiceMetadata {
    pub services: HashMap<String, ServiceInfo>,
    pub types: HashMap<String, TypeInfo>,
}

#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub methods: HashMap<String, MethodInfo>,
}

#[derive(Debug, Clone)]
pub struct MethodInfo {
    pub name: String,
    pub input_type: String,
    pub output_type: String,
    pub client_streaming: bool,
    pub server_streaming: bool,
}

#[derive(Debug, Clone)]
pub struct TypeInfo {
    pub name: String,
    pub fields: Vec<FieldInfo>,
    pub is_enum: bool,
    pub enum_values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FieldInfo {
    pub name: String,
    pub field_type: String,
    pub number: i32,
    pub optional: bool,
    pub repeated: bool,
}

impl ServiceMetadata {
    #[cfg(feature = "grpc")]
    pub fn from_descriptor(descriptor: &FileDescriptorProto) -> Self {
        let mut services = HashMap::new();
        let mut types = HashMap::new();

        // Extract service information
        for service in &descriptor.service {
            let mut methods = HashMap::new();

            for method in &service.method {
                methods.insert(
                    method.name().to_string(),
                    MethodInfo {
                        name: method.name().to_string(),
                        input_type: method.input_type().to_string(),
                        output_type: method.output_type().to_string(),
                        client_streaming: method.client_streaming(),
                        server_streaming: method.server_streaming(),
                    },
                );
            }

            services.insert(
                service.name().to_string(),
                ServiceInfo {
                    name: service.name().to_string(),
                    methods,
                },
            );
        }

        // Extract type information
        for message in &descriptor.message_type {
            let mut fields = Vec::new();

            for field in &message.field {
                fields.push(FieldInfo {
                    name: field.name().to_string(),
                    field_type: if field.type_name().is_empty() {
                        String::new()
                    } else {
                        field.type_name().to_string()
                    },
                    number: field.number(),
                    optional: field.label() == Label::LABEL_OPTIONAL,
                    repeated: field.label() == Label::LABEL_REPEATED,
                });
            }

            types.insert(
                message.name().to_string(),
                TypeInfo {
                    name: message.name().to_string(),
                    fields,
                    is_enum: false,
                    enum_values: Vec::new(),
                },
            );
        }

        // Extract enum information
        for enum_type in &descriptor.enum_type {
            let enum_values = enum_type
                .value
                .iter()
                .map(|v| v.name().to_string())
                .collect();

            types.insert(
                enum_type.name().to_string(),
                TypeInfo {
                    name: enum_type.name().to_string(),
                    fields: Vec::new(),
                    is_enum: true,
                    enum_values,
                },
            );
        }

        Self { services, types }
    }

    #[cfg(not(feature = "grpc"))]
    pub fn from_descriptor(_descriptor: &()) -> Self {
        Self {
            services: HashMap::new(),
            types: HashMap::new(),
        }
    }
}

impl Default for ProtoToWitConverter {
    fn default() -> Self {
        Self::new()
    }
}
