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
use std::collections::HashMap;

#[cfg(feature = "grpc")]
use protobuf::descriptor::{
    field_descriptor_proto::{Label, Type},
    FieldDescriptorProto, DescriptorProto, EnumDescriptorProto,
};

/// Maps protobuf types to their WIT equivalents
pub struct TypeMapper {
    /// Cache of resolved type mappings
    type_cache: HashMap<String, String>,
}

impl TypeMapper {
    pub fn new() -> Self {
        Self {
            type_cache: HashMap::new(),
        }
    }

    /// Map a protobuf scalar type to WIT type
    #[cfg(feature = "grpc")]
    pub fn map_scalar_type(&self, proto_type: Type) -> GrpcResult<&'static str> {
        let wit_type = match proto_type {
            Type::TYPE_BOOL => "bool",
            Type::TYPE_STRING => "string",
            Type::TYPE_BYTES => "list<u8>",
            
            // Integer types
            Type::TYPE_INT32 | Type::TYPE_SINT32 | Type::TYPE_SFIXED32 => "s32",
            Type::TYPE_INT64 | Type::TYPE_SINT64 | Type::TYPE_SFIXED64 => "s64",
            Type::TYPE_UINT32 | Type::TYPE_FIXED32 => "u32",
            Type::TYPE_UINT64 | Type::TYPE_FIXED64 => "u64",
            
            // Floating point types
            Type::TYPE_FLOAT => "float32",
            Type::TYPE_DOUBLE => "float64",
            
            // Complex types that need special handling
            Type::TYPE_MESSAGE => return Err(GrpcError::TypeMappingError {
                proto_type: "message".to_string(),
                message: "Message types should be handled separately".to_string(),
            }),
            Type::TYPE_ENUM => return Err(GrpcError::TypeMappingError {
                proto_type: "enum".to_string(),
                message: "Enum types should be handled separately".to_string(),
            }),
            Type::TYPE_GROUP => return Err(GrpcError::UnsupportedFeature(
                "GROUP type is deprecated and not supported".to_string()
            )),
        };
        
        Ok(wit_type)
    }

    #[cfg(not(feature = "grpc"))]
    pub fn map_scalar_type(&self, _proto_type: i32) -> GrpcResult<&'static str> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Map a protobuf field to WIT field, handling optional and repeated modifiers
    #[cfg(feature = "grpc")]
    pub fn map_field_type(
        &mut self,
        field: &FieldDescriptorProto,
        name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        let base_type = if !field.type_name().is_empty() {
            // Custom type (message or enum) - use name resolver to get WIT name
            name_resolver.proto_to_wit_type_name(field.type_name())
        } else {
            // Scalar type
            self.map_scalar_type(field.type_())?.to_string()
        };

        let wit_type = match field.label() {
            Label::LABEL_REPEATED => {
                format!("list<{}>", base_type)
            }
            Label::LABEL_OPTIONAL => {
                format!("option<{}>", base_type)
            }
            Label::LABEL_REQUIRED => {
                // Proto3 fields are implicitly optional for messages, required for scalars
                // We'll treat all fields as required unless explicitly optional
                base_type
            }
        };

        Ok(wit_type)
    }

    #[cfg(not(feature = "grpc"))]
    pub fn map_field_type(
        &mut self,
        _field: &(),
        _name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Generate WIT record definition from protobuf message
    #[cfg(feature = "grpc")]
    pub fn generate_wit_record(
        &mut self,
        message: &DescriptorProto,
        name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        let record_name = name_resolver.proto_to_wit_type_name(message.name());
        let mut fields = Vec::new();

        // Sort fields by field number to maintain consistent ordering
        let mut sorted_fields: Vec<_> = message.field.iter().collect();
        sorted_fields.sort_by_key(|f| f.number());

        for field in sorted_fields {
            let field_name = name_resolver.proto_to_wit_field_name(field.name());
            let field_type = self.map_field_type(field, name_resolver)?;
            fields.push(format!("  {}: {},", field_name, field_type));
        }

        let record_def = if fields.is_empty() {
            // Handle empty records
            format!("record {} {{}}", record_name)
        } else {
            format!(
                "record {} {{\n{}\n}}",
                record_name,
                fields.join("\n")
            )
        };

        Ok(record_def)
    }

    #[cfg(not(feature = "grpc"))]
    pub fn generate_wit_record(
        &mut self,
        _message: &(),
        _name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Generate WIT enum definition from protobuf enum
    #[cfg(feature = "grpc")]
    pub fn generate_wit_enum(
        &self,
        enum_desc: &EnumDescriptorProto,
        name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        let enum_name = name_resolver.proto_to_wit_type_name(enum_desc.name());
        let mut variants = Vec::new();

        // Sort enum values by number to maintain consistent ordering
        let mut sorted_values: Vec<_> = enum_desc.value.iter().collect();
        sorted_values.sort_by_key(|v| v.number());

        for value in sorted_values {
            let variant_name = name_resolver.proto_to_wit_field_name(value.name());
            variants.push(format!("  {},", variant_name));
        }

        let enum_def = if variants.is_empty() {
            return Err(GrpcError::InvalidProtobuf(
                format!("Enum '{}' has no values", enum_desc.name())
            ));
        } else {
            format!(
                "enum {} {{\n{}\n}}",
                enum_name,
                variants.join("\n")
            )
        };

        Ok(enum_def)
    }

    #[cfg(not(feature = "grpc"))]
    pub fn generate_wit_enum(
        &self,
        _enum_desc: &(),
        _name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Handle oneof fields (protobuf unions) by converting to WIT variants
    #[cfg(feature = "grpc")]
    pub fn generate_wit_variant_for_oneof(
        &mut self,
        oneof_name: &str,
        fields: &[&FieldDescriptorProto],
        name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        let variant_name = name_resolver.proto_to_wit_type_name(oneof_name);
        let mut cases = Vec::new();

        for field in fields {
            let case_name = name_resolver.proto_to_wit_field_name(field.name());
            let case_type = self.map_field_type(field, name_resolver)?;
            cases.push(format!("  {}({}),", case_name, case_type));
        }

        let variant_def = if cases.is_empty() {
            return Err(GrpcError::InvalidProtobuf(
                format!("Oneof '{}' has no fields", oneof_name)
            ));
        } else {
            format!(
                "variant {} {{\n{}\n}}",
                variant_name,
                cases.join("\n")
            )
        };

        Ok(variant_def)
    }

    #[cfg(not(feature = "grpc"))]
    pub fn generate_wit_variant_for_oneof(
        &mut self,
        _oneof_name: &str,
        _fields: &[&()],
        _name_resolver: &crate::grpc::NameResolver,
    ) -> GrpcResult<String> {
        Err(GrpcError::UnsupportedFeature(
            "gRPC support requires the 'grpc' feature to be enabled".to_string()
        ))
    }

    /// Cache a type mapping for reuse
    pub fn cache_type_mapping(&mut self, proto_type: String, wit_type: String) {
        self.type_cache.insert(proto_type, wit_type);
    }

    /// Get a cached type mapping
    pub fn get_cached_mapping(&self, proto_type: &str) -> Option<&String> {
        self.type_cache.get(proto_type)
    }
}

impl Default for TypeMapper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "grpc")]
    fn test_scalar_type_mapping() {
        let mapper = TypeMapper::new();
        
        assert_eq!(mapper.map_scalar_type(Type::TYPE_BOOL).unwrap(), "bool");
        assert_eq!(mapper.map_scalar_type(Type::TYPE_STRING).unwrap(), "string");
        assert_eq!(mapper.map_scalar_type(Type::TYPE_INT32).unwrap(), "s32");
        assert_eq!(mapper.map_scalar_type(Type::TYPE_UINT64).unwrap(), "u64");
        assert_eq!(mapper.map_scalar_type(Type::TYPE_FLOAT).unwrap(), "float32");
        assert_eq!(mapper.map_scalar_type(Type::TYPE_DOUBLE).unwrap(), "float64");
        assert_eq!(mapper.map_scalar_type(Type::TYPE_BYTES).unwrap(), "list<u8>");
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn test_unsupported_types() {
        let mapper = TypeMapper::new();
        
        let result = mapper.map_scalar_type(Type::TYPE_GROUP);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), GrpcError::UnsupportedFeature(_)));
    }
}