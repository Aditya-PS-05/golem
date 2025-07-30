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
use std::collections::{HashMap, HashSet};

/// Resolves naming conflicts and converts protobuf names to WIT-idiomatic names
pub struct NameResolver {
    /// Maps original protobuf names to WIT names
    name_mappings: HashMap<String, String>,
    /// Tracks used WIT names to detect conflicts
    used_names: HashSet<String>,
    /// WIT reserved words that need prefixing
    reserved_words: HashSet<&'static str>,
}

impl NameResolver {
    pub fn new() -> Self {
        let mut reserved_words = HashSet::new();
        
        // WIT reserved words that need to be prefixed with %
        reserved_words.insert("use");
        reserved_words.insert("type");
        reserved_words.insert("func");
        reserved_words.insert("u8");
        reserved_words.insert("u16");
        reserved_words.insert("u32");
        reserved_words.insert("u64");
        reserved_words.insert("s8");
        reserved_words.insert("s16");
        reserved_words.insert("s32");
        reserved_words.insert("s64");
        reserved_words.insert("f32");
        reserved_words.insert("f64");
        reserved_words.insert("float32");
        reserved_words.insert("float64");
        reserved_words.insert("char");
        reserved_words.insert("bool");
        reserved_words.insert("string");
        reserved_words.insert("option");
        reserved_words.insert("result");
        reserved_words.insert("list");
        reserved_words.insert("record");
        reserved_words.insert("variant");
        reserved_words.insert("enum");
        reserved_words.insert("flags");
        reserved_words.insert("resource");
        reserved_words.insert("static");
        reserved_words.insert("interface");
        reserved_words.insert("world");
        reserved_words.insert("import");
        reserved_words.insert("export");
        reserved_words.insert("package");
        reserved_words.insert("include");
        reserved_words.insert("with");
        reserved_words.insert("as");

        Self {
            name_mappings: HashMap::new(),
            used_names: HashSet::new(),
            reserved_words,
        }
    }

    /// Convert a protobuf name to WIT kebab-case
    pub fn to_kebab_case(&self, name: &str) -> String {
        // Special case for all-caps names (common in enums)
        if name.chars().all(|c| c.is_uppercase() || c == '_' || c.is_numeric()) {
            return name.to_lowercase().replace('_', "-");
        }
        
        // Handle common protobuf naming patterns
        let mut result = String::new();
        let mut chars = name.chars().peekable();
        let mut first = true;

        while let Some(ch) = chars.next() {
            if ch.is_uppercase() && !first {
                // Add hyphen before uppercase letters (except the first character)
                if let Some(&next_ch) = chars.peek() {
                    if next_ch.is_lowercase() || result.chars().last().map_or(false, |c| c.is_lowercase()) {
                        result.push('-');
                    }
                }
            }
            
            result.push(ch.to_lowercase().next().unwrap_or(ch));
            first = false;
        }

        // Replace underscores with hyphens
        result = result.replace('_', "-");
        
        // Remove multiple consecutive hyphens
        while result.contains("--") {
            result = result.replace("--", "-");
        }
        
        // Remove leading/trailing hyphens
        result = result.trim_matches('-').to_string();
        
        // Ensure the name is not empty
        if result.is_empty() {
            result = "unnamed".to_string();
        }

        result
    }

    /// Convert protobuf type name to WIT type name
    pub fn proto_to_wit_type_name(&self, proto_name: &str) -> String {
        // Remove leading dots (fully qualified names)
        let clean_name = proto_name.trim_start_matches('.');
        
        // Split by dots and convert each part
        let parts: Vec<String> = clean_name
            .split('.')
            .map(|part| self.to_kebab_case(part))
            .collect();
        
        // Join with hyphens for nested types
        let wit_name = parts.join("-");
        
        // Handle reserved words
        if self.reserved_words.contains(wit_name.as_str()) {
            format!("%{}", wit_name)
        } else {
            wit_name
        }
    }

    /// Convert protobuf field name to WIT field name
    pub fn proto_to_wit_field_name(&self, field_name: &str) -> String {
        let wit_name = self.to_kebab_case(field_name);
        
        // Handle reserved words
        if self.reserved_words.contains(wit_name.as_str()) {
            format!("%{}", wit_name)
        } else {
            wit_name
        }
    }

    /// Convert protobuf method name to WIT function name
    pub fn proto_to_wit_method_name(&self, method_name: &str) -> String {
        let wit_name = self.to_kebab_case(method_name);
        
        // Handle reserved words
        if self.reserved_words.contains(wit_name.as_str()) {
            format!("%{}", wit_name)
        } else {
            wit_name
        }
    }

    /// Convert protobuf service name to WIT interface name
    pub fn proto_to_wit_interface_name(&self, service_name: &str) -> String {
        let wit_name = self.to_kebab_case(service_name);
        
        // Handle reserved words
        if self.reserved_words.contains(wit_name.as_str()) {
            format!("%{}", wit_name)
        } else {
            wit_name
        }
    }

    /// Register a name mapping and check for conflicts
    pub fn register_name_mapping(
        &mut self,
        proto_name: String,
        wit_name: String,
    ) -> GrpcResult<String> {
        // Check if we already have a mapping for this proto name
        if let Some(existing) = self.name_mappings.get(&proto_name) {
            return Ok(existing.clone());
        }

        // Check for conflicts with already used names
        let final_wit_name = if self.used_names.contains(&wit_name) {
            self.resolve_name_conflict(&wit_name)?
        } else {
            wit_name
        };

        // Register the mapping
        self.name_mappings.insert(proto_name, final_wit_name.clone());
        self.used_names.insert(final_wit_name.clone());

        Ok(final_wit_name)
    }

    /// Resolve name conflicts by appending suffixes
    fn resolve_name_conflict(&self, base_name: &str) -> GrpcResult<String> {
        // Try adding numeric suffixes
        for i in 2..=100 {
            let candidate = format!("{}-{}", base_name, i);
            if !self.used_names.contains(&candidate) {
                return Ok(candidate);
            }
        }

        // If we can't resolve after 100 attempts, it's an error
        Err(GrpcError::NameResolutionError {
            name: base_name.to_string(),
            message: "Could not resolve name conflict after 100 attempts".to_string(),
        })
    }

    /// Validate that a WIT name is valid
    pub fn validate_wit_name(&self, name: &str) -> GrpcResult<()> {
        if name.is_empty() {
            return Err(GrpcError::NameResolutionError {
                name: name.to_string(),
                message: "Name cannot be empty".to_string(),
            });
        }

        if name.len() > 64 {
            return Err(GrpcError::NameResolutionError {
                name: name.to_string(),
                message: "Name exceeds 64 character limit".to_string(),
            });
        }

        // Check that the name starts with a letter or %
        let first_char = name.chars().next().unwrap();
        if !first_char.is_alphabetic() && first_char != '%' {
            return Err(GrpcError::NameResolutionError {
                name: name.to_string(),
                message: "Name must start with a letter or %".to_string(),
            });
        }

        // Check that all characters are valid (letters, numbers, hyphens, %)
        for ch in name.chars() {
            if !ch.is_alphanumeric() && ch != '-' && ch != '%' {
                return Err(GrpcError::NameResolutionError {
                    name: name.to_string(),
                    message: format!("Invalid character '{}' in name", ch),
                });
            }
        }

        Ok(())
    }

    /// Get the WIT name for a protobuf name, if it was registered
    pub fn get_wit_name(&self, proto_name: &str) -> Option<&String> {
        self.name_mappings.get(proto_name)
    }

    /// Check if a WIT name is already used
    pub fn is_name_used(&self, wit_name: &str) -> bool {
        self.used_names.contains(wit_name)
    }

    /// Generate a package name from protobuf package
    pub fn generate_package_name(&self, proto_package: &str, version: &str) -> String {
        let package_parts: Vec<String> = proto_package
            .split('.')
            .map(|part| self.to_kebab_case(part))
            .collect();
        
        format!("{}@{}", package_parts.join(":"), version)
    }
}

impl Default for NameResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_kebab_case() {
        let resolver = NameResolver::new();
        
        assert_eq!(resolver.to_kebab_case("HelloWorld"), "hello-world");
        assert_eq!(resolver.to_kebab_case("user_id"), "user-id");
        assert_eq!(resolver.to_kebab_case("getUserName"), "get-user-name");
        assert_eq!(resolver.to_kebab_case("HTTPRequest"), "http-request");
        assert_eq!(resolver.to_kebab_case("simple"), "simple");
        assert_eq!(resolver.to_kebab_case("SimpleMessage"), "simple-message");
        assert_eq!(resolver.to_kebab_case("ID"), "id");
        assert_eq!(resolver.to_kebab_case("user_ID"), "user-id");
    }

    #[test]
    fn test_reserved_words() {
        let resolver = NameResolver::new();
        
        assert_eq!(resolver.proto_to_wit_field_name("type"), "%type");
        assert_eq!(resolver.proto_to_wit_field_name("use"), "%use");
        assert_eq!(resolver.proto_to_wit_field_name("string"), "%string");
        assert_eq!(resolver.proto_to_wit_field_name("normal_field"), "normal-field");
    }

    #[test]
    fn test_name_validation() {
        let resolver = NameResolver::new();
        
        assert!(resolver.validate_wit_name("valid-name").is_ok());
        assert!(resolver.validate_wit_name("%type").is_ok());
        assert!(resolver.validate_wit_name("").is_err());
        assert!(resolver.validate_wit_name("123invalid").is_err());
        assert!(resolver.validate_wit_name("invalid@name").is_err());
    }

    #[test]
    fn test_name_conflict_resolution() {
        let mut resolver = NameResolver::new();
        
        // Register the first name
        let name1 = resolver.register_name_mapping("proto1".to_string(), "test".to_string()).unwrap();
        assert_eq!(name1, "test");
        
        // Register a conflicting name
        let name2 = resolver.register_name_mapping("proto2".to_string(), "test".to_string()).unwrap();
        assert_eq!(name2, "test-2");
        
        // Register another conflicting name
        let name3 = resolver.register_name_mapping("proto3".to_string(), "test".to_string()).unwrap();
        assert_eq!(name3, "test-3");
    }

    #[test]
    fn test_package_name_generation() {
        let resolver = NameResolver::new();
        
        let package = resolver.generate_package_name("com.example.service", "1.0.0");
        assert_eq!(package, "com:example:service@1.0.0");
        
        let package2 = resolver.generate_package_name("simple", "2.1.0");
        assert_eq!(package2, "simple@2.1.0");
    }
}