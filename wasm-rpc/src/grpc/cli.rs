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
use crate::grpc::ProtoToWitConverter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[cfg(feature = "cli")]
use serde_yaml;

/// Configuration for gRPC dependencies in golem.yaml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcDependencyConfig {
    /// Type of dependency - always "grpc" for gRPC dependencies
    #[serde(rename = "type")]
    pub dependency_type: String,
    
    /// Path to the .proto file
    pub proto: PathBuf,
    
    /// gRPC service endpoint URL
    pub endpoint: String,
    
    /// Protocol buffer package name
    pub package: String,
    
    /// Version of this dependency
    pub version: String,
    
    /// Authentication configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<GrpcAuthConfig>,
    
    /// TLS configuration
    #[serde(default = "default_tls")]
    pub tls: bool,
    
    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    
    /// Generated WIT output directory (relative to component root)
    #[serde(default = "default_wit_output")]
    pub wit_output: String,
}

/// Authentication configuration for gRPC services
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GrpcAuthConfig {
    #[serde(rename = "bearer")]
    Bearer { token: String },
    
    #[serde(rename = "basic")]
    Basic { username: String, password: String },
    
    #[serde(rename = "api-key")]
    ApiKey { key: String, header: String },
    
    #[serde(rename = "none")]
    None,
}

/// Extended golem.yaml configuration with gRPC dependencies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GolemYamlConfig {
    /// Traditional WIT dependencies
    #[serde(rename = "witDeps", skip_serializing_if = "Option::is_none")]
    pub wit_deps: Option<Vec<String>>,
    
    /// New-style dependencies including gRPC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<HashMap<String, GrpcDependencyConfig>>,
    
    /// Temporary directory for build artifacts
    #[serde(rename = "tempDir", skip_serializing_if = "Option::is_none")]
    pub temp_dir: Option<String>,
    
    /// Component templates configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub templates: Option<serde_yaml::Value>,
    
    /// Custom commands
    #[serde(rename = "customCommands", skip_serializing_if = "Option::is_none")]
    pub custom_commands: Option<serde_yaml::Value>,
}

/// CLI utilities for managing gRPC dependencies
pub struct GrpcCliManager {
    converter: ProtoToWitConverter,
}

impl GrpcCliManager {
    pub fn new() -> Self {
        Self {
            converter: ProtoToWitConverter::new(),
        }
    }
    
    /// Load golem.yaml configuration from a file
    pub fn load_golem_config<P: AsRef<Path>>(path: P) -> GrpcResult<GolemYamlConfig> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| GrpcError::IoError(format!("Failed to read golem.yaml: {}", e)))?;
        
        let config: GolemYamlConfig = serde_yaml::from_str(&content)
            .map_err(|e| GrpcError::ParseError(format!("Failed to parse golem.yaml: {}", e)))?;
        
        Ok(config)
    }
    
    /// Save golem.yaml configuration to a file
    pub fn save_golem_config<P: AsRef<Path>>(path: P, config: &GolemYamlConfig) -> GrpcResult<()> {
        let content = serde_yaml::to_string(config)
            .map_err(|e| GrpcError::ParseError(format!("Failed to serialize golem.yaml: {}", e)))?;
        
        std::fs::write(path, content)
            .map_err(|e| GrpcError::IoError(format!("Failed to write golem.yaml: {}", e)))?;
        
        Ok(())
    }
    
    /// Add a gRPC dependency to golem.yaml
    pub fn add_grpc_dependency(
        &self,
        config_path: &Path,
        dep_name: String,
        dep_config: GrpcDependencyConfig,
    ) -> GrpcResult<()> {
        let mut config = if config_path.exists() {
            Self::load_golem_config(config_path)?
        } else {
            GolemYamlConfig {
                wit_deps: None,
                dependencies: None,
                temp_dir: Some("target/golem-temp".to_string()),
                templates: None,
                custom_commands: None,
            }
        };
        
        // Initialize dependencies if not present
        if config.dependencies.is_none() {
            config.dependencies = Some(HashMap::new());
        }
        
        // Add the gRPC dependency
        config.dependencies
            .as_mut()
            .unwrap()
            .insert(dep_name, dep_config);
        
        Self::save_golem_config(config_path, &config)?;
        
        Ok(())
    }
    
    /// Generate WIT files for all gRPC dependencies in a golem.yaml
    pub fn generate_wit_for_dependencies(&mut self, config_path: &Path) -> GrpcResult<Vec<String>> {
        let config = Self::load_golem_config(config_path)?;
        let mut generated_files = Vec::new();
        
        if let Some(dependencies) = &config.dependencies {
            let base_dir = config_path.parent()
                .ok_or_else(|| GrpcError::InvalidConfiguration("Invalid config path".to_string()))?;
                
            for (dep_name, dep_config) in dependencies {
                if dep_config.dependency_type == "grpc" {
                    let proto_path = base_dir.join(&dep_config.proto);
                    let wit_output_dir = base_dir.join(&dep_config.wit_output);
                    
                    // Create output directory if it doesn't exist
                    std::fs::create_dir_all(&wit_output_dir)
                        .map_err(|e| GrpcError::IoError(format!("Failed to create WIT output directory: {}", e)))?;
                    
                    // Read and convert proto file
                    let proto_content = std::fs::read_to_string(&proto_path)
                        .map_err(|e| GrpcError::IoError(format!("Failed to read proto file {}: {}", proto_path.display(), e)))?;
                    
                    let wit_content = self.converter.convert_proto_to_wit(
                        &proto_content,
                        &dep_config.package,
                        &dep_config.version,
                    )?;
                    
                    // Generate output filename
                    let wit_filename = format!("{}.wit", dep_name.replace('-', "_"));
                    let wit_file_path = wit_output_dir.join(&wit_filename);
                    
                    // Write WIT file
                    std::fs::write(&wit_file_path, wit_content)
                        .map_err(|e| GrpcError::IoError(format!("Failed to write WIT file: {}", e)))?;
                    
                    generated_files.push(wit_file_path.to_string_lossy().to_string());
                }
            }
        }
        
        Ok(generated_files)
    }
    
    /// List all gRPC dependencies in a golem.yaml
    pub fn list_grpc_dependencies(&self, config_path: &Path) -> GrpcResult<Vec<(String, GrpcDependencyConfig)>> {
        let config = Self::load_golem_config(config_path)?;
        let mut grpc_deps = Vec::new();
        
        if let Some(dependencies) = &config.dependencies {
            for (name, dep_config) in dependencies {
                if dep_config.dependency_type == "grpc" {
                    grpc_deps.push((name.clone(), dep_config.clone()));
                }
            }
        }
        
        Ok(grpc_deps)
    }
    
    /// Remove a gRPC dependency from golem.yaml
    pub fn remove_grpc_dependency(&self, config_path: &Path, dep_name: &str) -> GrpcResult<bool> {
        let mut config = Self::load_golem_config(config_path)?;
        
        if let Some(dependencies) = &mut config.dependencies {
            if dependencies.remove(dep_name).is_some() {
                Self::save_golem_config(config_path, &config)?;
                return Ok(true);
            }
        }
        
        Ok(false)
    }
    
    /// Validate all gRPC dependencies in a golem.yaml
    pub fn validate_dependencies(&self, config_path: &Path) -> GrpcResult<Vec<String>> {
        let config = Self::load_golem_config(config_path)?;
        let mut errors = Vec::new();
        
        if let Some(dependencies) = &config.dependencies {
            let base_dir = config_path.parent()
                .ok_or_else(|| GrpcError::InvalidConfiguration("Invalid config path".to_string()))?;
                
            for (dep_name, dep_config) in dependencies {
                if dep_config.dependency_type == "grpc" {
                    // Validate proto file exists
                    let proto_path = base_dir.join(&dep_config.proto);
                    if !proto_path.exists() {
                        errors.push(format!("Proto file not found for dependency '{}': {}", dep_name, proto_path.display()));
                        continue;
                    }
                    
                    // Validate proto file is parseable
                    if let Ok(proto_content) = std::fs::read_to_string(&proto_path) {
                        let mut parser = crate::grpc::ProtobufParser::new();
                        if let Err(e) = parser.parse(&proto_content) {
                            errors.push(format!("Invalid proto file for dependency '{}': {}", dep_name, e));
                        }
                    } else {
                        errors.push(format!("Cannot read proto file for dependency '{}': {}", dep_name, proto_path.display()));
                    }
                    
                    // Validate endpoint URL format
                    if dep_config.endpoint.is_empty() {
                        errors.push(format!("Empty endpoint for dependency '{}'", dep_name));
                    } else if !dep_config.endpoint.starts_with("http://") && !dep_config.endpoint.starts_with("https://") {
                        errors.push(format!("Invalid endpoint URL format for dependency '{}': {}", dep_name, dep_config.endpoint));
                    }
                    
                    // Validate package name
                    if dep_config.package.is_empty() {
                        errors.push(format!("Empty package name for dependency '{}'", dep_name));
                    }
                    
                    // Validate version
                    if dep_config.version.is_empty() {
                        errors.push(format!("Empty version for dependency '{}'", dep_name));
                    }
                }
            }
        }
        
        Ok(errors)
    }
}

impl Default for GrpcCliManager {
    fn default() -> Self {
        Self::new()
    }
}

fn default_tls() -> bool {
    true
}

fn default_timeout() -> u64 {
    30
}

fn default_wit_output() -> String {
    "wit-generated/grpc".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_golem_yaml_serialization() {
        let mut dependencies = HashMap::new();
        dependencies.insert("user-service".to_string(), GrpcDependencyConfig {
            dependency_type: "grpc".to_string(),
            proto: PathBuf::from("./protos/user_service.proto"),
            endpoint: "https://api.example.com:443".to_string(),
            package: "com.example.userservice".to_string(),
            version: "1.0.0".to_string(),
            auth: Some(GrpcAuthConfig::Bearer {
                token: "${GRPC_TOKEN}".to_string(),
            }),
            tls: true,
            timeout: 30,
            wit_output: "wit-generated/grpc".to_string(),
        });
        
        let config = GolemYamlConfig {
            wit_deps: None,
            dependencies: Some(dependencies),
            temp_dir: Some("target/golem-temp".to_string()),
            templates: None,
            custom_commands: None,
        };
        
        let yaml = serde_yaml::to_string(&config).unwrap();
        println!("Generated YAML:\n{}", yaml);
        
        // Verify it can be parsed back
        let parsed: GolemYamlConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(parsed.dependencies.is_some());
        assert_eq!(parsed.dependencies.as_ref().unwrap().len(), 1);
    }
    
    #[test]
    fn test_cli_manager_operations() {
        let manager = GrpcCliManager::new();
        let temp_file = NamedTempFile::new().unwrap();
        let config_path = temp_file.path();
        
        // Add a gRPC dependency
        let dep_config = GrpcDependencyConfig {
            dependency_type: "grpc".to_string(),
            proto: PathBuf::from("./test.proto"),
            endpoint: "https://test-service:443".to_string(),
            package: "test.v1".to_string(),
            version: "1.0.0".to_string(),
            auth: None,
            tls: true,
            timeout: 30,
            wit_output: "wit-generated/grpc".to_string(),
        };
        
        manager.add_grpc_dependency(config_path, "test-service".to_string(), dep_config).unwrap();
        
        // List dependencies
        let deps = manager.list_grpc_dependencies(config_path).unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].0, "test-service");
        
        // Remove dependency
        let removed = manager.remove_grpc_dependency(config_path, "test-service").unwrap();
        assert!(removed);
        
        // Verify it's gone
        let deps = manager.list_grpc_dependencies(config_path).unwrap();
        assert_eq!(deps.len(), 0);
    }
}