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

use crate::workerctx::WorkerCtx;
use anyhow::{anyhow, Context};
use golem_common::model::component_metadata::DynamicLinkedGrpc;
use golem_wasm_rpc::{Value, WitValue, WitValueExtractor};
use std::collections::HashMap;
use wasmtime::component::types::{ComponentInstance, ComponentItem};
use wasmtime::component::{LinkerInstance, Resource, ResourceType, Type, Val};
use wasmtime::{AsContextMut, Engine, StoreContextMut};
use wasmtime_wasi::IoView;

/// Dynamic gRPC client entry for managing connections and calls
#[derive(Debug)]
pub struct DynamicGrpcEntry {
    pub endpoint: String,
    pub package: String,
    pub service_name: String,
    pub version: String,
    pub auth: Option<golem_common::model::component_metadata::GrpcAuthConfig>,
    pub tls: bool,
    pub timeout: u64,
}

/// Information about a gRPC method for dynamic stub generation
#[derive(Debug, Clone)]
struct GrpcMethodInfo {
    method_name: String,
    params: Vec<Type>,
    results: Vec<Type>,
    streaming_type: GrpcStreamingType,
}

/// Information about a gRPC service function for dynamic stub generation
#[derive(Debug, Clone)]
struct GrpcFunctionInfo {
    service_name: String,
    method_name: String,
    params: Vec<Type>,
    results: Vec<Type>,
    streaming_type: GrpcStreamingType,
}

/// Type of gRPC streaming for a method
#[derive(Debug, Clone, PartialEq)]
enum GrpcStreamingType {
    Unary,
    ClientStreaming,
    ServerStreaming,  
    Bidirectional,
}

/// Dynamic gRPC call types that can be handled
#[derive(Debug, Clone)]
enum DynamicGrpcCall {
    /// Constructor for gRPC service client
    ServiceConstructor {
        service_name: String,
    },
    /// Unary gRPC method call
    UnaryCall {
        service_name: String,
        method_name: String,
    },
    /// Server streaming gRPC method call (returns list of responses)  
    ServerStreamingCall {
        service_name: String,
        method_name: String,
    },
    /// Client streaming gRPC method call (not supported in WIT - should error)
    ClientStreamingCall {
        service_name: String,
        method_name: String,
    },
    /// Bidirectional streaming gRPC method call (not supported in WIT - should error)
    BidirectionalCall {
        service_name: String,
        method_name: String,
    },
}

/// Register dynamic gRPC linking for a component instance
pub fn dynamic_grpc_link<Ctx: WorkerCtx>(
    name: &str,
    grpc_metadata: &DynamicLinkedGrpc,
    engine: &Engine,
    root: &mut LinkerInstance<Ctx>,
    inst: &ComponentInstance,
) -> anyhow::Result<()> {
    let mut instance = root.instance(name)?;
    let mut services: HashMap<String, Vec<GrpcMethodInfo>> = HashMap::new();
    let mut functions = Vec::new();

    // Analyze the WIT interface to extract gRPC service methods
    for (inner_name, inner_item) in inst.exports(engine) {
        match inner_item {
            ComponentItem::ComponentFunc(fun) => {
                let param_types: Vec<Type> = fun.params().map(|(_, t)| t).collect();
                let result_types: Vec<Type> = fun.results().collect();

                // Parse method name to extract service and method information
                if let Some((service_name, method_name)) = parse_grpc_method_name(&inner_name) {
                    let streaming_type = infer_streaming_type(&param_types, &result_types);
                    
                    let method_info = GrpcMethodInfo {
                        method_name: method_name.clone(),
                        params: param_types.clone(),
                        results: result_types.clone(),
                        streaming_type: streaming_type.clone(),
                    };

                    services.entry(service_name.clone()).or_default().push(method_info);

                    functions.push(GrpcFunctionInfo {
                        service_name,
                        method_name,
                        params: param_types,
                        results: result_types,
                        streaming_type,
                    });
                }
            }
            ComponentItem::Resource(_resource) => {
                // Handle gRPC service resources (client stubs)
                services.entry(inner_name.to_string()).or_default();
            }
            _ => {}
        }
    }

    // Register resources for gRPC service clients
    let service_names: Vec<String> = services.keys()
        .filter(|service_name| grpc_metadata.targets.contains_key(*service_name))
        .cloned()
        .collect();
    
    for service_name in service_names {
        let service_name_clone = service_name.clone();
        instance.resource_async(
            &service_name,
            ResourceType::host::<DynamicGrpcEntry>(),
            move |store, rep| {
                let service_name = service_name_clone.clone();
                Box::new(async move {
                    drop_grpc_client(store, rep, &service_name).await
                })
            },
        )?;
    }

    // Register functions for gRPC method calls
    for function in functions {
        let call_type = DynamicGrpcCall::analyse(
            &function,
            grpc_metadata,
        )?;

        if let Some(call_type) = call_type {
            let function_name = format!("{}-{}", function.service_name, function.method_name);
            
            instance.func_new_async(
                &function_name,
                move |store, params, results| {
                    let param_types = function.params.clone();
                    let result_types = function.results.clone();
                    let call_type = call_type.clone();
                    let service_name = function.service_name.clone();
                    let method_name = function.method_name.clone();
                    
                    Box::new(async move {
                        dynamic_grpc_function_call(
                            store,
                            &service_name,
                            &method_name,
                            params,
                            &param_types,
                            results,
                            &result_types,
                            &call_type,
                        )
                        .await?;
                        Ok(())
                    })
                },
            )?;
        }
    }

    Ok(())
}

/// Parse a WIT function name to extract gRPC service and method names
/// Expected format: "service-name" for constructors, "method-name" for methods
fn parse_grpc_method_name(wit_name: &str) -> Option<(String, String)> {
    // For now, assume format is "service_method" or just "method" 
    // This would be determined by the proto-to-WIT conversion
    if wit_name.contains('-') {
        let parts: Vec<&str> = wit_name.splitn(2, '-').collect();
        if parts.len() == 2 {
            Some((parts[0].to_string(), parts[1].to_string()))
        } else {
            None
        }
    } else {
        // Assume it's a constructor or single method
        Some(("default".to_string(), wit_name.to_string()))
    }
}

/// Infer gRPC streaming type from WIT function signature
fn infer_streaming_type(param_types: &[Type], result_types: &[Type]) -> GrpcStreamingType {
    // Simple heuristic: if result is list<T>, it's server streaming
    // More sophisticated logic would be needed for a complete implementation
    if result_types.len() == 1 {
        // Check if result is a list type (server streaming)
        // This is a simplified check - real implementation would need deeper type analysis
        if let Type::List(_) = result_types[0] {
            return GrpcStreamingType::ServerStreaming;
        }
    }
    
    // Default to unary for now
    GrpcStreamingType::Unary
}

impl DynamicGrpcCall {
    pub fn analyse(
        function: &GrpcFunctionInfo,
        grpc_metadata: &DynamicLinkedGrpc,
    ) -> anyhow::Result<Option<DynamicGrpcCall>> {
        if let Some(_target) = grpc_metadata.targets.get(&function.service_name) {
            match function.streaming_type {
                GrpcStreamingType::Unary => {
                    Ok(Some(DynamicGrpcCall::UnaryCall {
                        service_name: function.service_name.clone(),
                        method_name: function.method_name.clone(),
                    }))
                }
                GrpcStreamingType::ServerStreaming => {
                    Ok(Some(DynamicGrpcCall::ServerStreamingCall {
                        service_name: function.service_name.clone(),
                        method_name: function.method_name.clone(),
                    }))
                }
                GrpcStreamingType::ClientStreaming => {
                    // Client streaming not supported in WIT - would need special handling
                    Ok(Some(DynamicGrpcCall::ClientStreamingCall {
                        service_name: function.service_name.clone(),
                        method_name: function.method_name.clone(),
                    }))
                }
                GrpcStreamingType::Bidirectional => {
                    // Bidirectional streaming not supported in WIT - would need special handling
                    Ok(Some(DynamicGrpcCall::BidirectionalCall {
                        service_name: function.service_name.clone(),
                        method_name: function.method_name.clone(),
                    }))
                }
            }
        } else {
            Ok(None)
        }
    }
}

/// Handle dynamic gRPC function calls
async fn dynamic_grpc_function_call<Ctx: WorkerCtx>(
    mut store: impl AsContextMut<Data = Ctx> + Send,
    service_name: &str,  
    method_name: &str,
    params: &[Val],
    param_types: &[Type],
    results: &mut [Val],
    result_types: &[Type],
    call_type: &DynamicGrpcCall,
) -> anyhow::Result<()> {
    let mut store = store.as_context_mut();
    
    match call_type {
        DynamicGrpcCall::ServiceConstructor { service_name } => {
            // Create a gRPC client stub
            let grpc_entry = create_grpc_client(&mut store, service_name).await?;
            let handle = register_grpc_entry(&mut store, grpc_entry)?;
            results[0] = Val::Resource(handle.try_into_resource_any(store)?);
        }
        DynamicGrpcCall::UnaryCall { service_name, method_name } => {
            // Handle unary gRPC call
            let handle = extract_grpc_handle(params)?;
            let grpc_entry: Resource<DynamicGrpcEntry> = handle.try_into_resource(&mut store)?;
            
            let result = grpc_unary_call(
                &mut store,
                grpc_entry,
                service_name,
                method_name,
                params,
                param_types,
            ).await?;
            
            encode_grpc_result(result, results, result_types, &mut store).await?;
        }
        DynamicGrpcCall::ServerStreamingCall { service_name, method_name } => {
            // Handle server streaming gRPC call (returns list of responses)
            let handle = extract_grpc_handle(params)?;
            let grpc_entry: Resource<DynamicGrpcEntry> = handle.try_into_resource(&mut store)?;
            
            let result = grpc_server_streaming_call(
                &mut store,
                grpc_entry,
                service_name,
                method_name,
                params,
                param_types,
            ).await?;
            
            encode_grpc_result(result, results, result_types, &mut store).await?;
        }
        DynamicGrpcCall::ClientStreamingCall { .. } => {
            return Err(anyhow!(
                "Client streaming gRPC calls are not supported in WIT interfaces"
            ));
        }
        DynamicGrpcCall::BidirectionalCall { .. } => {
            return Err(anyhow!(
                "Bidirectional streaming gRPC calls are not supported in WIT interfaces"
            ));
        }
    }

    Ok(())
}

/// Create a gRPC client for the specified service
async fn create_grpc_client<Ctx: WorkerCtx>(
    store: &mut StoreContextMut<'_, Ctx>,
    service_name: &str,
) -> anyhow::Result<DynamicGrpcEntry> {
    // This would typically get configuration from component metadata
    // For now, return a placeholder
    Ok(DynamicGrpcEntry {
        endpoint: format!("https://{}.example.com:443", service_name),
        package: "example.v1".to_string(),
        service_name: service_name.to_string(),
        version: "1.0.0".to_string(),
        auth: None,
        tls: true,
        timeout: 30,
    })
}

/// Register a gRPC entry in the resource table
fn register_grpc_entry<Ctx: WorkerCtx>(
    store: &mut StoreContextMut<'_, Ctx>,
    grpc_entry: DynamicGrpcEntry,
) -> anyhow::Result<Resource<DynamicGrpcEntry>> {
    let mut wasi = store.data_mut().as_wasi_view();
    let table = wasi.table();
    Ok(table.push(grpc_entry)?)
}

/// Extract gRPC handle from function parameters
fn extract_grpc_handle(params: &[Val]) -> anyhow::Result<wasmtime::component::ResourceAny> {
    match params.get(0) {
        Some(Val::Resource(handle)) => Ok(*handle),
        _ => Err(anyhow!("Invalid parameter - expected gRPC client handle as first parameter")),
    }
}

/// Perform a unary gRPC call
async fn grpc_unary_call<Ctx: WorkerCtx>(
    store: &mut StoreContextMut<'_, Ctx>,
    _grpc_entry: Resource<DynamicGrpcEntry>,
    service_name: &str,
    method_name: &str,
    params: &[Val],
    param_types: &[Type],
) -> anyhow::Result<Value> {
    // TODO: Implement actual gRPC call using tonic or similar
    // For now, return a mock response
    
    tracing::info!(
        "Mock gRPC unary call: {}.{} with {} parameters",
        service_name,
        method_name,
        params.len() - 1 // Exclude the handle parameter
    );
    
    // Return a mock successful response
    Ok(Value::Record(vec![
        Value::String("mock_response".to_string()),
        Value::S32(42),
    ]))
}

/// Perform a server streaming gRPC call  
async fn grpc_server_streaming_call<Ctx: WorkerCtx>(
    store: &mut StoreContextMut<'_, Ctx>,
    _grpc_entry: Resource<DynamicGrpcEntry>,
    service_name: &str,
    method_name: &str,
    params: &[Val],
    param_types: &[Type],
) -> anyhow::Result<Value> {
    // TODO: Implement actual gRPC streaming call
    // For now, return a mock list of responses
    
    tracing::info!(
        "Mock gRPC server streaming call: {}.{} with {} parameters",
        service_name,
        method_name, 
        params.len() - 1
    );
    
    // Return a mock list of responses
    Ok(Value::List(vec![
        Value::Record(vec![
            Value::String("response_1".to_string()),
            Value::S32(1),
        ]),
        Value::Record(vec![
            Value::String("response_2".to_string()),
            Value::S32(2),
        ]),
    ]))
}

/// Encode gRPC result value into WIT values for return
async fn encode_grpc_result<Ctx: WorkerCtx>(
    result: Value,
    results: &mut [Val],
    result_types: &[Type],  
    store: &mut StoreContextMut<'_, Ctx>,
) -> anyhow::Result<()> {
    // Convert Value to WitValue and then to wasmtime Val
    let wit_value: WitValue = result.into();
    
    // TODO: Implement proper encoding using golem_wasm_rpc::wasmtime::decode_param
    // For now, implement a simplified version
    match wit_value {
        _ if wit_value.field(0).is_some() => {
            // Handle record types - extract all fields 
            let mut field_idx = 0;
            while let Some(field_ptr) = wit_value.field(field_idx) {
                if field_idx < results.len() && field_idx < result_types.len() {
                    // Extract field value directly from the pointer
                    let field_val = if let Some(s) = field_ptr.string() {
                        Val::String(s.to_string())
                    } else if let Some(n) = field_ptr.s32() {
                        Val::S32(n)
                    } else if let Some(n) = field_ptr.u32() {
                        Val::U32(n)
                    } else if let Some(n) = field_ptr.s64() {
                        Val::S64(n)
                    } else if let Some(n) = field_ptr.u64() {
                        Val::U64(n)
                    } else if let Some(b) = field_ptr.bool() {
                        Val::Bool(b)
                    } else {
                        return Err(anyhow!("Unsupported field type at index {}", field_idx));
                    };
                    results[field_idx] = field_val;
                }
                field_idx += 1;
            }
        }
        _ if wit_value.list_elements(|_| ()).is_some() => {
            if !results.is_empty() {
                // For lists, extract each element directly
                if let Some(list_vals) = wit_value.list_elements(|ptr| {
                    if let Some(s) = ptr.string() {
                        Ok(Val::String(s.to_string()))
                    } else if let Some(n) = ptr.s32() {
                        Ok(Val::S32(n))
                    } else if let Some(n) = ptr.u32() {
                        Ok(Val::U32(n))
                    } else if let Some(n) = ptr.s64() {
                        Ok(Val::S64(n))
                    } else if let Some(n) = ptr.u64() {
                        Ok(Val::U64(n))
                    } else if let Some(b) = ptr.bool() {
                        Ok(Val::Bool(b))
                    } else {
                        Err(anyhow!("Unsupported list element type"))
                    }
                }) {
                    let vals: Result<Vec<_>, _> = list_vals.into_iter().collect();
                    results[0] = Val::List(vals?);
                }
            }
        }
        _ => {
            if !results.is_empty() {
                results[0] = encode_wit_value_to_val(&wit_value, &result_types[0])?;
            }
        }
    }
    
    Ok(())
}

/// Convert WitValue to wasmtime Val (simplified implementation)
fn encode_wit_value_to_val(wit_value: &WitValue, _result_type: &Type) -> anyhow::Result<Val> {
    match wit_value {
        _ if wit_value.string().is_some() => Ok(Val::String(wit_value.string().unwrap().to_string())),
        _ if wit_value.s32().is_some() => Ok(Val::S32(wit_value.s32().unwrap())),
        _ if wit_value.u32().is_some() => Ok(Val::U32(wit_value.u32().unwrap())),
        _ if wit_value.s64().is_some() => Ok(Val::S64(wit_value.s64().unwrap())),
        _ if wit_value.u64().is_some() => Ok(Val::U64(wit_value.u64().unwrap())),
        _ if wit_value.bool().is_some() => Ok(Val::Bool(wit_value.bool().unwrap())),
        _ if wit_value.field(0).is_some() => {
            // Handle record types - extract all fields
            let mut vals = Vec::new();
            let mut field_idx = 0;
            while let Some(field_ptr) = wit_value.field(field_idx) {
                // For now, use generic field names - in practice you'd need type info
                let field_name = format!("field_{}", field_idx);
                
                // For scalar fields, extract the value directly from the pointer
                let field_val = if let Some(s) = field_ptr.string() {
                    Val::String(s.to_string())
                } else if let Some(n) = field_ptr.s32() {
                    Val::S32(n)
                } else if let Some(n) = field_ptr.u32() {
                    Val::U32(n)
                } else if let Some(n) = field_ptr.s64() {
                    Val::S64(n)
                } else if let Some(n) = field_ptr.u64() {
                    Val::U64(n)
                } else if let Some(b) = field_ptr.bool() {
                    Val::Bool(b)
                } else {
                    // Fallback for complex types
                    return Err(anyhow!("Unsupported field type at index {}", field_idx));
                };
                
                vals.push((field_name, field_val));
                field_idx += 1;
            }
            Ok(Val::Record(vals))
        }
        _ => Err(anyhow!("Unsupported WitValue type for encoding: {:?}", wit_value)),
    }
}

/// Clean up gRPC client when resource is dropped
async fn drop_grpc_client<Ctx: WorkerCtx>(
    mut store: StoreContextMut<'_, Ctx>,
    rep: u32,
    service_name: &str,
) -> anyhow::Result<()> {
    tracing::debug!("Dropping gRPC client for service: {}", service_name);
    
    // TODO: Implement proper cleanup of gRPC connections
    // For now, just log the cleanup
    
    Ok(())
}