//! Integration test for gRPC proto-to-WIT conversion

#[cfg(feature = "grpc")]
#[cfg(test)]
mod grpc_tests {
    use golem_wasm_rpc::grpc::ProtoToWitConverter;

    #[test]
    fn test_simple_proto_to_wit_conversion() {
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

        let mut converter = ProtoToWitConverter::new();
        let result = converter.convert_proto_to_wit(proto_content, "test.v1", "1.0.0");

        assert!(
            result.is_ok(),
            "Proto to WIT conversion failed: {:?}",
            result.err()
        );

        let wit_content = result.unwrap();
        println!("Generated WIT:\n{}", wit_content);

        // Verify the generated WIT contains expected elements
        assert!(wit_content.contains("package test:v1@1.0.0"));
        assert!(wit_content.contains("interface types"));
        assert!(wit_content.contains("record hello-request"));
        assert!(wit_content.contains("record hello-response"));
        assert!(wit_content.contains("interface hello-service"));
        assert!(wit_content.contains("say-hello: func"));
        assert!(wit_content.contains("result<hello-response, grpc-error>"));
        assert!(wit_content.contains("world hello-service-api"));
    }

    #[test]
    fn test_streaming_methods_conversion() {
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

        let mut converter = ProtoToWitConverter::new();
        let result = converter.convert_proto_to_wit(proto_content, "stream.v1", "1.0.0");

        assert!(
            result.is_ok(),
            "Proto to WIT conversion failed: {:?}",
            result.err()
        );

        let wit_content = result.unwrap();
        println!("Generated streaming WIT:\n{}", wit_content);

        // Check that streaming methods are properly generated
        assert!(wit_content.contains("client-streaming: func(requests: list<stream-request>)"));
        assert!(wit_content.contains(
            "server-streaming: func(request: stream-request) -> result<list<stream-response>"
        ));
        assert!(wit_content.contains("bidirectional-streaming: func(requests: list<stream-request>) -> result<list<stream-response>"));
    }

    #[test]
    fn test_enum_conversion() {
        let proto_content = r#"
            syntax = "proto3";
            
            package enum_test.v1;
            
            enum Status {
                UNKNOWN = 0;
                PENDING = 1;
                COMPLETED = 2;
                FAILED = 3;
            }
            
            message StatusRequest {
                Status status = 1;
            }
            
            service StatusService {
                rpc GetStatus(StatusRequest) returns (StatusRequest);
            }
        "#;

        let mut converter = ProtoToWitConverter::new();
        let result = converter.convert_proto_to_wit(proto_content, "enum_test.v1", "1.0.0");

        assert!(
            result.is_ok(),
            "Proto to WIT conversion failed: {:?}",
            result.err()
        );

        let wit_content = result.unwrap();
        println!("Generated enum WIT:\n{}", wit_content);

        // Check that enums are properly converted
        assert!(wit_content.contains("enum status"));
        assert!(wit_content.contains("unknown"));
        assert!(wit_content.contains("pending"));
        assert!(wit_content.contains("completed"));
        assert!(wit_content.contains("failed"));
    }
}
