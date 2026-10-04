use acyclic_sdk_contract_wire::{
    facade_operations, generate_remote_facades, CancellationKind, FacadeLanguage,
    family_registry::FAMILY_VIEWS,
};

fn cancellation_name(cancellation: CancellationKind) -> &'static str {
    match cancellation {
        CancellationKind::None => "none",
        CancellationKind::Call => "call",
        CancellationKind::Operation => "operation",
    }
}

fn rendered_operation_line<'a>(source: &'a str, rpc: &str, language: &str) -> &'a str {
    source
        .lines()
        .find(|line| line.contains(rpc))
        .unwrap_or_else(|| panic!("{language} facade omitted Rust-authority RPC {rpc}"))
}

#[test]
fn every_generated_remote_facade_contains_every_rust_authority_rpc() {
    let outputs = generate_remote_facades();
    assert_eq!(outputs.len(), 5, "portable facade target inventory drifted");

    for output in outputs {
        for family in FAMILY_VIEWS {
            for operation in facade_operations(family) {
                assert!(
                    output.source.contains(operation.rpc),
                    "{} facade omitted Rust-authority RPC {}",
                    output.language.name(),
                    operation.rpc
                );
            }
        }
    }
}

#[test]
fn every_rendered_operation_preserves_rust_policy_metadata() {
    for output in generate_remote_facades() {
        for family in FAMILY_VIEWS {
            for operation in facade_operations(family) {
                let line = rendered_operation_line(
                    &output.source,
                    operation.rpc,
                    output.language.name(),
                );
                let cancellation = cancellation_name(operation.cancellation);

                match output.language {
                    FacadeLanguage::Ruby => {
                        assert!(line.contains(&format!(
                            "client_streaming: {}, server_streaming: {}, bearer_auth: {}, cancellation: {:?}",
                            operation.client_streaming,
                            operation.server_streaming,
                            operation.bearer_auth,
                            cancellation,
                        )));
                        assert!(line.contains("capabilities:"));
                        assert!(line.contains("errors:"));
                        assert!(line.contains("validations:"));
                    }
                    FacadeLanguage::Php => {
                        assert!(line.contains(&format!(
                            "'client_streaming' => {}, 'server_streaming' => {}, 'bearer_auth' => {}, 'cancellation' => '{}'",
                            operation.client_streaming,
                            operation.server_streaming,
                            operation.bearer_auth,
                            cancellation,
                        )));
                        assert!(line.contains("'capabilities' =>"));
                        assert!(line.contains("'errors' =>"));
                        assert!(line.contains("'validations' =>"));
                    }
                    FacadeLanguage::Dart => {
                        assert!(line.contains(&format!(
                            "'clientStreaming': {}, 'serverStreaming': {}, 'bearerAuth': {}, 'cancellation': {:?}",
                            operation.client_streaming,
                            operation.server_streaming,
                            operation.bearer_auth,
                            cancellation,
                        )));
                        assert!(line.contains("'capabilities':"));
                        assert!(line.contains("'errors':"));
                        assert!(line.contains("'validations':"));
                    }
                    FacadeLanguage::Java => {
                        assert!(line.contains(&format!(
                            "new Operation({}, {}, {}, {:?})",
                            operation.client_streaming,
                            operation.server_streaming,
                            operation.bearer_auth,
                            cancellation,
                        )));
                    }
                    FacadeLanguage::Csharp => {
                        assert!(line.contains(&format!(
                            "new({}, {}, {}, {:#?})",
                            operation.client_streaming,
                            operation.server_streaming,
                            operation.bearer_auth,
                            cancellation,
                        )));
                    }
                }
            }
        }
    }
}
