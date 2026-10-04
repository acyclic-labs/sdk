use acyclic_sdk_contract_wire::{
    facade_operations, generate_remote_facades,
    family_registry::FAMILY_VIEWS,
};

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
