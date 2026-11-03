#[path = "../support/grpc_codegen.rs"]
mod oracle;

#[test]
fn original_tonic_clients_preserve_bodies_and_reject_unknown_shapes() {
    let source = include_str!("../corpus/grpc_codegen/actors-native.rs");
    assert_eq!(oracle::check(source), Some(13));
    for (before, after) in [
        (".unary(", ".streaming("),
        ("GrpcMethod::new", "Other::new"),
    ] {
        assert!(source.contains(before));
        assert!(
            acyclic_grpc_observability::codegen::observe_clients(
                &source.replace(before, after),
                "actors"
            )
            .is_err()
        );
    }
    assert!(
        acyclic_grpc_observability::codegen::observe_clients(&source[..source.len() / 2], "actors")
            .is_err()
    );
}
