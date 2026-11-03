//! Check the production transformer against retained Tonic ingress.

#[path = "../support/grpc_codegen.rs"]
mod oracle;

#[test]
fn original_tonic_clients_preserve_bodies_and_reject_unknown_shapes() -> std::io::Result<()> {
    let source = include_str!("../corpus/grpc_codegen/actors-native.rs");
    assert_eq!(oracle::check(source)?, Some(13));
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
    let truncated = source
        .get(..source.len() / 2)
        .ok_or_else(|| std::io::Error::other("original source midpoint is not a UTF-8 boundary"))?;
    assert!(acyclic_grpc_observability::codegen::observe_clients(truncated, "actors").is_err());
    Ok(())
}
