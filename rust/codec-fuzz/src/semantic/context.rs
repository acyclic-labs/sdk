//! Positive encoder producers for every Context provenance alternative.
use acyclic_inference::{validate_customer_wire, wire};
use prost::Message;

pub(super) fn seeds()
-> Result<Vec<(&'static str, &'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    use wire::context_provenance::Origin;
    let receipt = wire::MutationReceipt {
        revision: vec![1; 32],
        command_digest: vec![2; 32],
        sequence: 1,
        retained: true,
    }
    .encode_to_vec();
    validate_customer_wire("mutation_receipt", &receipt, &[], &[])
        .map_err(|error| format!("mutation receipt seed admission failed: {error}; recovery: review semantic::context::seeds, then run cargo run --example capture"))?;
    let mut outputs = vec![(
        "inference.customer.v1.MutationReceipt",
        "published",
        receipt,
    )];
    for (variant, origin) in [
        ("created", Origin::Created(wire::Empty {})),
        (
            "derived",
            Origin::Derived(wire::ProvenanceSource {
                source: vec![3; 32],
            }),
        ),
        (
            "forked",
            Origin::Forked(wire::ProvenanceSource {
                source: vec![3; 32],
            }),
        ),
        (
            "transferred",
            Origin::Transferred(wire::TransferProvenance {
                source: vec![3; 32],
                reused_compatible_state: true,
            }),
        ),
        (
            "generated",
            Origin::Generated(wire::GenerationProvenance {
                run_id: vec![4; 16],
                terminal_receipt_digest: vec![5; 32],
            }),
        ),
        (
            "run-input",
            Origin::RunInput(wire::RunInputProvenance {
                source: vec![3; 32],
                run_id: vec![4; 16],
                maximum_output: 1,
                seed: Some(u64::MAX),
            }),
        ),
    ] {
        let view = wire::ContextView {
            revision: vec![1; 32],
            parent: Some(vec![3; 32]),
            lineage: vec![6; 32],
            execution_profile: vec![7; 32],
            content_digest: vec![8; 32],
            items: Vec::new(),
            model: "model".into(),
            provenance: Some(wire::ContextProvenance {
                origin: Some(origin),
            }),
        };
        let encoded = view.encode_to_vec();
        validate_customer_wire("context_view", &encoded, &view.revision, &[])
            .map_err(|error| format!("Context {variant} seed admission failed: {error}; recovery: review semantic::context::seeds, then run cargo run --example capture"))?;
        outputs.push(("inference.customer.v1.ContextView", variant, encoded));
    }
    Ok(outputs)
}
