//! Bind admitted evaluation responses to the caller's complete original spec.
use acyclic_inference::{validate_customer_wire, wire};
use prost::Message;

pub(super) fn check(bytes: &[u8]) {
    let Ok(view) = wire::EvaluationView::decode(bytes) else {
        return;
    };
    let Some(spec) = &view.spec else {
        return;
    };
    if super::inference_pair(
        "evaluation_view",
        bytes,
        &view,
        &view.evaluation_id,
        &spec.encode_to_vec(),
    ) {
        assert!(validate_customer_wire("evaluation_spec", &spec.encode_to_vec(), &[], &[]).is_ok());
        let mut foreign = spec.clone();
        *foreign
            .spec_digest
            .first_mut()
            .expect("admission requires a 32-byte spec identity") ^= 1;
        assert!(
            validate_customer_wire(
                "evaluation_view",
                bytes,
                &view.evaluation_id,
                &foreign.encode_to_vec()
            )
            .is_err()
        );
    }
}

pub(super) fn seeds()
-> Result<Vec<(&'static str, &'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let spec = wire::EvaluationSpec {
        candidates: vec![wire::EvaluationArtifact {
            digest: vec![1; 32],
            media_type: "application/octet-stream".into(),
            logical_size: 1,
        }],
        suite: Some(wire::EvaluationSuite {
            identity: "codec-suite".into(),
            digest: vec![2; 32],
            cases: vec![wire::EvaluationCase {
                case_id: vec![3; 16],
                input: b"input".to_vec(),
                input_artifact_digest: None,
            }],
        }),
        grader: Some(wire::EvaluationGrader {
            handle: b"grader".to_vec(),
            artifact_digest: vec![4; 32],
        }),
        metrics: vec![wire::EvaluationMetric {
            identity: "score".into(),
            aggregation: wire::EvaluationAggregation::Mean.into(),
        }],
        maximum_case_results: 1,
        spec_digest: vec![5; 32],
    };
    let related = spec.encode_to_vec();
    let view = wire::EvaluationView {
        evaluation_id: vec![6; 16],
        spec: Some(spec),
        state: wire::EvaluationState::Admitted.into(),
        result: None,
        sequence: 1,
    };
    let encoded = view.encode_to_vec();
    validate_customer_wire("evaluation_view", &encoded, &view.evaluation_id, &related)
        .map_err(|error| format!("evaluation pair seed admission failed: {error}; recovery: review semantic::evaluation::seeds, then run cargo run --example capture"))?;
    Ok(vec![
        ("inference.customer.v1.EvaluationSpec", "admitted", related),
        ("inference.customer.v1.EvaluationView", "admitted", encoded),
    ])
}
