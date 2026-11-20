use super::OperationId;

/// Native image container recognition and every adapter preserve arbitrary bytes.
pub fn assert_image_invariants(input: &[u8]) {
    crate::projection::image::assert_roundtrip(input);
}

/// Accepted alloy manifests preserve policy and deterministic selection through JSON.
pub fn assert_alloy_invariants(input: &[u8]) {
    let result = check_alloy_invariants(input);
    assert!(result.is_ok(), "alloy invariant failed: {result:?}");
}

fn check_alloy_invariants(input: &[u8]) -> serde_json::Result<()> {
    if let Ok(spec) = serde_json::from_slice::<crate::AlloySpec>(input) {
        let encoded = serde_json::to_vec(&spec)?;
        let replay: crate::AlloySpec = serde_json::from_slice(&encoded)?;
        assert_eq!(spec, replay);
        assert_eq!(spec.validate().is_ok(), replay.validate().is_ok());
        if spec.validate().is_ok() {
            for ordinal in [0, 1, u64::MAX] {
                for role in ["", "worker", "reviewer"] {
                    let turn = crate::AlloyTurn {
                        operation_id: OperationId::from_bytes([1; 16]),
                        ordinal,
                        role,
                    };
                    assert!(spec.index(turn) < spec.models.len());
                    assert_eq!(spec.index(turn), replay.index(turn));
                }
            }
        }
    }
    Ok(())
}
