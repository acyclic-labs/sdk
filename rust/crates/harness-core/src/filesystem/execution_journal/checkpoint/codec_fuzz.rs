use super::*;

pub(in crate::filesystem::execution_journal) fn check(bytes: &[u8]) {
    if let Ok(value) = codec::decode(bytes, None) {
        // This is metadata syntax only. Signature, volume, operation and
        // original-turn admission remain in verify_checkpoint.
        let encoded = codec::encode(&value, None);
        assert!(
            matches!(&encoded, Ok(encoded) if encoded.as_ref() == bytes),
            "checkpoint round trip failed: {encoded:?}"
        );
        let result = crate::filesystem::execution_journal::codec_fuzz::check_versioned_frame(
            bytes,
            [b'0', b'3', b'4', b'5', b'6', b'7', b'8', u8::MAX],
            |frame| codec::decode(frame, None),
        );
        assert!(result.is_ok(), "checkpoint framing failed: {result:?}");
    }
}

pub(in crate::filesystem::execution_journal) fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>> {
    let mut checkpoint = Checkpoint {
        state: IndexState {
            operation: OperationId::from_bytes([1; 16]),
            turn: OperationId::from_bytes([2; 16]),
            volume: [3; 32],
            tail: u64::MAX,
            nodes_tail: u64::MAX,
            roots: [None; 5],
            pending_dispatches: u64::MAX,
            anchor: [4; 32],
            latest_response: None,
            latest_response_output: false,
            latest_response_completed: false,
            model_dispatched: false,
        },
        proof: [5; 64],
    };
    let mut seeds = Vec::new();
    for (name, present) in [
        ("execution-checkpoint-empty", false),
        ("execution-checkpoint-roots", true),
    ] {
        if present {
            checkpoint.state.roots = [Some(NodeRef {
                sequence: u64::MAX - 1,
                digest: [6; 32],
            }); 5];
            checkpoint.state.latest_response = Some(u32::MAX);
            checkpoint.state.latest_response_output = true;
            checkpoint.state.latest_response_completed = true;
            checkpoint.state.model_dispatched = true;
        }
        let bytes = codec::encode(&checkpoint, None)?;
        codec::decode(&bytes, None)?;
        seeds.push((name, bytes.to_vec()));
    }
    Ok(seeds)
}
