use super::*;

pub(in crate::filesystem::execution_journal) fn check(bytes: &[u8]) {
    if let Ok(value) = codec::decode(bytes, None) {
        // This is metadata syntax only. Signature, volume, operation and
        // original-turn admission remain in verify_checkpoint.
        let encoded = codec::encode(&value, None).expect("admitted checkpoint encodes");
        assert_eq!(encoded.as_ref(), bytes);
        for version in [b'0', b'3', b'5', u8::MAX] {
            let mut header = bytes[..4].to_vec();
            header[3] = version;
            assert_eq!(
                codec::decode(&header, None).err().unwrap().code(),
                crate::wire::ErrorCode::Unsupported
            );
        }
        assert_ne!(
            codec::decode(&bytes[..4], None).err().unwrap().code(),
            crate::wire::ErrorCode::Unsupported
        );
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(codec::decode(&trailing, None).is_err());
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
            checkpoint.state.model_dispatched = true;
        }
        let bytes = codec::encode(&checkpoint, None)?;
        codec::decode(&bytes, None)?;
        seeds.push((name, bytes.to_vec()));
    }
    Ok(seeds)
}
