//! Exercise the exact codec used by the original physical fence adapter.
use super::*;

fn decode(bytes: &[u8]) -> Result<NativeOwnershipReceipt, NativeTreeMaterializationError> {
    let mut hashes = 0;
    let result = NativeOwnershipReceipt::decode(bytes, || {
        hashes += 1;
        Ok(())
    });
    assert!(hashes <= 1);
    if bytes.len() != RECEIPT_BYTES || !bytes.starts_with(b"ACYPFSROOT000003") {
        assert_eq!(hashes, 0, "unadmitted receipt reached hashing");
    }
    result
}

/// Canonical bytes, physical binding, checksum and refusal ordering controls.
pub fn check(bytes: &[u8]) -> bool {
    if let Ok(value) = decode(bytes) {
        assert_eq!(value.encode().as_slice(), bytes);
        assert!(value.binds(value.fence, value.parent));
        let mut other = value.fence.to_bytes();
        other[0] ^= 1;
        assert!(!value.binds(NativeRootIdentity::from_bytes(other), value.parent));
        let mut other = value.parent.to_bytes();
        other[0] ^= 1;
        assert!(!value.binds(value.fence, NativeRootIdentity::from_bytes(other)));
    }
    let original = sample().encode();
    assert_eq!(decode(&original).unwrap(), sample());
    for index in [16, 32, 48, 64, 80, 84, 92] {
        let mut corrupt = original;
        corrupt[index] ^= 1;
        assert!(matches!(
            decode(&corrupt),
            Err(NativeTreeMaterializationError::InvalidLayout)
        ));
    }
    assert!(matches!(
        decode(&original[..RECEIPT_BYTES - 1]),
        Err(NativeTreeMaterializationError::InvalidLayout)
    ));
    let mut trailing = original.to_vec();
    trailing.push(0);
    assert!(matches!(
        decode(&trailing),
        Err(NativeTreeMaterializationError::InvalidLayout)
    ));
    assert!(matches!(
        NativeOwnershipReceipt::decode(&original, || Err(
            NativeTreeMaterializationError::Cancelled
        )),
        Err(NativeTreeMaterializationError::Cancelled)
    ));
    for version in [b"000000", b"000001", b"000002", b"000004", b"999999"] {
        let mut unsupported = original;
        unsupported[10..16].copy_from_slice(version);
        for candidate in [&unsupported[..16], &unsupported[..]] {
            assert!(matches!(
                decode(candidate),
                Err(NativeTreeMaterializationError::UnsupportedFormat)
            ));
        }
    }
    true
}

fn sample() -> NativeOwnershipReceipt {
    NativeOwnershipReceipt {
        root: NativeRootIdentity::from_bytes([1; 16]),
        fence: NativeRootIdentity::from_bytes([2; 16]),
        parent: NativeRootIdentity::from_bytes([3; 16]),
        volume: VolumeId::from_bytes([4; 16]),
        pid: 5,
        started: 6,
    }
}

/// Capture production encoder output; physical identities remain test values.
pub fn seeds() -> Vec<(&'static str, Vec<u8>)> {
    let edge = NativeOwnershipReceipt {
        root: NativeRootIdentity::from_bytes([0; 16]),
        fence: NativeRootIdentity::from_bytes([u8::MAX; 16]),
        parent: NativeRootIdentity::from_bytes([0; 16]),
        volume: VolumeId::from_bytes([u8::MAX; 16]),
        pid: u32::MAX,
        started: u64::MAX,
    };
    vec![
        ("native-ownership-receipt", sample().encode().to_vec()),
        ("native-ownership-receipt-edges", edge.encode().to_vec()),
    ]
}
