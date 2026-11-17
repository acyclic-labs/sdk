//! The native adapter, seed capture and fuzzing use the same image writers.
use super::*;

/// Check canonical fields, typed version ordering and full metadata round trips.
pub fn check(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if check_inner(bytes) {
        Ok(())
    } else {
        Err("filesystem oracle invariant failed".into())
    }
}

fn check_inner(bytes: &[u8]) -> bool {
    let cancellation = crate::CancellationToken::new();
    let Ok(work) = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
    else {
        return false;
    };
    let Ok(baseline) = work.live_allocation_bytes() else {
        return false;
    };
    if let Ok(witness) = decode_native_witness(bytes) {
        if !(&(encode_witness_header(witness.fingerprints)) == &(bytes[..67])) {
            return false;
        };
        if let Ok(metadata) = witness.metadata() {
            let Ok(encoded) = encode_native_witness(
                witness.fingerprints[0],
                witness.fingerprints[1],
                metadata,
                &work,
            ) else {
                return false;
            };
            let Ok(restored) = decode_native_witness(&encoded.0) else {
                return false;
            };
            if !(&(restored.fingerprints) == &(witness.fingerprints)) {
                return false;
            };
            if !(restored
                .metadata()
                .is_ok_and(|restored| restored == metadata))
            {
                return false;
            };
        }
    }
    if let Ok(value) = decode_native_metadata(bytes) {
        let Ok(encoded) = encode_native_metadata(&value, &work) else {
            return false;
        };
        if !(decode_native_metadata(&encoded.0).is_ok_and(|decoded| decoded == value)) {
            return false;
        };
        let bare = &encoded.0[METADATA_DOMAIN.len() + METADATA_VERSION.len()..];
        if !(matches!(
            decode_native_metadata(bare),
            Err(NativeTreeMaterializationError::UnsupportedFormat)
        )) {
            return false;
        };
        let mut trailing = encoded.0.clone();
        trailing.extend_from_slice(b" false");
        if !(matches!(
            decode_native_metadata(&trailing),
            Err(NativeTreeMaterializationError::InvalidLayout)
        )) {
            return false;
        };
    }
    for version in [0_u8, 1, 3, u8::MAX] {
        if !(matches!(
            decode_native_witness(&[version]),
            Err(NativeTreeMaterializationError::UnsupportedFormat)
        )) {
            return false;
        };
    }
    for version in [b'0', b'2', u8::MAX] {
        let mut header = METADATA_DOMAIN.to_vec();
        header.extend_from_slice(&[version, 0]);
        if !(matches!(
            decode_native_metadata(&header),
            Err(NativeTreeMaterializationError::UnsupportedFormat)
        )) {
            return false;
        };
    }
    let mut header = METADATA_DOMAIN.to_vec();
    header.extend_from_slice(METADATA_VERSION);
    if !(matches!(
        decode_native_metadata(&header),
        Err(NativeTreeMaterializationError::InvalidLayout)
    )) {
        return false;
    };
    // A missing fingerprint has exactly one encoding, including its zero tail.
    let mut noncanonical = encode_witness_header([None, None]);
    noncanonical[2] = 1;
    if !(matches!(
        decode_native_witness(&noncanonical),
        Err(NativeTreeMaterializationError::InvalidLayout)
    )) {
        return false;
    };
    if !(&(work.live_allocation_bytes()) == &(Ok(baseline))) {
        return false;
    };
    true
}

/// Capture optional fingerprint combinations, preimage metadata and scalar edges.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, NativeTreeMaterializationError> {
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = work.live_allocation_bytes()?;
    let metadata = crate::WorkspaceMetadata::from_engine(crate::kernel::FileMetadata::default());
    let mut seeds = Vec::new();
    for (tag, fingerprints) in [
        [None, None],
        [None, Some([1; 32])],
        [Some([2; 32]), None],
        [Some([2; 32]), Some([1; 32])],
    ]
    .into_iter()
    .enumerate()
    {
        for present in [false, true] {
            let encoded = encode_native_witness(
                fingerprints[0],
                fingerprints[1],
                present.then_some(metadata),
                &work,
            )?;
            seeds.push((format!("native-witness-{tag}-{present}"), encoded.0.clone()));
            drop(encoded);
        }
    }
    let edges = crate::WorkspaceMetadata {
        posix_mode: Some(u32::MAX),
        posix_uid: Some(u32::MAX),
        posix_gid: Some(u32::MAX),
        posix_flags: Some(u64::MAX),
        windows_attributes: Some(u32::MAX),
        created_ns: Some(i64::MIN),
        modified_ns: Some(i64::MAX),
        accessed_ns: Some(i64::MIN),
        changed_ns: Some(i64::MAX),
        has_named_attributes: true,
        has_acl: true,
        has_security_descriptor: true,
    };
    for (tag, value) in [("absent", metadata), ("edges", edges)] {
        let encoded = encode_native_metadata(&value, &work)?;
        seeds.push((format!("native-metadata-{tag}"), encoded.0.clone()));
        drop(encoded);
    }
    if !(&(work.live_allocation_bytes()) == &(Ok(baseline))) {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    };
    Ok(seeds)
}
