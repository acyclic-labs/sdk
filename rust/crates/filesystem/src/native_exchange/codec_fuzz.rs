//! Finite byte checks use the same admitted codec as crash recovery.
use super::*;

/// Accepted input reaches a canonical fixed point without any filesystem IO.
pub fn check(bytes: &[u8]) -> bool {
    let Ok(value) = codec::decode(bytes) else {
        return true;
    };
    let Ok(canonical) = codec::encode(&value) else {
        return false;
    };
    if !codec::decode(&canonical).is_ok_and(|repeated| repeated == value) {
        return false;
    }
    let mut trailing = canonical;
    trailing.extend_from_slice(b" false");
    if !matches!(
        codec::decode(&trailing),
        Err(NativeExchangeError::IncompatibleJournal)
    ) {
        return false;
    }
    for version in [0_u32, 2, u32::MAX] {
        // Deliberately no body: unsupported versions must settle before paths,
        // carried entries or caller recovery data can be visited.
        let header = format!("{{\"version\":{version},");
        if !matches!(codec::decode(header.as_bytes()),
            Err(NativeExchangeError::UnsupportedJournalVersion(found)) if found == version)
        {
            return false;
        }
    }
    true
}

/// Capture both durable phases through the encoder used by the disk writer.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, NativeExchangeError> {
    let mut seeds = Vec::new();
    for (name, phase) in [
        ("native-exchange-carrying", NativeExchangePhase::Carrying),
        (
            "native-exchange-exchanging",
            NativeExchangePhase::Exchanging,
        ),
    ] {
        let journal = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([1; 16]),
            recovery: vec![2, 3, 5],
            live: "live".into(),
            prepared: "prepared".into(),
            carried: vec!["retained".into(), Path::new("nested").join("entry")],
            roots: [[4; 16], [5; 16]],
            phase,
        };
        seeds.push((name.into(), codec::encode(&journal)?));
    }
    Ok(seeds)
}
