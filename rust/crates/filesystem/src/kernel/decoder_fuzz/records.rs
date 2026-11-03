//! Fixed durable records share the page oracle without inventing copy receipts.
use super::super::CanonicalDecodeError;
use super::super::source_state::*;
use crate::{Digest, GenerationId, VolumeId};

/// Require original writer admission, complete framing and canonical identity.
pub(crate) fn record<T: PartialEq + std::fmt::Debug, E: std::fmt::Debug>(
    input: &[u8],
    expected: &T,
    encoded: Result<Vec<u8>, E>,
    decode: impl Fn(&[u8]) -> Result<T, E>,
    encode: impl Fn(&T) -> Result<Vec<u8>, E>,
) -> Vec<u8> {
    let bytes = super::canonical::<_, _, false>(expected, encoded, &decode);
    for end in [0, bytes.len() / 2, bytes.len() - 1] {
        assert!(
            decode(&bytes[..end]).is_err(),
            "truncated filesystem record admitted"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(
        decode(&trailing).is_err(),
        "trailing filesystem record bytes admitted"
    );
    if let Ok(value) = decode(input) {
        assert_eq!(
            super::canonical::<_, _, false>(&value, encode(&value), &decode),
            input,
            "filesystem record changed its original bytes"
        );
    }
    bytes
}

fn fact(input: &[u8], selector: u8) -> SourceFact {
    let mut identity = [0; 32];
    let length = input.len().min(identity.len());
    identity[..length].copy_from_slice(&input[..length]);
    let reasons = [
        SourceInvalidation::InitialSnapshotRequired,
        SourceInvalidation::QueueOverflow,
        SourceInvalidation::NativeRescanRequired,
        SourceInvalidation::BackendError,
        SourceInvalidation::UnrepresentablePath,
        SourceInvalidation::AmbiguousRename,
        SourceInvalidation::RootChanged,
    ];
    SourceFact {
        volume_id: VolumeId::from_bytes(identity[..16].try_into().unwrap()),
        root_identity: identity[16..].try_into().unwrap(),
        mode: if selector & 1 == 0 {
            DurableSourceMode::Pinned
        } else {
            DurableSourceMode::Tracking
        },
        maximum_paths: u32::from_le_bytes(identity[..4].try_into().unwrap()).max(1),
        maximum_extent_spans: u32::from_le_bytes(identity[4..8].try_into().unwrap()).max(1),
        maximum_queued_changes: u32::from_le_bytes(identity[8..12].try_into().unwrap()).max(1),
        capture_policy: Digest::from_bytes(identity),
        state: match (selector / 2) % 11 {
            0 => DurableSourceState::Clean,
            1 => DurableSourceState::PendingCapture,
            2 => DurableSourceState::Conflict,
            3 => DurableSourceState::Sealed,
            index => DurableSourceState::NeedsRescan(reasons[usize::from(index - 4)]),
        },
        generation_id: GenerationId::new(Digest::from_bytes(identity)),
    }
}

/// Exercise the production source decoder and, when enabled, stream records.
pub fn check(input: &[u8]) {
    let value = fact(input, input.first().copied().unwrap_or(0));
    let bytes = record(
        input,
        &value,
        encode_source_fact(value),
        |bytes| decode_source_fact(bytes, 4096),
        |value| encode_source_fact(*value),
    );
    let domain = DOMAIN.len();
    for version in [0_u16, 2, u16::MAX] {
        let mut changed = bytes.clone();
        changed[domain..domain + 2].copy_from_slice(&version.to_le_bytes());
        assert!(
            matches!(decode_source_fact(&changed, 4096), Err(SourceFactError::Decode(CanonicalDecodeError::UnsupportedVersion {found, expected: 1})) if found == version),
            "source version admitted"
        );
    }
    for offset in [domain + 2 + 32, domain + 2 + 32 + 1 + 12 + 32] {
        let mut changed = bytes.clone();
        changed[offset] = 255;
        assert!(
            decode_source_fact(&changed, 4096).is_err(),
            "unknown source tag admitted"
        );
    }
    for bound in 0..3 {
        let mut invalid = value;
        match bound {
            0 => invalid.maximum_paths = 0,
            1 => invalid.maximum_extent_spans = 0,
            _ => invalid.maximum_queued_changes = 0,
        }
        assert!(matches!(
            encode_source_fact(invalid),
            Err(SourceFactError::ZeroBound)
        ));
        let mut changed = bytes.clone();
        let offset = domain + 2 + 32 + 1 + bound * 4;
        changed[offset..offset + 4].fill(0);
        assert!(
            matches!(
                decode_source_fact(&changed, 4096),
                Err(SourceFactError::ZeroBound)
            ),
            "zero source bound admitted"
        );
    }
    if matches!(value.state, DurableSourceState::NeedsRescan(_)) {
        let mut changed = bytes;
        changed[domain + 2 + 32 + 1 + 12 + 32 + 1] = 255;
        assert!(
            decode_source_fact(&changed, 4096).is_err(),
            "unknown source reason admitted"
        );
    }
    #[cfg(feature = "distributed")]
    crate::distributed::record_codec_fuzz::check(input);
}

/// Original library writer output; these are authored values, not native captures.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
    let mut seeds = Vec::new();
    for selector in 0..22 {
        seeds.push((format!("source-fact-{selector:02}"), {
            let value = fact(&[selector; 32], selector);
            record(
                &[],
                &value,
                encode_source_fact(value),
                |bytes| decode_source_fact(bytes, 4096),
                |value| encode_source_fact(*value),
            )
        }));
    }
    #[cfg(feature = "distributed")]
    seeds.extend(crate::distributed::record_codec_fuzz::seeds()?);
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    #[test]
    fn original_writer_records_and_arbitrary_bytes_share_the_oracle() {
        for (_, bytes) in super::seeds().unwrap() {
            super::check(&bytes);
        }
        for length in 0..64 {
            super::check(&vec![length as u8; length]);
        }
    }
}
