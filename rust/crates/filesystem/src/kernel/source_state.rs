//! Canonical durable facts for one workspace-attached native source.

#[cfg(feature = "native-watch")]
use super::codec::Encoder;
use super::codec::{CanonicalDecodeError, Decoder};
use crate::foundation::{AuthorityId, Digest, GenerationId, VolumeId};
use thiserror::Error;

const DOMAIN: &[u8] = b"acyclic-fs-source-state-v1\0";
const ID_DOMAIN: &[u8] = b"acyclic-fs-source-authority-v1\0";
const VERSION: u16 = 1;

/// Durable source advancement policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurableSourceMode {
    Pinned,
    Tracking,
}

/// Durable fail-closed source lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurableSourceState {
    Clean,
    PendingCapture,
    NeedsRescan(SourceInvalidation),
    Conflict,
    Sealed,
}

/// Portable durable reason that source continuity was lost.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceInvalidation {
    InitialSnapshotRequired,
    QueueOverflow,
    NativeRescanRequired,
    BackendError,
    UnrepresentablePath,
    AmbiguousRename,
    RootChanged,
}

/// Complete latest source fact; replaying the authority needs no side state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SourceFact {
    pub(crate) volume_id: VolumeId,
    pub(crate) root_identity: [u8; 16],
    pub(crate) mode: DurableSourceMode,
    pub(crate) maximum_paths: u32,
    pub(crate) maximum_extent_spans: u32,
    pub(crate) maximum_queued_changes: u32,
    pub(crate) state: DurableSourceState,
    pub(crate) generation_id: GenerationId,
}

/// Canonical source-fact codec failure.
#[derive(Debug, Error)]
pub(crate) enum SourceFactError {
    #[error("source bounds must be non-zero")]
    ZeroBound,
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
}

/// Derives the sole source-state authority for one workspace.
pub(crate) fn source_authority_id(volume_id: VolumeId) -> AuthorityId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ID_DOMAIN);
    hasher.update(&volume_id.into_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    AuthorityId::from_bytes(bytes)
}

#[cfg(feature = "native-watch")]
pub(crate) fn encode_source_fact(value: SourceFact) -> Result<Vec<u8>, SourceFactError> {
    validate(value)?;
    let mut encoder = Encoder::new(DOMAIN, VERSION);
    encoder.fixed(&value.volume_id.into_bytes());
    encoder.fixed(&value.root_identity);
    encoder.u8(match value.mode {
        DurableSourceMode::Pinned => 1,
        DurableSourceMode::Tracking => 2,
    });
    encoder.u32(value.maximum_paths);
    encoder.u32(value.maximum_extent_spans);
    encoder.u32(value.maximum_queued_changes);
    match value.state {
        DurableSourceState::Clean => encoder.u8(1),
        DurableSourceState::PendingCapture => encoder.u8(2),
        DurableSourceState::NeedsRescan(reason) => {
            encoder.u8(3);
            encoder.u8(reason_tag(reason));
        }
        DurableSourceState::Conflict => encoder.u8(4),
        DurableSourceState::Sealed => encoder.u8(5),
    }
    encoder.fixed(value.generation_id.digest().as_bytes());
    Ok(encoder.finish())
}

pub(crate) fn decode_source_fact(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<SourceFact, SourceFactError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_payload_bytes)?;
    let value = SourceFact {
        volume_id: VolumeId::from_bytes(decoder.fixed()?),
        root_identity: decoder.fixed()?,
        mode: match decoder.u8()? {
            1 => DurableSourceMode::Pinned,
            2 => DurableSourceMode::Tracking,
            _ => return Err(invariant("unknown source mode").into()),
        },
        maximum_paths: decoder.u32()?,
        maximum_extent_spans: decoder.u32()?,
        maximum_queued_changes: decoder.u32()?,
        state: match decoder.u8()? {
            1 => DurableSourceState::Clean,
            2 => DurableSourceState::PendingCapture,
            3 => DurableSourceState::NeedsRescan(reason_from_tag(decoder.u8()?)?),
            4 => DurableSourceState::Conflict,
            5 => DurableSourceState::Sealed,
            _ => return Err(invariant("unknown source state").into()),
        },
        generation_id: GenerationId::new(Digest::from_bytes(decoder.fixed()?)),
    };
    decoder.finish()?;
    validate(value)?;
    Ok(value)
}

pub(crate) fn decode_source_volume(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<VolumeId, SourceFactError> {
    decode_source_fact(bytes, maximum_payload_bytes).map(|fact| fact.volume_id)
}

fn validate(value: SourceFact) -> Result<(), SourceFactError> {
    if value.maximum_paths == 0
        || value.maximum_extent_spans == 0
        || value.maximum_queued_changes == 0
    {
        return Err(SourceFactError::ZeroBound);
    }
    Ok(())
}

#[cfg(feature = "native-watch")]
const fn reason_tag(reason: SourceInvalidation) -> u8 {
    match reason {
        SourceInvalidation::InitialSnapshotRequired => 1,
        SourceInvalidation::QueueOverflow => 2,
        SourceInvalidation::NativeRescanRequired => 3,
        SourceInvalidation::BackendError => 4,
        SourceInvalidation::UnrepresentablePath => 5,
        SourceInvalidation::AmbiguousRename => 6,
        SourceInvalidation::RootChanged => 7,
    }
}

fn reason_from_tag(tag: u8) -> Result<SourceInvalidation, CanonicalDecodeError> {
    match tag {
        1 => Ok(SourceInvalidation::InitialSnapshotRequired),
        2 => Ok(SourceInvalidation::QueueOverflow),
        3 => Ok(SourceInvalidation::NativeRescanRequired),
        4 => Ok(SourceInvalidation::BackendError),
        5 => Ok(SourceInvalidation::UnrepresentablePath),
        6 => Ok(SourceInvalidation::AmbiguousRename),
        7 => Ok(SourceInvalidation::RootChanged),
        _ => Err(invariant("unknown source invalidation reason")),
    }
}

fn invariant(message: &str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_fact_round_trips_and_authority_is_domain_separated()
    -> Result<(), Box<dyn std::error::Error>> {
        let volume_id = VolumeId::from_bytes([7; 16]);
        let value = SourceFact {
            volume_id,
            root_identity: [8; 16],
            mode: DurableSourceMode::Tracking,
            maximum_paths: 10,
            maximum_extent_spans: 11,
            maximum_queued_changes: 12,
            state: DurableSourceState::NeedsRescan(SourceInvalidation::RootChanged),
            generation_id: GenerationId::new(Digest::from_bytes([9; 32])),
        };
        let encoded = encode_source_fact(value)?;
        assert_eq!(decode_source_fact(&encoded, 4096)?, value);
        assert_ne!(
            source_authority_id(volume_id).into_bytes(),
            volume_id.into_bytes()
        );
        Ok(())
    }

    #[test]
    fn source_fact_rejects_zero_bounds_and_unknown_tags() {
        let value = SourceFact {
            volume_id: VolumeId::from_bytes([1; 16]),
            root_identity: [2; 16],
            mode: DurableSourceMode::Pinned,
            maximum_paths: 0,
            maximum_extent_spans: 1,
            maximum_queued_changes: 1,
            state: DurableSourceState::Clean,
            generation_id: GenerationId::new(Digest::from_bytes([3; 32])),
        };
        assert!(matches!(
            encode_source_fact(value),
            Err(SourceFactError::ZeroBound)
        ));
    }
}
