//! Canonical durable facts for one workspace-attached native source.

#[cfg(any(
    test,
    feature = "native-watch",
    feature = "test-support",
    feature = "fuzzing"
))]
use super::codec::Encoder;
use super::codec::{CanonicalDecodeError, Decoder};
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
use crate::foundation::AuthorityId;
use crate::foundation::{Digest, GenerationId, VolumeId};
use thiserror::Error;

pub(super) const DOMAIN: &[u8] = b"acyclic-fs-source-state-v1\0";
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
const ID_DOMAIN: &[u8] = b"acyclic-fs-source-authority-v1\0";
const VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurableSourceMode {
    Pinned,
    Tracking,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurableSourceState {
    Clean,
    PendingCapture,
    NeedsRescan(SourceInvalidation),
    Conflict,
    Sealed,
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SourceFact {
    pub(crate) volume_id: VolumeId,
    pub(crate) root_identity: [u8; 16],
    pub(crate) mode: DurableSourceMode,
    pub(crate) maximum_paths: u32,
    pub(crate) maximum_extent_spans: u32,
    pub(crate) maximum_queued_changes: u32,
    pub(crate) capture_policy: Digest,
    pub(crate) state: DurableSourceState,
    pub(crate) generation_id: GenerationId,
}

#[derive(Debug, Error)]
pub(crate) enum SourceFactError {
    #[error("source bounds must be non-zero")]
    ZeroBound,
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) fn source_authority_id(volume_id: VolumeId) -> AuthorityId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ID_DOMAIN);
    hasher.update(&volume_id.into_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    AuthorityId::from_bytes(bytes)
}

#[cfg(any(
    test,
    feature = "native-watch",
    feature = "test-support",
    feature = "fuzzing"
))]
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
    encoder.fixed(value.capture_policy.as_bytes());
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
    encoder.object(value.generation_id.object_id());
    Ok(encoder.finish())
}

pub(crate) fn decode_source_fact(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<SourceFact, SourceFactError> {
    let mut decoder = Decoder::new(bytes, DOMAIN, VERSION, maximum_payload_bytes, &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })?;
    let value = SourceFact {
        volume_id: VolumeId::from_bytes(
            decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
        ),
        root_identity: decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
        mode: match decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))? {
            1 => DurableSourceMode::Pinned,
            2 => DurableSourceMode::Tracking,
            _ => return Err(invariant("unknown source mode").into()),
        },
        maximum_paths: decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
        maximum_extent_spans: decoder
            .u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
        maximum_queued_changes: decoder
            .u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
        capture_policy: Digest::from_bytes(
            decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
        ),
        state: match decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))? {
            1 => DurableSourceState::Clean,
            2 => DurableSourceState::PendingCapture,
            3 => DurableSourceState::NeedsRescan(reason_from_tag(
                decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
            )?),
            4 => DurableSourceState::Conflict,
            5 => DurableSourceState::Sealed,
            _ => return Err(invariant("unknown source state").into()),
        },
        generation_id: GenerationId::from_root(
            decoder.object(crate::ObjectKind::GenerationRoot, &mut |_| {
                Ok::<_, crate::kernel::CanonicalDecodeError>(())
            })?,
        ),
    };
    decoder.finish()?;
    validate(value)?;
    Ok(value)
}

#[cfg(all(
    any(test, feature = "local"),
    feature = "native-watch",
    not(target_arch = "wasm32")
))]
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

#[cfg(any(
    test,
    feature = "native-watch",
    feature = "test-support",
    feature = "fuzzing"
))]
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

fn invariant(message: &'static str) -> CanonicalDecodeError {
    CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(message))
}

#[cfg(all(test, feature = "native-watch", not(target_arch = "wasm32")))]
mod tests {
    #[test]
    fn source_authority_is_domain_separated() {
        let volume = super::VolumeId::from_bytes([7; 16]);
        assert_ne!(
            super::source_authority_id(volume).into_bytes(),
            volume.into_bytes()
        );
    }
}
