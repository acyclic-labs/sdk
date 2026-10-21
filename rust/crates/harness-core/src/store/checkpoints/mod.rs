//! Durable accelerators over the same authenticated reducer and canonical Stream.

use super::{HistoryReadLimits, history::HistoryIo, operations};

use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event, Reducer, Snapshot},
};

use acyclic_stream::{
    CommitCondition, CommitOutcome, CommittedMutation, StreamPath, StreamProvider,
};

use crate::wire_codec::CodecQuota;
use serde::{Deserialize, Serialize};

/// Default checkpoint interval and cold decoded-event allowance, including its anchor.
pub const DEFAULT_PROJECTION_EVENTS: u32 = 64;

const MAX_PROJECTION_BYTES: u64 = 16 * acyclic_stream::MAX_COMMAND_BYTES as u64;
const MIN_PROJECTION_CHUNK_BYTES: usize = 16 * 1024;

/// Default finite cold input allowance; includes pointer, snapshot, anchor and suffix.
#[must_use]
pub const fn default_projection_read_limits() -> HistoryReadLimits {
    HistoryReadLimits {
        maximum_events: DEFAULT_PROJECTION_EVENTS,
        maximum_bytes: 32 * acyclic_stream::MAX_COMMAND_BYTES as u64,
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectionLocation {
    pub(super) bytes: u64,
    pub(super) chunks: Vec<ProjectionChunk>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectionChunk {
    digest: [u8; 32],
    bytes: u32,
}

pub(super) struct LoadedProjection {
    pub(super) snapshot: Option<Snapshot>,
    pub(super) through_revision: u64,
    pub(super) consumed_bytes: u64,
    pub(super) consumed_events: u32,
}

async fn projection_cut<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    original_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let (path, retained) = operations::publication_path(authority, "harness/v2/", None, work)?;
    let result = operations::tail_optional_with_work(client, &path, work).await;
    drop(path);
    let committed_tail = operations::release_owned_after_drop(result, retained, work)?.unwrap_or(0);
    let through_revision = original_revision.unwrap_or(committed_tail);
    if through_revision > committed_tail {
        return Err(super::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "projection revision exceeds committed tail",
        ));
    }
    Ok(through_revision)
}

pub(in crate::store) fn charge(
    used: &mut u64,
    bytes: u64,
    maximum: u64,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<()> {
    *used = used
        .checked_add(bytes)
        .filter(|total| *total <= maximum)
        .ok_or_else(|| {
            super::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "projection read exceeds byte allowance",
            )
        })?;
    Ok(())
}

pub(super) async fn supplied<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    snapshot: Snapshot,
    limits: HistoryReadLimits,
    original_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<LoadedProjection> {
    if &snapshot.authority != authority {
        return Err(Error::Invalid(
            "snapshot authority does not match requested aggregate".into(),
        ));
    }
    let consumed_events = u32::try_from(snapshot.events.len())
        .ok()
        .filter(|count| *count <= limits.maximum_events)
        .ok_or_else(|| Error::Invalid("snapshot cache exceeds event allowance".into()))?;
    let consumed_bytes =
        crate::executor::json::json_byte_length(&snapshot, limits.maximum_bytes, &mut None)?;
    Ok(LoadedProjection {
        snapshot: Some(snapshot),
        through_revision: projection_cut(client, authority, original_revision, work).await?,
        consumed_bytes,
        consumed_events,
    })
}

pub(super) async fn load_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    limits: HistoryReadLimits,
    original_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<LoadedProjection> {
    verifier.verify_audience(authority, work)?;
    if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
        return Err(super::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "projection read bounds must be positive",
        ));
    }
    let through_revision = projection_cut(client, authority, original_revision, work).await?;
    let checkpoint_count = through_revision / u64::from(DEFAULT_PROJECTION_EVENTS);
    if checkpoint_count == 0 {
        return Ok(LoadedProjection {
            snapshot: None,
            through_revision,
            consumed_bytes: 0,
            consumed_events: 0,
        });
    }
    let mut quota = CodecQuota::from_retained(work, 0);
    let path = operations::publication_path(
        authority,
        "harness/v3/projection-checkpoints/",
        None,
        quota.work(),
    )?;
    let index = operations::publication_record(client, path, checkpoint_count - 1, &mut quota)
        .await?
        .ok_or_else(|| {
            super::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unsupported,
                "canonical history requires a durable projection checkpoint",
            )
        })?;
    let mut consumed_bytes = 0;
    charge(
        &mut consumed_bytes,
        index.value.len() as u64,
        limits.maximum_bytes,
        quota.work(),
    )?;
    let location = decode_projection_location(&index.value, &mut quota)?;
    let revision = validate_projection_bounds(
        &location,
        through_revision,
        limits,
        consumed_bytes,
        &mut quota,
    )?;
    let event = verify_projection_anchor(
        client,
        authority,
        verifier,
        revision,
        &index,
        (&mut consumed_bytes, limits.maximum_bytes),
        &mut quota,
    )
    .await?;
    if let Some(io) = quota.work().as_deref_mut() {
        io.stage_output(location.bytes, 1)?;
    }
    let bytes = load_chunks(
        client,
        authority,
        &location,
        &mut consumed_bytes,
        limits.maximum_bytes,
        quota.work(),
    )
    .await?;
    quota.adopt(bytes.capacity() as u64)?;
    let snapshot = Snapshot::decode(&bytes, limits.maximum_bytes, quota.work())?;
    if &snapshot.authority != authority
        || snapshot.revision != revision
        || snapshot.events.last() != Some(&event)
    {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "projection snapshot differs from its canonical anchor",
        ));
    }
    if let Some(io) = quota.work().as_deref() {
        io.check_cancellation()?;
    }
    Ok(LoadedProjection {
        snapshot: Some(snapshot),
        through_revision,
        consumed_bytes,
        consumed_events: 1,
    })
}

async fn load_chunks<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    location: &ProjectionLocation,
    consumed: &mut u64,
    maximum: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let length = usize::try_from(location.bytes).map_err(|_| {
        super::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "projection size exceeds platform",
        )
    })?;
    let mut quota = CodecQuota::from_retained(work, 0);
    let mut bytes = crate::core::reserve_copy_vector(length, &mut quota)?;
    for chunk in &location.chunks {
        let expected = chunk.bytes as usize;
        if expected as u64 > maximum.saturating_sub(*consumed) {
            return Err(super::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "projection read exceeds byte allowance",
            ));
        }
        let digest = blake3::Hash::from_bytes(chunk.digest);
        let path = operations::publication_path(
            authority,
            "harness/v3/projection-chunks/",
            Some(&digest.to_hex()),
            quota.work(),
        )?;
        let mut received = CodecQuota::from_retained(quota.work(), 0);
        let record = operations::publication_record(client, path, 0, &mut received)
            .await?
            .ok_or_else(|| {
                super::guarded_boundary(
                    received.work(),
                    crate::wire::ErrorCode::Storage,
                    "projection chunk is missing",
                )
            })?;
        charge(
            consumed,
            record.value.len() as u64,
            maximum,
            received.work(),
        )?;
        if record.value.len() != expected || {
            if let Some(io) = received.work().as_deref_mut() {
                io.hashed_bytes(record.value.len() as u64)?;
            }
            blake3::hash(&record.value).as_bytes() != &chunk.digest
        } {
            return Err(super::guarded_boundary(
                received.work(),
                crate::wire::ErrorCode::Storage,
                "projection chunk length or digest differs",
            ));
        }
        if let Some(io) = received.work().as_deref_mut() {
            io.copy_bytes(record.value.len() as u64)?;
        }
        bytes.extend_from_slice(&record.value);
    }
    quota.transfer();
    Ok(bytes)
}

mod staging;
pub(super) use staging::*;
#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_malformed_receipt_tests;
#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::store) mod native_staging_custody_tests;

fn decode_projection_location(
    index: &[u8],
    quota: &mut CodecQuota<'_, '_>,
) -> Result<ProjectionLocation> {
    let location: ProjectionLocation = operations::decode_index(
        index,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        "ProjectionLocation ProjectionChunk Vec u8 u32 u64 bytes chunks digest",
        |shape, _| {
            shape
                .lexical_items()
                .checked_mul(64)
                .and_then(|bytes| {
                    bytes.checked_add(128 + std::mem::size_of::<ProjectionLocation>() as u64)
                })
                .ok_or_else(|| Error::Invalid("projection locator reservation overflows".into()))
        },
        |location: &ProjectionLocation| {
            (
                (location.chunks.capacity() * std::mem::size_of::<ProjectionChunk>()) as u64
                    + std::mem::size_of::<ProjectionLocation>() as u64,
                0,
            )
        },
        quota.work(),
    )?;
    quota.adopt(
        (location.chunks.capacity() * std::mem::size_of::<ProjectionChunk>()) as u64
            + std::mem::size_of::<ProjectionLocation>() as u64,
    )?;
    Ok(location)
}

fn validate_projection_bounds(
    location: &ProjectionLocation,
    through_revision: u64,
    limits: HistoryReadLimits,
    consumed_bytes: u64,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<u64> {
    let revision = through_revision - through_revision % u64::from(DEFAULT_PROJECTION_EVENTS);
    if location.bytes == 0
        || location.bytes > MAX_PROJECTION_BYTES
        || through_revision - revision >= u64::from(limits.maximum_events)
        || location.bytes > limits.maximum_bytes - consumed_bytes
    {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Invalid,
            "projection checkpoint boundary exceeds read allowance",
        ));
    }
    let mut total = 0_u64;
    if location.chunks.is_empty() {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "projection chunk manifest is empty",
        ));
    }
    if let Some(io) = quota.work().as_deref_mut() {
        io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
            items: location.chunks.len() as u64,
        })?;
    }
    for (index, chunk) in location.chunks.iter().enumerate() {
        if chunk.bytes == 0
            || chunk.bytes as usize > acyclic_stream::MAX_RECORD_BYTES
            || (index + 1 < location.chunks.len()
                && (chunk.bytes as usize) < MIN_PROJECTION_CHUNK_BYTES)
        {
            return Err(super::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
                "projection chunk manifest has invalid lengths",
            ));
        }
        total = total.checked_add(u64::from(chunk.bytes)).ok_or_else(|| {
            super::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
                "projection chunk manifest overflows",
            )
        })?;
    }
    if total != location.bytes {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "projection chunk manifest total differs",
        ));
    }
    Ok(revision)
}

async fn verify_projection_anchor<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    revision: u64,
    index: &operations::ObservedRecord,
    bounds: (&mut u64, u64),
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Event> {
    let (consumed_bytes, maximum_bytes) = bounds;
    let path = operations::publication_path(authority, "harness/v2/", None, quota.work())?;
    let canonical = operations::publication_record(client, path, revision - 1, quota)
        .await?
        .ok_or_else(|| {
            super::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
                "projection canonical anchor is missing",
            )
        })?;
    charge(
        consumed_bytes,
        canonical.value.len() as u64,
        maximum_bytes,
        quota.work(),
    )?;
    if canonical.commit_id != index.commit_id {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "projection head is outside its canonical atomic commit",
        ));
    }
    let (event, held) = super::history::verify_history_record_with_retained_quota(
        verifier,
        authority,
        revision - 1,
        &canonical,
        quota.work(),
    )?;
    quota.adopt(held)?;
    Ok(event)
}
