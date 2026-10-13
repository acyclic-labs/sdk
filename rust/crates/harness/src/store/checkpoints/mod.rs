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
    pub(super) digest: [u8; 32],
    pub(super) chunks: Vec<ProjectionChunk>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectionChunk {
    pub(super) digest: [u8; 32],
    pub(super) bytes: u32,
}

pub(super) fn decode_projection_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(ProjectionLocation, u64)> {
    let location: ProjectionLocation = operations::decode_index(
        bytes,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &[
            "ProjectionLocation",
            "ProjectionChunk",
            "Vec",
            "u8",
            "u32",
            "u64",
            "bytes",
            "digest",
            "chunks",
        ],
        |shape, _| {
            shape
                .lexical_items()
                .checked_mul(2)
                .and_then(|slots| slots.checked_add(4))
                .and_then(|slots| slots.checked_mul(std::mem::size_of::<ProjectionChunk>() as u64))
                .and_then(|bytes| {
                    bytes.checked_add(std::mem::size_of::<ProjectionLocation>() as u64)
                })
                .ok_or_else(|| Error::Invalid("projection locator reservation overflows".into()))
        },
        |location: &ProjectionLocation| {
            (
                location.chunks.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64
                    + std::mem::size_of::<ProjectionLocation>() as u64,
                0,
            )
        },
        work,
    )?;
    let owned = if work.is_some() {
        location.chunks.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64
            + std::mem::size_of::<ProjectionLocation>() as u64
    } else {
        0
    };
    Ok((location, owned))
}

pub(super) struct LoadedProjection {
    pub(super) snapshot: Option<Snapshot>,
    pub(super) through_revision: u64,
    pub(super) consumed_bytes: u64,
    pub(super) consumed_events: u32,
}

async fn tail<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let (path, retained) = operations::publication_path(authority, "harness/v2/", None, work)?;
    let result = operations::tail_optional_with_work(client, &path, work)
        .await
        .map(|tail| tail.unwrap_or(0));
    drop(path);
    operations::release_owned_after_drop(result, retained, work)
}

fn projection_cut(
    committed_tail: u64,
    original_revision: Option<u64>,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<u64> {
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

fn charge(
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
        through_revision: projection_cut(
            tail(client, authority, work).await?,
            original_revision,
            work,
        )?,
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
    let through_revision = projection_cut(
        tail(client, authority, work).await?,
        original_revision,
        work,
    )?;
    let checkpoint_count = through_revision / u64::from(DEFAULT_PROJECTION_EVENTS);
    if checkpoint_count == 0 {
        return Ok(LoadedProjection {
            snapshot: None,
            through_revision,
            consumed_bytes: 0,
            consumed_events: 0,
        });
    }
    let (path, path_owned) =
        operations::publication_path(authority, "harness/v3/projection-checkpoints/", None, work)?;
    let mut quota = CodecQuota::from_retained(work, path_owned);
    let index = operations::one_record(client, &path, checkpoint_count - 1, quota.work()).await;
    drop(path);
    quota.relinquish(path_owned)?;
    let index = index?.ok_or_else(|| {
        super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Unsupported,
            "canonical history requires a durable projection checkpoint",
        )
    })?;
    let held = operations::sdk_record_retained_bytes(&index, quota.work());
    quota.adopt(held)?;
    let mut consumed_bytes = 0;
    charge(
        &mut consumed_bytes,
        index.value.len() as u64,
        limits.maximum_bytes,
        quota.work(),
    )?;
    let (location, held) = decode_projection_location_with_work(&index.value, quota.work())?;
    quota.adopt(held)?;
    let revision = through_revision - through_revision % u64::from(DEFAULT_PROJECTION_EVENTS);
    if location.bytes == 0
        || location.bytes > MAX_PROJECTION_BYTES
        || through_revision - revision >= u64::from(limits.maximum_events)
    {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Invalid,
            "projection checkpoint boundary exceeds read allowance",
        ));
    }
    if location.bytes > limits.maximum_bytes - consumed_bytes {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Invalid,
            "projection read exceeds byte allowance",
        ));
    }
    let snapshot = location
        .load_snapshot(
            client,
            verifier,
            &index,
            &mut consumed_bytes,
            limits.maximum_bytes,
            quota.work(),
        )
        .await?;
    Ok(LoadedProjection {
        snapshot: Some(snapshot),
        through_revision,
        consumed_bytes,
        consumed_events: 1,
    })
}

impl ProjectionLocation {
    async fn load_snapshot<P: StreamProvider>(
        &self,
        client: &std::sync::Arc<P>,
        verifier: &AuthorityVerifier,
        index: &acyclic_stream::Record,
        consumed_bytes: &mut u64,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Snapshot> {
        let authority = verifier.audience();
        let revision = (index.sequence + 1) * u64::from(DEFAULT_PROJECTION_EVENTS);
        let (path, path_owned) =
            operations::publication_path(authority, "harness/v2/", None, work)?;
        let mut quota = CodecQuota::from_retained(work, path_owned);
        let canonical = operations::one_record(client, &path, revision - 1, quota.work()).await;
        drop(path);
        quota.relinquish(path_owned)?;
        let canonical = canonical?.ok_or_else(|| {
            super::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Storage,
                "projection canonical anchor is missing",
            )
        })?;
        let held = operations::sdk_record_retained_bytes(&canonical, quota.work());
        quota.adopt(held)?;
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
        if let Some(io) = quota.work().as_deref_mut() {
            io.stage_output(self.bytes, 1)?;
        }
        let bytes = load_chunks(
            client,
            authority,
            self,
            consumed_bytes,
            maximum_bytes,
            quota.work(),
        )
        .await?;
        quota.adopt(bytes.capacity() as u64)?;
        let snapshot = Snapshot::decode(&bytes, maximum_bytes, quota.work())?;
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
        Ok(snapshot)
    }
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
    let mut total = 0_u64;
    if location.chunks.is_empty() {
        return Err(super::guarded_boundary(
            work,
            crate::wire::ErrorCode::Storage,
            "projection chunk manifest is empty",
        ));
    }
    for (index, chunk) in location.chunks.iter().enumerate() {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        if chunk.bytes == 0
            || chunk.bytes as usize > acyclic_stream::MAX_RECORD_BYTES
            || (index + 1 < location.chunks.len() && chunk.bytes < 16 * 1024)
        {
            return Err(super::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "projection chunk manifest has invalid lengths",
            ));
        }
        total = total.checked_add(u64::from(chunk.bytes)).ok_or_else(|| {
            super::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "projection chunk manifest overflows",
            )
        })?;
    }
    if total != location.bytes {
        return Err(super::guarded_boundary(
            work,
            crate::wire::ErrorCode::Storage,
            "projection chunk manifest total differs",
        ));
    }
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
        let (path, held) = operations::publication_path(
            authority,
            "harness/v3/projection-chunks/",
            Some((&digest.to_hex(), 64)),
            quota.work(),
        )?;
        let mut received = CodecQuota::from_retained(quota.work(), held);
        let record = operations::one_record(client, &path, 0, received.work()).await;
        drop(path);
        received.relinquish(held)?;
        let record = record?.ok_or_else(|| {
            super::guarded_boundary(
                received.work(),
                crate::wire::ErrorCode::Storage,
                "projection chunk is missing",
            )
        })?;
        let held = operations::sdk_record_retained_bytes(&record, received.work());
        received.adopt(held)?;
        charge(
            consumed,
            record.value.len() as u64,
            maximum,
            received.work(),
        )?;
        if record.value.len() != expected {
            return Err(super::guarded_boundary(
                received.work(),
                crate::wire::ErrorCode::Storage,
                "projection chunk length or digest differs",
            ));
        }
        if let Some(io) = received.work().as_deref_mut() {
            io.hashed_bytes(record.value.len() as u64)?;
        }
        if blake3::hash(&record.value).as_bytes() != &chunk.digest {
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
    if let Some(io) = quota.work().as_deref_mut() {
        io.hashed_bytes(bytes.len() as u64)?;
    }
    if blake3::hash(&bytes).as_bytes() != &location.digest {
        return Err(super::guarded_boundary(
            quota.work(),
            crate::wire::ErrorCode::Storage,
            "projection payload digest differs",
        ));
    }
    quota.transfer();
    Ok(bytes)
}

mod staging;
pub(super) use staging::*;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_malformed_receipt_tests;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_staging_custody_tests;
