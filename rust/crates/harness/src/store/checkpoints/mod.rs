//! Durable accelerators over the same authenticated reducer and canonical Stream.

use super::{HistoryReadLimits, history::HistoryIo, operations};

use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event, Reducer, Snapshot},
};

use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, CommittedMutation,
    IdempotencyKey, OwnedBytes, StreamError, StreamPath, StreamProvider,
};

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
    pub(super) authority: Authority,
    pub(super) revision: u64,
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
            "Authority",
            "AggregateKind",
            "String",
            "Vec",
            "u8",
            "u32",
            "u64",
            "authority",
            "revision",
            "bytes",
            "digest",
            "chunks",
            "kind",
            "id",
            "agent",
            "conversation",
            "session",
            "turn",
            "task",
        ],
        |shape, observer| {
            observer.admit_copy(shape.input_bytes())?;
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
                location.authority.id.capacity() as u64
                    + location.chunks.capacity() as u64
                        * std::mem::size_of::<ProjectionChunk>() as u64
                    + std::mem::size_of::<ProjectionLocation>() as u64,
                location.authority.id.len() as u64,
            )
        },
        work,
    )?;
    let owned = if work.is_some() {
        location.authority.id.capacity() as u64
            + location.chunks.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64
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

fn suffix(authority: &Authority) -> Result<String> {
    authority
        .stream_path(&mut None)?
        .strip_prefix("harness/v2/")
        .map(str::to_owned)
        .ok_or_else(|| Error::Invalid("projection authority path is invalid".into()))
}

fn head_path(authority: &Authority) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "harness/v3/projection-checkpoints/{}",
        suffix(authority)?
    ))?)
}

fn chunk_path(authority: &Authority, chunk: &ProjectionChunk) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "harness/v3/projection-chunks/{}/{}",
        suffix(authority)?,
        blake3::Hash::from_bytes(chunk.digest).to_hex(),
    ))?)
}

async fn tail<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    authority: &Authority,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    if work.is_none() {
        let stream = acyclic_stream::Stream::new(
            std::sync::Arc::clone(&client),
            authority.stream_path(&mut None)?,
        )?;
        return match stream.tail().await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(error.into()),
        };
    }
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
    let index = operations::one_record(client, &path, checkpoint_count - 1, work).await;
    drop(path);
    let index =
        operations::release_owned_after_drop(index, path_owned, work)?.ok_or_else(|| {
            super::guarded_boundary(
                work,
                crate::wire::ErrorCode::Unsupported,
                "canonical history requires a durable projection checkpoint",
            )
        })?;
    let result = async {
        let mut consumed_bytes = 0;
        charge(
            &mut consumed_bytes,
            index.value.len() as u64,
            limits.maximum_bytes,
            work,
        )?;
        let (location, location_owned) = decode_projection_location_with_work(&index.value, work)?;
        let result = async {
            if &location.authority != authority
                || location.revision
                    != checkpoint_count
                        .checked_mul(u64::from(DEFAULT_PROJECTION_EVENTS))
                        .ok_or_else(|| {
                            super::guarded_boundary(
                                work,
                                crate::wire::ErrorCode::Storage,
                                "projection head revision overflows",
                            )
                        })?
                || location.revision > through_revision
                || location.bytes == 0
                || location.bytes > MAX_PROJECTION_BYTES
                || through_revision - location.revision >= u64::from(limits.maximum_events)
            {
                return Err(super::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "projection checkpoint boundary exceeds read allowance",
                ));
            }
            if location.bytes > limits.maximum_bytes.saturating_sub(consumed_bytes) {
                return Err(super::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "projection read exceeds byte allowance",
                ));
            }
            let snapshot = location
                .load_snapshot(
                    client,
                    verifier,
                    &index.commit_id,
                    &mut consumed_bytes,
                    limits.maximum_bytes,
                    work,
                )
                .await?;
            Ok(LoadedProjection {
                snapshot: Some(snapshot),
                through_revision,
                consumed_bytes,
                consumed_events: 1,
            })
        }
        .await;
        drop(location);
        operations::release_owned_after_drop(result, location_owned, work)
    }
    .await;
    let index_bytes = operations::sdk_record_retained_bytes(&index, work);
    drop(index);
    let released = match work.as_deref_mut() {
        Some(observer) => observer.release_owned(index_bytes),
        None => Ok(()),
    };
    match result {
        Ok(value) => released.map(|()| value),
        Err(error) => Err(error),
    }
}

impl ProjectionLocation {
    async fn load_snapshot<P: StreamProvider>(
        &self,
        client: &std::sync::Arc<P>,
        verifier: &AuthorityVerifier,
        commit_id: &acyclic_stream::CommitId,
        consumed_bytes: &mut u64,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Snapshot> {
        let (path, path_owned) =
            operations::publication_path(&self.authority, "harness/v2/", None, work)?;
        let canonical = operations::one_record(client, &path, self.revision - 1, work).await;
        drop(path);
        let canonical = operations::release_owned_after_drop(canonical, path_owned, work)?
            .ok_or_else(|| {
                super::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "projection canonical anchor is missing",
                )
            })?;
        let result = async {
            charge(
                consumed_bytes,
                canonical.value.len() as u64,
                maximum_bytes,
                work,
            )?;
            if &canonical.commit_id != commit_id {
                return Err(super::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "projection head is outside its canonical atomic commit",
                ));
            }
            let ((actual_authority, event), event_retained) =
                crate::wire_codec::decode_event(&canonical.value, work)?;
            let authority_owned = if work.is_some() {
                actual_authority.id.capacity() as u64
            } else {
                0
            };
            let result = async {
                verifier.verify_event(&event, work)?;
                if actual_authority != self.authority || event.revision != self.revision {
                    return Err(super::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "projection canonical anchor differs",
                    ));
                }
                if let Some(observer) = work.as_deref_mut() {
                    observer.stage_output(self.bytes, 1)?;
                }
                let bytes = load_chunks(client, self, consumed_bytes, maximum_bytes, work).await?;
                let result = (|| {
                    let snapshot = Snapshot::decode(&bytes, maximum_bytes, work)?;
                    if snapshot.authority != self.authority
                        || snapshot.revision != self.revision
                        || snapshot.events.last() != Some(&event)
                    {
                        return Err(super::guarded_boundary(
                            work,
                            crate::wire::ErrorCode::Storage,
                            "projection snapshot differs from its canonical anchor",
                        ));
                    }
                    if let Some(observer) = work.as_deref_mut() {
                        observer.check_cancellation()?;
                    }
                    Ok(snapshot)
                })();
                let capacity = bytes.capacity() as u64;
                drop(bytes);
                let released = match work.as_deref_mut() {
                    Some(observer) => observer.release_owned(capacity),
                    None => Ok(()),
                };
                match result {
                    Ok(snapshot) => released.map(|()| snapshot),
                    Err(error) => Err(error),
                }
            }
            .await;
            drop(event);
            drop(actual_authority);
            let result = operations::release_owned_after_drop(result, event_retained, work);
            operations::release_owned_after_drop(result, authority_owned, work)
        }
        .await;
        let canonical_bytes = operations::sdk_record_retained_bytes(&canonical, work);
        drop(canonical);
        let released = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(canonical_bytes),
            None => Ok(()),
        };
        match result {
            Ok(snapshot) => released.map(|()| snapshot),
            Err(error) => Err(error),
        }
    }
}

async fn load_chunks<P: StreamProvider>(
    client: &std::sync::Arc<P>,
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
    let mut bytes = match work.as_deref_mut() {
        Some(work) => {
            let mut bytes = Vec::new();
            crate::store::history::reserve(&mut bytes, length, work)?;
            bytes
        }
        None => Vec::with_capacity(length),
    };
    let result = async {
        for chunk in &location.chunks {
            let expected = chunk.bytes as usize;
            if expected as u64 > maximum.saturating_sub(*consumed) {
                return Err(super::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "projection read exceeds byte allowance",
                ));
            }
            let digest = blake3::Hash::from_bytes(chunk.digest);
            let (path, path_owned) = operations::publication_path(
                &location.authority,
                "harness/v3/projection-chunks/",
                Some((&digest.to_hex(), 64)),
                work,
            )?;
            let record = operations::one_record(client, &path, 0, work).await;
            drop(path);
            let record = operations::release_owned_after_drop(record, path_owned, work)?
                .ok_or_else(|| {
                    super::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "projection chunk is missing",
                    )
                })?;
            let result = (|| {
                charge(consumed, record.value.len() as u64, maximum, work)?;
                let digest_matches = if record.value.len() == expected {
                    if let Some(observer) = work.as_deref_mut() {
                        observer.hashed_bytes(record.value.len() as u64)?;
                    }
                    blake3::hash(&record.value).as_bytes() == &chunk.digest
                } else {
                    false
                };
                if !digest_matches {
                    return Err(super::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "projection chunk length or digest differs",
                    ));
                }
                if let Some(observer) = work.as_deref_mut() {
                    observer.copy_bytes(record.value.len() as u64)?;
                }
                bytes.extend_from_slice(&record.value);
                Ok(())
            })();
            let record_bytes = operations::sdk_record_retained_bytes(&record, work);
            drop(record);
            let released = match work.as_deref_mut() {
                Some(observer) => observer.release_owned(record_bytes),
                None => Ok(()),
            };
            match result {
                Ok(()) => released?,
                Err(error) => return Err(error),
            }
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.hashed_bytes(bytes.len() as u64)?;
        }
        if blake3::hash(&bytes).as_bytes() != &location.digest {
            return Err(super::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "projection payload digest differs",
            ));
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(bytes),
        Err(error) => {
            let capacity = bytes.capacity() as u64;
            drop(bytes);
            if let Some(observer) = work.as_deref_mut() {
                let _ = observer.release_owned(capacity);
            }
            Err(error)
        }
    }
}

mod staging;
pub(super) use staging::*;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_malformed_receipt_tests;
#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_staging_custody_tests;
