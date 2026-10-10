//! Durable accelerators over the same authenticated reducer and canonical Stream.

use super::{HistoryReadLimits, history::HistoryIo, operations};
use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event, Reducer, Snapshot},
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, CommittedMutation,
    IdempotencyKey, StreamClient, StreamError, StreamPath, StreamProvider,
};
use bytes::Bytes;
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
struct ProjectionLocation {
    authority: Authority,
    revision: u64,
    bytes: u64,
    digest: [u8; 32],
    chunks: Vec<ProjectionChunk>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionChunk {
    digest: [u8; 32],
    bytes: u32,
}

pub(super) struct LoadedProjection {
    pub(super) snapshot: Option<Snapshot>,
    pub(super) through_revision: u64,
    pub(super) consumed_bytes: u64,
    pub(super) consumed_events: u32,
}

fn suffix(authority: &Authority) -> Result<String> {
    authority
        .stream_path()?
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

// Content-defined boundaries converge after a changed prefix, so appending
// history does not retain another copy of its unchanged interior every 64 events.
// The rolling value depends only on the last 64 bytes; boundaries are bounded
// between 16 KiB and one Stream record, apart from the final chunk.
fn projection_chunks<'a>(
    bytes: &'a [u8],
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<&'a [u8]>> {
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut rolling = 0_u64;
    for (index, byte) in bytes.iter().enumerate() {
        let mut gear = u64::from(*byte).wrapping_add(0x9e37_79b9_7f4a_7c15);
        gear = (gear ^ (gear >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        gear = (gear ^ (gear >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        rolling = (rolling << 1).wrapping_add(gear ^ (gear >> 31));
        let length = index + 1 - start;
        if length == acyclic_stream::MAX_RECORD_BYTES
            || (length >= 16 * 1024 && rolling & 0x3fff == 0)
        {
            reserve_chunk_slot(&mut chunks, bytes.len() / (16 * 1024) + 1, work)?;
            chunks.push(
                bytes
                    .get(start..=index)
                    .ok_or_else(|| Error::Invalid("projection chunk boundary is invalid".into()))?,
            );
            start = index + 1;
        }
    }
    if start < bytes.len() {
        reserve_chunk_slot(&mut chunks, bytes.len() / (16 * 1024) + 1, work)?;
        chunks.push(
            bytes.get(start..).ok_or_else(|| {
                Error::Invalid("projection final chunk boundary is invalid".into())
            })?,
        );
    }
    Ok(chunks)
}

fn reserve_chunk_slot<T>(
    values: &mut Vec<T>,
    maximum: usize,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else { return Ok(()); };
    if values.len() < values.capacity() { return Ok(()); }
    let target = (values.capacity().max(1) * 2).min(maximum);
    let element_bytes = std::mem::size_of::<T>() as u64;
    let old_capacity = values.capacity();
    observer.admit_owned(target as u64 * element_bytes)?;
    observer.copy_bytes(values.len() as u64 * element_bytes)?;
    values.try_reserve_exact(target - values.len())
        .map_err(|_| Error::Invalid("projection chunk vector allocation failed".into()))?;
    observer.retain_owned(values.capacity() as u64 * element_bytes)?;
    observer.release_owned(old_capacity as u64 * element_bytes)?;
    Ok(())
}

fn chunk_path(authority: &Authority, chunk: &ProjectionChunk) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "harness/v3/projection-chunks/{}/{}",
        suffix(authority)?,
        blake3::Hash::from_bytes(chunk.digest).to_hex(),
    ))?)
}

pub(super) async fn stage_immutable_record<P: StreamProvider>(
    client: &StreamClient<P>,
    path: StreamPath,
    bytes: Bytes,
) -> Result<()> {
    stage_immutable_record_with_work(client, path, bytes, &mut None).await
}

pub(super) async fn stage_immutable_record_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: StreamPath,
    bytes: Bytes,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
        observer.hashed_bytes(path.as_str().len() as u64)?;
        observer.copy_bytes(32)?;
    }
    let key = IdempotencyKey::new(Bytes::copy_from_slice(
        blake3::hash(path.as_str().as_bytes()).as_bytes(),
    ))?;
    let request = CommitRequest {
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![bytes.clone()],
        }],
        idempotency_key: key,
    };
    if let Some(observer) = work.as_deref_mut() {
        observer.admit_acknowledgment(bytes.len() as u64, 1)?;
        observer.begin_write()?;
    }
    let outcome = client.commit(request).await?;
    if let CommitOutcome::Committed(envelope) = outcome {
        let mut received_failure = None;
        if let Some(observer) = work.as_deref_mut() {
            for mutation in &envelope.mutations {
                let records = match mutation {
                    CommittedMutation::Append(append) => &append.records,
                    CommittedMutation::Fork(fork) => &fork.records,
                };
                for record in records {
                    let received = observer.receive_record(record.value.len() as u64);
                    if received_failure.is_none() { received_failure = received.err(); }
                }
            }
        }
        if let [CommittedMutation::Append(append)] = envelope.mutations.as_slice()
            && append.path == path
            && append.start == 0
            && append.end == 1
            && append.tail == 1
            && let [record] = append.records.as_slice()
            && record.sequence == 0
            && record.value == bytes
            && record.commit_id == envelope.commit_id
        {
            if let Some(observer) = work.as_deref_mut() {
                observer.acknowledged(bytes.len() as u64, 1)?;
                if let Some(error) = received_failure { return Err(error); }
                observer.release_record(bytes.len() as u64)?;
            }
            return Ok(());
        }
        return Err(Error::Storage("immutable record receipt is invalid".into()));
    }
    // A competing publication may stage identical content. Its immutable bytes
    // must match; only the later canonical commit makes the checkpoint visible.
    let record = operations::one_record_with_work(client, &path, 0, work).await?;
    let identical = record.as_ref().is_some_and(|record| record.value == bytes);
    if let (Some(observer), Some(record)) = (work.as_deref_mut(), record.as_ref()) {
        observer.release_record(record.value.len() as u64)?;
    }
    if !identical {
        return Err(Error::Conflict("immutable record identity differs".into()));
    }
    if let Some(observer) = work.as_deref_mut() {
        observer.begin_call()?;
    }
    let tail = client.stream(path.as_str())?.tail().await?;
    if tail == 1 {
        Ok(())
    } else {
        Err(Error::Conflict("immutable record identity differs".into()))
    }
}

pub(super) async fn publication<P: StreamProvider>(
    client: &StreamClient<P>,
    reducer: &Reducer,
    event: &Event,
) -> Result<operations::IndexedPublication> {
    publication_with_work(client, reducer, event, &mut None).await
}

pub(super) async fn publication_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    reducer: &Reducer,
    event: &Event,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<operations::IndexedPublication> {
    let mut publication = operations::IndexedPublication::new_with_work(
        reducer.authority(),
        event,
        crate::wire_codec::encode_event_with_work(reducer.authority(), event, work)?,
        work,
    )?;
    super::effects::add_publication_index_with_work(client, reducer, event, &mut publication, work).await?;
    if !event
        .revision
        .is_multiple_of(u64::from(DEFAULT_PROJECTION_EVENTS))
    {
        return Ok(publication);
    }
    // The staged projection becomes visible only in the same canonical commit
    // that publishes these original operation/effect indexes and checkpoint.
    let (snapshot, snapshot_reservation) =
        reducer.publication_snapshot_with_work(event, 1, work)?;
    let encoded = super::history::canonical_bytes_with_work(&snapshot, MAX_PROJECTION_BYTES, work);
    drop(snapshot);
    if let Some(observer) = work.as_deref_mut() {
        observer.release_owned(snapshot_reservation)?;
    }
    let bytes = encoded?;
    let chunks = projection_chunks(&bytes, work)?;
    if let Some(observer) = work.as_deref_mut() {
        observer.hashed_bytes(bytes.len() as u64)?;
    }
    let digest = *blake3::hash(&bytes).as_bytes();
    let chunk_slots = chunks.capacity() as u64 * std::mem::size_of::<&[u8]>() as u64;
    let mut descriptors = Vec::new();
    for chunk in &chunks {
        reserve_chunk_slot(&mut descriptors, chunks.len(), work)?;
        let length = u32::try_from(chunk.len())
            .map_err(|_| Error::Invalid("projection chunk exceeds platform".into()))?;
        if let Some(observer) = work.as_deref_mut() {
            observer.hashed_bytes(chunk.len() as u64)?;
        }
        descriptors.push(ProjectionChunk {
            digest: *blake3::hash(chunk).as_bytes(),
            bytes: length,
        });
    }
    let location = ProjectionLocation {
        authority: match work.as_deref_mut() {
            Some(observer) => observer.clone_authority(reducer.authority())?,
            None => reducer.authority().clone(),
        },
        revision: event.revision,
        bytes: bytes.len() as u64,
        digest,
        chunks: descriptors,
    };
    let location_bytes = super::history::canonical_bytes_with_work(&location, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    if location_bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::Invalid(
            "projection manifest exceeds one record".into(),
        ));
    }
    for (descriptor, chunk) in location.chunks.iter().zip(chunks) {
        if let Some(observer) = work.as_deref_mut() {
            observer.admit_owned(chunk.len() as u64)?;
            observer.copy_bytes(chunk.len() as u64)?;
            observer.retain_owned(chunk.len() as u64)?;
        }
        stage_immutable_record_with_work(
            client,
            chunk_path(&location.authority, descriptor)?,
            Bytes::copy_from_slice(chunk),
            work,
        )
        .await?;
        if let Some(observer) = work.as_deref_mut() {
            observer.release_owned(chunk.len() as u64)?;
        }
    }
    let location_owned = location.authority.id.capacity() as u64
        + location.chunks.capacity() as u64 * std::mem::size_of::<ProjectionChunk>() as u64;
    let source_capacity = bytes.capacity() as u64;
    drop(location);
    drop(bytes);
    if let Some(observer) = work.as_deref_mut() {
        observer.release_owned(source_capacity)?;
        observer.release_owned(chunk_slots)?;
        observer.release_owned(location_owned)?;
    }
    publication.add_derived_index_with_work(
        head_path(reducer.authority())?,
        event.revision / u64::from(DEFAULT_PROJECTION_EVENTS) - 1,
        Bytes::from(location_bytes),
        work,
    )?;
    Ok(publication)
}

async fn tail<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let stream = client.stream(authority.stream_path()?)?;
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    match stream.tail().await {
        Ok(tail) => Ok(tail),
        Err(StreamError::NotFound) => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn projection_cut(committed_tail: u64, original_revision: Option<u64>) -> Result<u64> {
    let through_revision = original_revision.unwrap_or(committed_tail);
    if through_revision > committed_tail {
        return Err(Error::Invalid("projection revision exceeds committed tail".into()));
    }
    Ok(through_revision)
}

fn charge(used: &mut u64, bytes: u64, maximum: u64) -> Result<()> {
    *used = used
        .checked_add(bytes)
        .filter(|total| *total <= maximum)
        .ok_or_else(|| Error::Invalid("projection read exceeds byte allowance".into()))?;
    Ok(())
}

pub(super) async fn supplied<P: StreamProvider>(
    client: &StreamClient<P>,
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
    let consumed_bytes = crate::contract::json_byte_length(&snapshot, limits.maximum_bytes)?;
    // All caller input preflight precedes provider IO; the original restore
    // still authenticates the issuer MAC and registry before state is used.
    Ok(LoadedProjection {
        snapshot: Some(snapshot),
        through_revision: projection_cut(tail(client, authority, work).await?, original_revision)?,
        consumed_bytes,
        consumed_events,
    })
}

pub(super) async fn load<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    limits: HistoryReadLimits,
) -> Result<LoadedProjection> {
    load_with_work(client, authority, verifier, limits, None, &mut None).await
}

pub(super) async fn load_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    limits: HistoryReadLimits,
    original_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<LoadedProjection> {
    verifier.verify_audience(authority)?;
    if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
        return Err(Error::Invalid(
            "projection read bounds must be positive".into(),
        ));
    }
    // Observe the real committed tail before choosing a checkpoint. Explicit
    // historical cuts select their own floor position; later publications
    // cannot change that authenticated checkpoint or its canonical anchor.
    let through_revision = projection_cut(tail(client, authority, work).await?, original_revision)?;
    let checkpoint_count = through_revision / u64::from(DEFAULT_PROJECTION_EVENTS);
    if checkpoint_count == 0 {
        return Ok(LoadedProjection {
            snapshot: None,
            through_revision,
            consumed_bytes: 0,
            consumed_events: 0,
        });
    }
    let index = operations::one_record_with_work(client, &head_path(authority)?, checkpoint_count - 1, work)
        .await?
        .ok_or_else(|| {
            Error::Unsupported("canonical history requires a durable projection checkpoint".into())
        })?;
    let mut consumed_bytes = 0;
    charge(
        &mut consumed_bytes,
        index.value.len() as u64,
        limits.maximum_bytes,
    )?;
    let location: ProjectionLocation = crate::contract::json_from_slice(&index.value)
        .map_err(|error| Error::Storage(error.to_string()))?;
    if &location.authority != authority
        || location.revision
            != checkpoint_count
                .checked_mul(u64::from(DEFAULT_PROJECTION_EVENTS))
                .ok_or_else(|| Error::Storage("projection head revision overflows".into()))?
        || location.revision > through_revision
        || location.bytes == 0
        || location.bytes > MAX_PROJECTION_BYTES
        || through_revision - location.revision >= u64::from(limits.maximum_events)
    {
        return Err(Error::Invalid(
            "projection checkpoint boundary exceeds read allowance".into(),
        ));
    }
    // Reject claimed payload sizes before allocating or reading any chunks.
    if location.bytes > limits.maximum_bytes.saturating_sub(consumed_bytes) {
        return Err(Error::Invalid(
            "projection read exceeds byte allowance".into(),
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
    let index_bytes = index.value.len() as u64;
    drop(index);
    if let Some(work) = work.as_deref_mut() { work.release_record(index_bytes)?; }
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
        client: &StreamClient<P>,
        verifier: &AuthorityVerifier,
        commit_id: &acyclic_stream::CommitId,
        consumed_bytes: &mut u64,
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Snapshot> {
        let canonical = operations::one_record_with_work(
            client,
            &StreamPath::new(self.authority.stream_path()?)?,
            self.revision - 1,
            work,
        )
        .await?
        .ok_or_else(|| Error::Storage("projection canonical anchor is missing".into()))?;
        charge(consumed_bytes, canonical.value.len() as u64, maximum_bytes)?;
        if &canonical.commit_id != commit_id {
            return Err(Error::Storage(
                "projection head is outside its canonical atomic commit".into(),
            ));
        }
        let (actual_authority, event) = crate::wire_codec::decode_event_with_work(&canonical.value, work)?;
        verifier.verify_event_with_work(&event, work)?;
        if actual_authority != self.authority || event.revision != self.revision {
            return Err(Error::Storage("projection canonical anchor differs".into()));
        }
        if let Some(work) = work.as_deref_mut() { work.stage_output(self.bytes, 1)?; }
        let bytes = load_chunks(client, self, consumed_bytes, maximum_bytes, work).await?;
        let snapshot: Snapshot = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if snapshot.authority != self.authority
            || snapshot.revision != self.revision
            || snapshot.events.last() != Some(&event)
        {
            return Err(Error::Storage(
                "projection snapshot differs from its canonical anchor".into(),
            ));
        }
        if let Some(work) = work.as_deref_mut() {
            work.check_cancellation()?;
        }
        let capacity = bytes.capacity() as u64;
        drop(bytes);
        let canonical_bytes = canonical.value.len() as u64;
        drop(canonical);
        if let Some(work) = work.as_deref_mut() {
            work.release_bytes(capacity)?;
            work.release_record(canonical_bytes)?;
        }
        Ok(snapshot)
    }
}

async fn load_chunks<P: StreamProvider>(
    client: &StreamClient<P>,
    location: &ProjectionLocation,
    consumed: &mut u64,
    maximum: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let length = usize::try_from(location.bytes)
        .map_err(|_| Error::Invalid("projection size exceeds platform".into()))?;
    // Validate the complete manifest before allocation or chunk IO. Minimum
    // interior lengths also bound IO count independently of attacker-provided
    // lists of tiny chunks. The last chunk may be shorter.
    let mut total = 0_u64;
    if location.chunks.is_empty() {
        return Err(Error::Storage("projection chunk manifest is empty".into()));
    }
    for (index, chunk) in location.chunks.iter().enumerate() {
        if chunk.bytes == 0
            || chunk.bytes as usize > acyclic_stream::MAX_RECORD_BYTES
            || (index + 1 < location.chunks.len() && chunk.bytes < 16 * 1024)
        {
            return Err(Error::Storage(
                "projection chunk manifest has invalid lengths".into(),
            ));
        }
        total = total
            .checked_add(u64::from(chunk.bytes))
            .ok_or_else(|| Error::Storage("projection chunk manifest overflows".into()))?;
    }
    if total != location.bytes {
        return Err(Error::Storage(
            "projection chunk manifest total differs".into(),
        ));
    }
    let mut bytes = match work.as_deref_mut() {
        Some(work) => {
            let mut bytes = Vec::new();
            work.reserve_bytes(&mut bytes, length)?;
            bytes
        }
        None => Vec::with_capacity(length),
    };
    for chunk in &location.chunks {
        let expected = chunk.bytes as usize;
        if expected as u64 > maximum.saturating_sub(*consumed) {
            return Err(Error::Invalid(
                "projection read exceeds byte allowance".into(),
            ));
        }
        let record = operations::one_record_with_work(client, &chunk_path(&location.authority, chunk)?, 0, work)
            .await?
            .ok_or_else(|| Error::Storage("projection chunk is missing".into()))?;
        charge(consumed, record.value.len() as u64, maximum)?;
        let digest_matches = if record.value.len() == expected {
            if let Some(work) = work.as_deref_mut() { work.hashed_bytes(record.value.len() as u64)?; }
            blake3::hash(&record.value).as_bytes() == &chunk.digest
        } else {
            false
        };
        if !digest_matches {
            return Err(Error::Storage(
                "projection chunk length or digest differs".into(),
            ));
        }
        if let Some(work) = work.as_deref_mut() { work.copy_bytes(record.value.len() as u64)?; }
        bytes.extend_from_slice(&record.value);
        let record_bytes = record.value.len() as u64;
        drop(record);
        if let Some(work) = work.as_deref_mut() { work.release_record(record_bytes)?; }
    }
    if let Some(work) = work.as_deref_mut() { work.hashed_bytes(bytes.len() as u64)?; }
    if blake3::hash(&bytes).as_bytes() != &location.digest {
        return Err(Error::Storage("projection payload digest differs".into()));
    }
    Ok(bytes)
}
