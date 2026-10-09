//! Durable accelerators over the same authenticated reducer and canonical Stream.

use super::{HistoryReadLimits, operations};
use crate::{
    Error, Result,
    core::{Authority, AuthorityVerifier, Event, Reducer, Snapshot},
    wire_codec::{decode_event, encode_event},
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
fn projection_chunks(bytes: &[u8]) -> Result<Vec<&[u8]>> {
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
            chunks.push(
                bytes
                    .get(start..=index)
                    .ok_or_else(|| Error::Invalid("projection chunk boundary is invalid".into()))?,
            );
            start = index + 1;
        }
    }
    if start < bytes.len() {
        chunks.push(
            bytes.get(start..).ok_or_else(|| {
                Error::Invalid("projection final chunk boundary is invalid".into())
            })?,
        );
    }
    Ok(chunks)
}

fn chunk_path(authority: &Authority, chunk: &ProjectionChunk) -> Result<StreamPath> {
    Ok(StreamPath::new(format!(
        "harness/v3/projection-chunks/{}/{}",
        suffix(authority)?,
        blake3::Hash::from_bytes(chunk.digest).to_hex(),
    ))?)
}

async fn stage_chunk<P: StreamProvider>(
    client: &StreamClient<P>,
    path: StreamPath,
    bytes: Bytes,
) -> Result<()> {
    let key = IdempotencyKey::new(Bytes::copy_from_slice(
        blake3::hash(path.as_str().as_bytes()).as_bytes(),
    ))?;
    let outcome = client
        .commit(CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![bytes.clone()],
            }],
            idempotency_key: key,
        })
        .await?;
    if let CommitOutcome::Committed(envelope) = outcome {
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
            return Ok(());
        }
        return Err(Error::Storage("projection chunk receipt is invalid".into()));
    }
    // A competing publication may stage identical content. Its immutable bytes
    // must match; only the later canonical commit makes the checkpoint visible.
    if operations::one_record(client, &path, 0)
        .await?
        .is_some_and(|record| record.value == bytes)
    {
        Ok(())
    } else {
        Err(Error::Conflict("projection chunk identity differs".into()))
    }
}

pub(super) async fn publication<P: StreamProvider>(
    client: &StreamClient<P>,
    reducer: &Reducer,
    event: &Event,
) -> Result<operations::IndexedPublication> {
    let mut publication = operations::IndexedPublication::new(
        reducer.authority(),
        event,
        encode_event(reducer.authority(), event)?,
    )?;
    super::effects::add_publication_index(client, reducer, event, &mut publication).await?;
    if !event
        .revision
        .is_multiple_of(u64::from(DEFAULT_PROJECTION_EVENTS))
    {
        return Ok(publication);
    }
    let mut preview = reducer.clone();
    preview.apply_committed(event.clone())?;
    let snapshot = preview.snapshot_with_event_limit(1)?;
    crate::contract::validate_json_byte_bound(&snapshot, MAX_PROJECTION_BYTES)?;
    let bytes = crate::contract::canonical_json_bytes(&snapshot)?;
    let chunks = projection_chunks(&bytes)?;
    let location = ProjectionLocation {
        authority: reducer.authority().clone(),
        revision: event.revision,
        bytes: bytes.len() as u64,
        digest: *blake3::hash(&bytes).as_bytes(),
        chunks: chunks
            .iter()
            .map(|chunk| {
                Ok(ProjectionChunk {
                    digest: *blake3::hash(chunk).as_bytes(),
                    bytes: u32::try_from(chunk.len())
                        .map_err(|_| Error::Invalid("projection chunk exceeds platform".into()))?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let location_bytes = crate::contract::canonical_json_bytes(&location)?;
    if location_bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::Invalid(
            "projection manifest exceeds one record".into(),
        ));
    }
    for (descriptor, chunk) in location.chunks.iter().zip(chunks) {
        stage_chunk(
            client,
            chunk_path(&location.authority, descriptor)?,
            Bytes::copy_from_slice(chunk),
        )
        .await?;
    }
    publication.add_derived_index(
        head_path(reducer.authority())?,
        event.revision / u64::from(DEFAULT_PROJECTION_EVENTS) - 1,
        Bytes::from(location_bytes),
    );
    Ok(publication)
}

async fn tail<P: StreamProvider>(client: &StreamClient<P>, authority: &Authority) -> Result<u64> {
    match client.stream(authority.stream_path()?)?.tail().await {
        Ok(tail) => Ok(tail),
        Err(StreamError::NotFound) => Ok(0),
        Err(error) => Err(error.into()),
    }
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
        through_revision: tail(client, authority).await?,
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
    verifier.verify_audience(authority)?;
    if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
        return Err(Error::Invalid(
            "projection read bounds must be positive".into(),
        ));
    }
    let head = client.stream(head_path(authority)?.as_str())?;
    let head_tail = match head.tail().await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => return Err(error.into()),
    };
    // Observe the checkpoint first, then pin a canonical cut. Later appends
    // cannot move the selected checkpoint into the future of this traversal.
    let index = if head_tail == 0 {
        None
    } else {
        Some(
            operations::one_record(client, head.path(), head_tail - 1)
                .await?
                .ok_or_else(|| Error::Storage("projection head is missing".into()))?,
        )
    };
    let through_revision = tail(client, authority).await?;
    let Some(index) = index else {
        if through_revision >= u64::from(DEFAULT_PROJECTION_EVENTS) {
            return Err(Error::Unsupported(
                "canonical history requires a durable projection checkpoint".into(),
            ));
        }
        return Ok(LoadedProjection {
            snapshot: None,
            through_revision,
            consumed_bytes: 0,
            consumed_events: 0,
        });
    };
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
            != head_tail
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
        client: &StreamClient<P>,
        verifier: &AuthorityVerifier,
        commit_id: &acyclic_stream::CommitId,
        consumed_bytes: &mut u64,
        maximum_bytes: u64,
    ) -> Result<Snapshot> {
        let canonical = operations::one_record(
            client,
            &StreamPath::new(self.authority.stream_path()?)?,
            self.revision - 1,
        )
        .await?
        .ok_or_else(|| Error::Storage("projection canonical anchor is missing".into()))?;
        charge(consumed_bytes, canonical.value.len() as u64, maximum_bytes)?;
        if &canonical.commit_id != commit_id {
            return Err(Error::Storage(
                "projection head is outside its canonical atomic commit".into(),
            ));
        }
        let (actual_authority, event) = decode_event(&canonical.value)?;
        verifier.verify_event(&event)?;
        if actual_authority != self.authority || event.revision != self.revision {
            return Err(Error::Storage("projection canonical anchor differs".into()));
        }
        let bytes = load_chunks(client, self, consumed_bytes, maximum_bytes).await?;
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
        Ok(snapshot)
    }
}

async fn load_chunks<P: StreamProvider>(
    client: &StreamClient<P>,
    location: &ProjectionLocation,
    consumed: &mut u64,
    maximum: u64,
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
    let mut bytes = Vec::with_capacity(length);
    for chunk in &location.chunks {
        let expected = chunk.bytes as usize;
        if expected as u64 > maximum.saturating_sub(*consumed) {
            return Err(Error::Invalid(
                "projection read exceeds byte allowance".into(),
            ));
        }
        let record = operations::one_record(client, &chunk_path(&location.authority, chunk)?, 0)
            .await?
            .ok_or_else(|| Error::Storage("projection chunk is missing".into()))?;
        charge(consumed, record.value.len() as u64, maximum)?;
        if record.value.len() != expected || blake3::hash(&record.value).as_bytes() != &chunk.digest
        {
            return Err(Error::Storage(
                "projection chunk length or digest differs".into(),
            ));
        }
        bytes.extend_from_slice(&record.value);
    }
    if blake3::hash(&bytes).as_bytes() != &location.digest {
        return Err(Error::Storage("projection payload digest differs".into()));
    }
    Ok(bytes)
}
