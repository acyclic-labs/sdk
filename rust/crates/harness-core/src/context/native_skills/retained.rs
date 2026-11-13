//! Content-addressed source metadata in the original owner's Streams state.
//! Each indexed record retains a frame-sized discovery chunk atomically;
//! sibling lookups share its bounded read and decode, without native sidecars.
use super::*;
use acyclic_stream::{
    AppendRecord, AppendRequest, NativeStreamProvider, RecordHeaders, StreamPath, WriteOutcome,
    WriteRequest,
};
use std::collections::{BTreeMap, HashMap};

fn path() -> Result<StreamPath> {
    StreamPath::new("harness/native-skill-sources/v1").map_err(storage)
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedRecords {
    pub(super) version: u8,
    pub(super) sources: RetainedSource,
}

/// Shared by owner bindings; each immutable record retains its own owner check.
#[derive(Clone)]
pub(super) struct RecordCache {
    provider: Arc<dyn NativeStreamProvider>,
    records: Arc<crate::sync::owned::Mutex<HashMap<String, Arc<RetainedSource>>>>,
}

impl RecordCache {
    pub(super) fn new(provider: Arc<dyn NativeStreamProvider>) -> Self {
        Self {
            provider,
            records: Arc::default(),
        }
    }

    pub(super) async fn read(
        &self,
        digest: String,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Arc<RetainedSource>>> {
        // Sibling first loads share this asynchronous lock through the native
        // read and decode. No runtime thread waits for a blocking file lock.
        let mut records = self.records.lock().await;
        if let Some(record) = records.get(&digest) {
            return Ok(Some(Arc::clone(record)));
        }
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let mut observer = io.history_io();
        let record = crate::store::operations::one_keyed_record(
            &self.provider,
            &path()?,
            digest.as_bytes().to_vec(),
            &mut Some(&mut observer),
        )
        .await?;
        let Some(record) = record else {
            return Ok(None);
        };
        let batch = decode_stream_records(&record.value, &digest)?;
        // Validate every sibling before making even the selected record hot.
        records.extend(
            batch
                .into_iter()
                .map(|(key, record)| (key, Arc::new(record))),
        );
        Ok(records.get(&digest).map(Arc::clone))
    }
}

#[derive(Default)]
pub(super) struct RetainedBatch {
    records: BTreeMap<String, (RetainedSource, usize)>,
}

impl RetainedBatch {
    pub(super) fn retain(
        &mut self,
        journal: &VolumeRef,
        record: RetainedSource,
    ) -> Result<PinnedContextPath> {
        let bytes = serde_json::to_vec(&record).map_err(storage)?;
        let digest = blake3::hash(&bytes);
        self.records
            .insert(digest.to_string(), (record, bytes.len()));
        Ok(PinnedContextPath {
            root: ContextRoot {
                volume: journal.clone(),
                directory: "inputs/skills".into(),
            },
            generation: crate::resources::GenerationRef::new(
                journal.provider(),
                digest.as_bytes(),
                None,
                None,
            )?
            .0,
            path: format!("inputs/skills/{digest}/SKILL.md"),
        })
    }

    pub(super) async fn publish(
        self,
        cache: &RecordCache,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        // Account for the exact JSON envelope, map punctuation and indexed
        // header keys. Lengths were measured while hashing each source, so
        // chunking is linear in the catalog, without repeatedly encoding it.
        let empty_bytes = serde_json::to_vec(&RetainedRecords {
            version: 1,
            sources: RetainedSource::Batch(BTreeMap::new()),
        })
        .map_err(storage)?
        .len();
        let mut chunk = BTreeMap::new();
        let mut bytes = empty_bytes;
        for (key, (record, length)) in self.records {
            // Digests are ASCII hex: JSON quotes, a colon, and the header key.
            let entry = key.len() * 2 + 3 + length;
            let separator = usize::from(!chunk.is_empty());
            if !chunk.is_empty() && bytes + entry + separator > acyclic_stream::MAX_RECORD_BYTES {
                Self::publish_chunk(std::mem::take(&mut chunk), cache, work).await?;
                bytes = empty_bytes;
            }
            // A singleton uses the source directly, without a Batch map. This
            // also allows a source that fits only without the map's overhead.
            if empty_bytes + entry > acyclic_stream::MAX_RECORD_BYTES {
                Self::publish_chunk(BTreeMap::from([(key, record)]), cache, work).await?;
            } else {
                bytes += entry + usize::from(!chunk.is_empty());
                chunk.insert(key, record);
            }
        }
        if !chunk.is_empty() {
            Self::publish_chunk(chunk, cache, work).await?;
        }
        Ok(())
    }

    async fn publish_chunk(
        records: BTreeMap<String, RetainedSource>,
        cache: &RecordCache,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        // Sorted keys make both chunk boundaries and each retry identity
        // independent of discovery order. Catalog pins become visible only
        // after all chunks commit; a partial retry reuses committed chunks.
        let keys = records
            .keys()
            .map(|key| key.as_bytes().to_vec().into())
            .collect();
        let sources = if records.len() == 1 {
            records
                .into_values()
                .next()
                .ok_or_else(|| Error::Invalid("retained skill batch is empty".into()))?
        } else {
            RetainedSource::Batch(records)
        };
        let bytes = serde_json::to_vec(&RetainedRecords {
            version: 1,
            sources,
        })
        .map_err(storage)?;
        let headers = RecordHeaders::new(keys, vec![]).map_err(storage)?;
        if bytes.len().saturating_add(headers.payload_bytes()) > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::Invalid(
                "retained skill source exceeds the Streams record frame".into(),
            ));
        }
        let key = acyclic_stream::IdempotencyKey::new(format!(
            "native-skill-sources:v1:{}",
            blake3::hash(&bytes)
        ))
        .map_err(storage)?;
        let request = WriteRequest::Append(AppendRequest {
            path: path()?,
            records: vec![AppendRecord {
                value: bytes.into(),
                headers,
            }],
            if_tail: None,
            idempotency_key: key,
        });
        let outcome = crate::store::history::native_sdk_result(
            work,
            cache
                .provider
                .native_write(request, work.runtime_meter())
                .await,
        )?;
        let outcome = crate::store::operations::original_sdk_metadata_output(work, outcome)?;
        if !matches!(
            outcome.value(),
            WriteOutcome::Append(acyclic_stream::AppendOutcome::Committed(_))
        ) {
            return Err(Error::Storage(
                "retained skill batch was not appended".into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn decode_stream_records(
    bytes: &[u8],
    digest: &str,
) -> Result<BTreeMap<String, RetainedSource>> {
    let batch: RetainedRecords = serde_json::from_slice(bytes).map_err(storage)?;
    if batch.version != 1 {
        return Err(Error::Unsupported(
            "retained skill records require format v1".into(),
        ));
    }
    admit_records(batch.sources, digest)
}

/// The source identity codec is shared by the writer, indexed storage and fuzzing.
#[cfg(any(test, feature = "test-support"))]
pub(super) fn decode_records(
    bytes: &[u8],
    digest: &str,
) -> Result<BTreeMap<String, RetainedSource>> {
    admit_records(serde_json::from_slice(bytes).map_err(storage)?, digest)
}

fn admit_records(record: RetainedSource, digest: &str) -> Result<BTreeMap<String, RetainedSource>> {
    let batch = match record {
        RetainedSource::Batch(batch) => batch,
        record => BTreeMap::from([(digest.to_owned(), record)]),
    };
    if !batch.contains_key(digest) {
        return Err(Error::Conflict("retained skill source is absent".into()));
    }
    for (key, record) in &batch {
        validate_record(record, key)?;
    }
    Ok(batch)
}

fn validate_record(record: &RetainedSource, digest: &str) -> Result<()> {
    if matches!(record, RetainedSource::Batch(_)) {
        return Err(Error::Invalid("nested retained skill batch".into()));
    }
    if blake3::hash(&serde_json::to_vec(record).map_err(storage)?)
        .to_hex()
        .as_str()
        != digest
    {
        return Err(Error::Conflict("retained skill source changed".into()));
    }
    Ok(())
}
