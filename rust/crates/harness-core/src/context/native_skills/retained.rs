//! Content-addressed source metadata in the original owner's Streams state.
//! One indexed record retains a discovery batch atomically; sibling lookups
//! share one bounded read and decode, without native sidecar files.
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
    records: BTreeMap<String, RetainedSource>,
}

impl RetainedBatch {
    pub(super) fn retain(
        &mut self,
        journal: &VolumeRef,
        record: RetainedSource,
    ) -> Result<PinnedContextPath> {
        let digest = blake3::hash(&serde_json::to_vec(&record).map_err(storage)?);
        self.records.insert(digest.to_string(), record);
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
        // Sorted keys also make the serialized batch and retry identity stable
        // across independently rebuilt catalogs and discovery orderings.
        let keys = self
            .records
            .keys()
            .map(|key| key.as_bytes().to_vec().into())
            .collect();
        let sources = if self.records.len() == 1 {
            self.records
                .into_values()
                .next()
                .ok_or_else(|| Error::Invalid("retained skill batch is empty".into()))?
        } else {
            RetainedSource::Batch(self.records)
        };
        let bytes = serde_json::to_vec(&RetainedRecords {
            version: 1,
            sources,
        })
        .map_err(storage)?;
        let key = acyclic_stream::IdempotencyKey::new(format!(
            "native-skill-sources:v1:{}",
            blake3::hash(&bytes)
        ))
        .map_err(storage)?;
        let request = WriteRequest::Append(AppendRequest {
            path: path()?,
            records: vec![AppendRecord {
                value: bytes.into(),
                headers: RecordHeaders::new(keys, vec![]).map_err(storage)?,
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
