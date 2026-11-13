//! Durable settings evidence before an agent root exists. Ordinary Streams
//! reads and follows provide the same paged, publication-driven query.
use super::Admission;
use crate::{AgentId, Error, Result};
use acyclic_stream::{
    AppendOutcome, AppendRecord, AppendRequest, IdempotencyKey, NativeStreamProvider, ReadKeySeek,
    ReadRequest, RecordHeaders, StreamPath, WriteOutcome, WriteRequest,
};
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    version: u8,
    agent: AgentId,
    admission: Admission,
}

/// Owner-bound settings decisions, retained even when session startup refuses.
/// The provider owns authority to this data; no task volume grant is delegated.
#[derive(Clone)]
pub struct SettingsAdmissions {
    provider: Arc<dyn NativeStreamProvider>,
}

impl SettingsAdmissions {
    /// Uses the original owner's Streams consistency and native funding domain.
    #[must_use]
    pub fn new(provider: Arc<dyn NativeStreamProvider>) -> Self {
        Self { provider }
    }

    /// The ordinary stream to read in bounded pages or follow from a retained cut.
    pub fn path(agent: AgentId) -> Result<StreamPath> {
        StreamPath::new(format!("harness/settings-admissions/v1/{agent}")).map_err(invalid)
    }

    /// The latest decision for exactly one settings path and source version.
    /// This query pins its read interval in Streams; it needs no prior tail read.
    pub fn query(agent: AgentId, path: &str, version: &str) -> Result<ReadRequest> {
        Ok(ReadRequest {
            key: Some(ReadKeySeek::latest(key(path, version)?, u64::MAX)),
            path: Self::path(agent)?,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
    }

    /// Retains this actual decision before startup returns success or refusal.
    /// Identical decisions replay their original append; later decisions keep
    /// history and become the latest value under the same indexed identity.
    pub async fn record(
        &self,
        agent: AgentId,
        admission: Admission,
        maximum_bytes: Option<u64>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let key = key(&admission.path, &admission.version)?;
        if let Some(maximum) = maximum_bytes {
            let bytes = serde_json::to_vec(&admission)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() as u64 > maximum {
                return Err(Error::Invalid(
                    "settings admission exceeds content limit".into(),
                ));
            }
        }
        let bytes = serde_json::to_vec(&Decision {
            version: 1,
            agent,
            admission,
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let idempotency_key = IdempotencyKey::new(format!(
            "settings-admission:v1:{agent}:{}",
            blake3::hash(&bytes)
        ))
        .map_err(invalid)?;
        let request = WriteRequest::Append(AppendRequest {
            path: Self::path(agent)?,
            records: vec![AppendRecord {
                value: bytes.into(),
                headers: RecordHeaders::new(vec![key.into()], vec![]).map_err(invalid)?,
            }],
            if_tail: None,
            idempotency_key,
        });
        let output = crate::store::history::native_sdk_result(
            work,
            self.provider
                .native_write(request, work.runtime_meter())
                .await,
        )?;
        let output = crate::store::operations::original_sdk_metadata_output(work, output)?;
        if !matches!(
            output.value(),
            WriteOutcome::Append(AppendOutcome::Committed(_))
        ) {
            return Err(Error::Storage("settings admission was not appended".into()));
        }
        Ok(())
    }

    /// Executes the same bounded query and verifies its original identity.
    pub async fn latest(
        &self,
        agent: AgentId,
        path: &str,
        version: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Admission>> {
        let request = Self::query(agent, path, version)?;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let mut observer = io.history_io();
        let Some(record) = crate::store::operations::one_selected_record(
            &self.provider,
            request,
            &mut Some(&mut observer),
        )
        .await?
        else {
            return Ok(None);
        };
        let decision: Decision = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if decision.version != 1 {
            return Err(Error::Unsupported(
                "settings decisions require format v1".into(),
            ));
        }
        if decision.agent != agent
            || decision.admission.path != path
            || decision.admission.version != version
        {
            return Err(Error::Storage(
                "settings decision belongs to another query".into(),
            ));
        }
        Ok(Some(decision.admission))
    }
}

fn key(path: &str, version: &str) -> Result<Vec<u8>> {
    crate::conversation::validate_content_path(path, &mut None)?;
    if version.is_empty() {
        return Err(Error::Invalid("settings version is empty".into()));
    }
    let mut hash = blake3::Hasher::new_derive_key("acyclic.settings-admission/v1");
    hash.update(&(path.len() as u64).to_be_bytes());
    hash.update(path.as_bytes());
    hash.update(version.as_bytes());
    Ok(hash.finalize().as_bytes().to_vec())
}

fn invalid(error: acyclic_stream::StreamError) -> Error {
    Error::Invalid(error.to_string())
}

#[cfg(all(test, not(loom)))]
mod tests;
