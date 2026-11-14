//! Durable settings evidence before an agent root exists. Ordinary Streams
//! reads and follows provide the same paged, publication-driven query.
use super::Admission;
use crate::{AgentId, Error, Result};
use acyclic_stream::{
    AppendOutcome, AppendRecord, AppendRequest, IdempotencyKey, NativeStreamProvider, ReadKeySeek,
    ReadRequest, RecordHeaders, StreamPath,
};
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision<T = Admission, C = String> {
    version: u8,
    agent: AgentId,
    cut: C,
    admission: T,
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

    /// The latest decision for one settings path, complete cut and file version.
    /// This query pins its read interval in Streams; it needs no prior tail read.
    pub fn query(
        agent: AgentId,
        path: &str,
        cut: &str,
        version: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ReadRequest> {
        Ok(ReadRequest {
            key: Some(ReadKeySeek::latest(
                key(path, cut, version, io)?.to_vec(),
                u64::MAX,
            )),
            path: Self::path(agent)?,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
    }

    /// Retains this actual decision before startup returns success or refusal.
    /// Identical decisions replay their original append; later decisions keep
    /// history and become the latest value under the same indexed identity.
    pub async fn record<T: serde::Serialize + Sync>(
        &self,
        agent: AgentId,
        (path, cut, version): (&str, &str, &str),
        admission: &T,
        maximum_bytes: Option<u64>,
        io: &crate::executor::PreparationIo,
    ) -> Result<()> {
        // The caller lends its canonical Admission representation, including
        // borrowed diagnostics, rather than allocating a second owned DTO.
        if maximum_bytes.is_some() {
            io.encode(admission, maximum_bytes, false)?;
        }
        let key = key(path, cut, version, Some(io))?;
        let _key_hold = io.guard_owned(key.len() as u64)?;
        io.allocation_attempt(key.len() as u64)?;
        let key = key.to_vec();
        let bytes = io.encode(
            &Decision {
                version: 1,
                agent,
                cut,
                admission,
            },
            None,
            true,
        )?;
        io.charge(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..Default::default()
        })?;
        let identity = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!("settings-admission:v1:{agent}:{}", blake3::hash(&bytes)),
                work,
            )
        })?;
        let _identity_hold = io.guard_owned(identity.capacity() as u64)?;
        let request = AppendRequest {
            path: Self::path(agent)?,
            records: vec![AppendRecord {
                value: bytes,
                headers: RecordHeaders::new(vec![key.into()], vec![]).map_err(invalid)?,
            }],
            if_tail: None,
            idempotency_key: IdempotencyKey::new(identity).map_err(invalid)?,
        };
        let mut history = io.history_io();
        let output = crate::store::operations::append_with_work(
            &self.provider,
            request,
            &mut Some(&mut history),
        )
        .await?;
        if !matches!(&*output, AppendOutcome::Committed(_)) {
            return Err(Error::Storage("settings admission was not appended".into()));
        }
        Ok(())
    }
    /// Executes the same bounded query and verifies its original identity.
    pub async fn latest(
        &self,
        agent: AgentId,
        path: &str,
        cut: &str,
        version: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Admission>> {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let request = Self::query(agent, path, cut, version, Some(&io))?;
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
            || decision.cut != cut
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

fn key(
    path: &str,
    cut: &str,
    version: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<[u8; 32]> {
    crate::executor::with_history_io(io, |work| {
        crate::conversation::validate_content_path(path, work)?;
        if cut.is_empty() || version.is_empty() {
            return Err(Error::Invalid("settings cut or version is empty".into()));
        }
        const DOMAIN: &str = "acyclic.settings-admission/v1";
        let length = crate::core::owned_sum([
            DOMAIN.len() as u64,
            16,
            path.len() as u64,
            cut.len() as u64,
            version.len() as u64,
        ])?;
        if let Some(work) = work.as_deref_mut() {
            work.hashed_bytes(length)?;
        }
        let mut hash = blake3::Hasher::new_derive_key(DOMAIN);
        hash.update(&(path.len() as u64).to_be_bytes());
        hash.update(path.as_bytes());
        hash.update(&(cut.len() as u64).to_be_bytes());
        hash.update(cut.as_bytes());
        hash.update(version.as_bytes());
        Ok(*hash.finalize().as_bytes())
    })
}
fn invalid(error: acyclic_stream::StreamError) -> Error {
    Error::Invalid(error.to_string())
}

#[cfg(all(test, not(loom)))]
mod tests;
