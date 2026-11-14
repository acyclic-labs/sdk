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
#[serde(tag = "storage", rename_all = "snake_case", deny_unknown_fields)]
enum Decision<T = Admission, C = String> {
    Inline {
        version: u8,
        agent: AgentId,
        cut: C,
        admission: T,
    },
    Chunks {
        version: u8,
        digest: [u8; 32],
        bytes: u64,
    },
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
        let _key_hold = io.reserve_decode(key.len() as u64)?;
        io.allocation_attempt(key.len() as u64)?;
        let key = key.to_vec();
        let bytes = io.encode(
            &Decision::Inline {
                version: 1,
                agent,
                cut,
                admission,
            },
            None,
            true,
        )?;
        let digest = crate::model::hash_bytes(&bytes, Some(io))?;
        let identity = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!(
                    "settings-admission:v1:{agent}:{}",
                    blake3::Hash::from_bytes(digest)
                ),
                work,
            )
        })?;
        let _identity_hold = io.guard_owned(identity.capacity() as u64)?;
        let headers = RecordHeaders::new(vec![key.into()], vec![]).map_err(invalid)?;
        let bytes = if bytes.len().saturating_add(headers.payload_bytes())
            > acyclic_stream::MAX_RECORD_BYTES
        {
            let before = io.owned_bytes()?;
            let path = Self::payload_path(agent, &digest, Some(io))?;
            let _path_hold = io.guard_owned(io.owned_since(before)?)?;
            crate::store::operations::stage_chunks(
                &self.provider,
                &path,
                &bytes,
                Some(io),
                |sequence| {
                    let mut chunk = [0; 40];
                    chunk[..32].copy_from_slice(&digest);
                    chunk[32..].copy_from_slice(&sequence.to_be_bytes());
                    let digest = crate::model::hash_bytes(&chunk, Some(io))?;
                    let key = crate::executor::with_history_io(Some(io), |work| {
                        crate::executor::preparation_format(
                            format_args!(
                                "settings-admission-chunk:v1:{}",
                                blake3::Hash::from_bytes(digest)
                            ),
                            work,
                        )
                    })?;
                    let retained = key.capacity() as u64;
                    IdempotencyKey::new(key)
                        .map(|key| (key, retained))
                        .map_err(invalid)
                },
            )
            .await?;
            io.encode(
                &Decision::<&T, &str>::Chunks {
                    version: 1,
                    digest,
                    bytes: bytes.len() as u64,
                },
                None,
                true,
            )?
        } else {
            bytes
        };
        let request = AppendRequest {
            path: Self::path(agent)?,
            records: vec![AppendRecord {
                value: bytes,
                headers,
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
        let mut decision: Decision = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if let Decision::Chunks {
            version,
            digest,
            bytes,
        } = decision
        {
            if version != 1 {
                return Err(Error::Unsupported(
                    "settings decisions require format v1".into(),
                ));
            }
            let before = io.owned_bytes()?;
            let path = Self::payload_path(agent, &digest, Some(&io))?;
            let _path_hold = io.guard_owned(io.owned_since(before)?)?;
            let (body, _hold) =
                crate::store::operations::read_chunks(&self.provider, &path, bytes, &digest, &io)
                    .await?;
            decision =
                serde_json::from_slice(&body).map_err(|error| Error::Storage(error.to_string()))?;
        }
        let Decision::Inline {
            version: format,
            agent: owner,
            cut: source_cut,
            admission,
        } = decision
        else {
            return Err(Error::Storage(
                "settings payload does not contain a complete decision".into(),
            ));
        };
        if format != 1 {
            return Err(Error::Unsupported(
                "settings decisions require format v1".into(),
            ));
        }
        if owner != agent
            || source_cut != cut
            || admission.path != path
            || admission.version != version
        {
            return Err(Error::Storage(
                "settings decision belongs to another query".into(),
            ));
        }
        Ok(Some(admission))
    }

    fn payload_path(
        agent: AgentId,
        digest: &[u8; 32],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<StreamPath> {
        crate::executor::with_history_io(io, |work| {
            let path = crate::executor::preparation_format(
                format_args!(
                    "harness/settings-admissions/v1/{agent}/values/{}",
                    blake3::Hash::from_bytes(*digest)
                ),
                work,
            )?;
            let _hold = io
                .map(|io| io.guard_owned(path.capacity() as u64))
                .transpose()?;
            crate::distributed::stream_path(&path, work)
        })
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
