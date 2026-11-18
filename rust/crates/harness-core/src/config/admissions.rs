//! Durable settings evidence before an agent root exists. Ordinary Streams
//! reads and follows provide the same paged, publication-driven query.
use super::{Admission, AdmissionStatus};
use crate::{AgentId, Error, Result};
use acyclic_stream::{
    AppendOutcome, AppendRecord, AppendRequest, HeadProjection, HeadsRequest, IdempotencyKey,
    NativeStreamProvider, ReadKeySeek, ReadRequest, RecordHeaders, RevisionCut, StreamPath,
};
use std::sync::Arc;

/// The one persisted format, written and accepted by every path here. Earlier
/// formats are not read: a decision is evidence of one startup, not state.
const FORMAT: u8 = 2;
const ROOT: &str = "harness/settings-admissions/v2";
const SELECTION: &str = "acyclic.settings-admission/v2";
const LATEST: &str = "acyclic.settings-admission-path/v2";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "storage", rename_all = "snake_case", deny_unknown_fields)]
enum Decision<T = Outcome<String, String>, C = String> {
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

/// A stored outcome carries no time of its own. Its time is the commit time
/// of the record that first published it, so a repeat cannot differ from it.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Outcome<S, E> {
    path: S,
    version: S,
    status: AdmissionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<E>,
}

/// Serializes the caller's diagnostic directly, without an owned copy.
struct Shown<'a>(&'a (dyn std::fmt::Display + Sync));

impl serde::Serialize for Shown<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self.0)
    }
}

/// Owner-bound settings decisions, retained even when session startup refuses.
/// The provider owns authority to this data; no task volume grant is delegated.
///
/// It borrows the owner's stream and never holds a handle of its own. A
/// stream handle keeps its local root's ownership, so an owned clone here
/// could outlive its harness and make that root's next open wait forever.
/// The borrow makes such a clone a compile error instead.
pub struct SettingsAdmissions<'a, P: ?Sized = dyn NativeStreamProvider> {
    provider: &'a Arc<P>,
}

impl<P: ?Sized> Clone for SettingsAdmissions<'_, P> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P: ?Sized> Copy for SettingsAdmissions<'_, P> {}

impl<'a, P: crate::store::JournalProvider + ?Sized> SettingsAdmissions<'a, P> {
    /// Uses the original owner's Streams consistency and native funding domain.
    #[must_use]
    pub const fn new(provider: &'a Arc<P>) -> Self {
        Self { provider }
    }

    /// Retains this actual decision before startup returns success or refusal.
    ///
    /// An unchanged decision appends nothing; a changed one is ordered after
    /// the decision it replaces. "Unchanged" means byte-equal to the latest
    /// decision for the same settings path, whatever its cut or version, so
    /// returning to an earlier outcome is a change and is retained.
    ///
    /// The stream tail is observed first, and the latest decision for the path
    /// is then read strictly before that tail at the same snapshot. The
    /// comparison therefore sees exactly the history that the append, made
    /// conditional on that tail, extends. A decision published between the two
    /// moves the tail, so the append refuses with a tail conflict and the
    /// comparison repeats against the newer decision. Reading the keyed latest
    /// first would let such a decision slip in unseen and be overtaken.
    pub async fn record(
        &self,
        agent: AgentId,
        (path, cut, version): (&str, &str, &str),
        (status, error): (AdmissionStatus, Option<&(dyn std::fmt::Display + Sync)>),
        maximum_bytes: Option<u64>,
        io: &crate::executor::PreparationIo,
    ) -> Result<()> {
        // The caller lends its diagnostic, rather than allocating an owned DTO.
        let outcome = Outcome {
            path,
            version,
            status,
            error: error.map(Shown),
        };
        if maximum_bytes.is_some() {
            io.encode(&outcome, maximum_bytes, false)?;
        }
        let selection = key(SELECTION, path, Some((cut, version)), Some(io))?;
        let latest = key(LATEST, path, None, Some(io))?;
        let _key_hold = io.reserve_decode(2 * selection.len() as u64)?;
        io.allocation_attempt(selection.len() as u64)?;
        io.allocation_attempt(latest.len() as u64)?;
        let headers = headers([selection, latest])?;
        let bytes = io.encode(
            &Decision::Inline {
                version: FORMAT,
                agent,
                cut,
                admission: &outcome,
            },
            None,
            true,
        )?;
        let digest = crate::model::hash_bytes(&bytes, Some(io))?;
        let chunked =
            bytes.len().saturating_add(headers.payload_bytes()) > acyclic_stream::MAX_RECORD_BYTES;
        let value = if chunked {
            io.encode(
                &Decision::<&Outcome<&str, Shown<'_>>, &str>::Chunks {
                    version: FORMAT,
                    digest,
                    bytes: bytes.len() as u64,
                },
                None,
                true,
            )?
        } else {
            bytes.clone()
        };
        let stream = SettingsAdmissions::path(agent)?;
        let mut history = io.history_io();
        let mut staged = false;
        loop {
            let request = HeadsRequest {
                at: None,
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::Tail,
                paths: std::iter::once(stream.clone()).collect(),
                children: None,
                limit: 1,
                at_least: Default::default(),
            };
            let heads = crate::store::operations::heads_with_work(
                self.provider.as_ref(),
                &request,
                &mut Some(&mut history),
            )
            .await?;
            let tail = heads
                .heads
                .first()
                .and_then(Option::as_ref)
                .map_or(0, |head| head.tail);
            let at_least = RevisionCut::of(heads.snapshot.clone());
            drop(heads);
            if tail != 0 {
                let current = crate::store::operations::one_selected_record(
                    self.provider.as_ref(),
                    ReadRequest {
                        key: Some(ReadKeySeek::latest(latest.to_vec(), tail)),
                        path: stream.clone(),
                        from: 0,
                        limit: 1,
                        at_least,
                    },
                    &mut Some(&mut history),
                )
                .await?;
                if current.is_some_and(|record| record.value == value) {
                    return Ok(());
                }
            }
            if chunked && !staged {
                self.stage(agent, &digest, &bytes, io).await?;
                staged = true;
            }
            // The tail names this append's place in history, so a provider
            // retry replays it, while the same decision placed later after a
            // change is a new append. The digest gives concurrent different
            // decisions at one tail distinct identities, so the loser sees
            // a tail conflict rather than an idempotency mismatch.
            let identity = crate::executor::with_history_io(Some(io), |work| {
                crate::executor::preparation_format(
                    format_args!(
                        "settings-admission:v{FORMAT}:{agent}:{tail}:{}",
                        blake3::Hash::from_bytes(digest)
                    ),
                    work,
                )
            })?;
            let _identity_hold = io.guard_owned(identity.capacity() as u64)?;
            let request = AppendRequest {
                path: stream.clone(),
                records: vec![AppendRecord {
                    value: value.clone(),
                    headers: headers.clone(),
                }],
                if_tail: Some(tail),
                idempotency_key: IdempotencyKey::new(identity).map_err(invalid)?,
            };
            match &*crate::store::operations::append_with_work(
                self.provider.as_ref(),
                request,
                &mut Some(&mut history),
            )
            .await?
            {
                AppendOutcome::Committed(_) => return Ok(()),
                AppendOutcome::TailConflict { .. } => {}
            }
        }
    }

    /// Stages a decision too large for one record as content-addressed chunks
    /// before its indexed reference is published.
    async fn stage(
        &self,
        agent: AgentId,
        digest: &[u8; 32],
        bytes: &[u8],
        io: &crate::executor::PreparationIo,
    ) -> Result<()> {
        let before = io.owned_bytes()?;
        let path = SettingsAdmissions::payload_path(agent, digest, Some(io))?;
        let _path_hold = io.guard_owned(io.owned_since(before)?)?;
        crate::store::operations::stage_chunks(
            self.provider.as_ref(),
            &path,
            bytes,
            Some(io),
            |sequence| {
                let mut chunk = [0; 40];
                chunk[..32].copy_from_slice(digest);
                chunk[32..].copy_from_slice(&sequence.to_be_bytes());
                let digest = crate::model::hash_bytes(&chunk, Some(io))?;
                let key = crate::executor::with_history_io(Some(io), |work| {
                    crate::executor::preparation_format(
                        format_args!(
                            "settings-admission-chunk:v{FORMAT}:{}",
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
        .await
    }

    /// Executes the same bounded query and verifies its original identity.
    /// The decision's time is the commit time of the record that published it.
    pub async fn latest(
        &self,
        agent: AgentId,
        path: &str,
        cut: &str,
        version: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Admission>> {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let request = SettingsAdmissions::query(agent, path, cut, version, Some(&io))?;
        let mut observer = io.history_io();
        let Some(record) = crate::store::operations::one_selected_record(
            self.provider.as_ref(),
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
            if version != FORMAT {
                return Err(unsupported());
            }
            let before = io.owned_bytes()?;
            let path = SettingsAdmissions::payload_path(agent, &digest, Some(&io))?;
            let _path_hold = io.guard_owned(io.owned_since(before)?)?;
            let (body, _hold) = crate::store::operations::read_chunks(
                self.provider.as_ref(),
                &path,
                bytes,
                &digest,
                &io,
            )
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
        if format != FORMAT {
            return Err(unsupported());
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
        Ok(Some(Admission {
            path: admission.path,
            version: admission.version,
            status: admission.status,
            error: admission.error,
            at: super::utc(record.committed_at_micros / 1000),
        }))
    }
}

impl SettingsAdmissions<'_> {
    /// The ordinary stream to read in bounded pages or follow from a retained cut.
    pub fn path(agent: AgentId) -> Result<StreamPath> {
        StreamPath::new(format!("{ROOT}/{agent}")).map_err(invalid)
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
                key(SELECTION, path, Some((cut, version)), io)?.to_vec(),
                u64::MAX,
            )),
            path: SettingsAdmissions::path(agent)?,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
    }

    fn payload_path(
        agent: AgentId,
        digest: &[u8; 32],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<StreamPath> {
        crate::executor::with_history_io(io, |work| {
            let path = crate::executor::preparation_format(
                format_args!(
                    "{ROOT}/{agent}/values/{}",
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

/// The indexed identity of a settings path, alone or with its complete cut
/// and file version. Each domain separates the two key spaces.
fn key(
    domain: &'static str,
    path: &str,
    selection: Option<(&str, &str)>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<[u8; 32]> {
    crate::executor::with_history_io(io, |work| {
        crate::conversation::validate_content_path(path, work)?;
        let (cut, version) = selection.unwrap_or_default();
        if selection.is_some() && (cut.is_empty() || version.is_empty()) {
            return Err(Error::Invalid("settings cut or version is empty".into()));
        }
        let length = crate::core::owned_sum([
            domain.len() as u64,
            16,
            path.len() as u64,
            cut.len() as u64,
            version.len() as u64,
        ])?;
        if let Some(work) = work.as_deref_mut() {
            work.hashed_bytes(length)?;
        }
        let mut hash = blake3::Hasher::new_derive_key(domain);
        hash.update(&(path.len() as u64).to_be_bytes());
        hash.update(path.as_bytes());
        if selection.is_some() {
            hash.update(&(cut.len() as u64).to_be_bytes());
            hash.update(cut.as_bytes());
            hash.update(version.as_bytes());
        }
        Ok(*hash.finalize().as_bytes())
    })
}

/// Both indexed keys of one decision, in the canonical order Streams requires.
fn headers(mut keys: [[u8; 32]; 2]) -> Result<RecordHeaders> {
    keys.sort_unstable();
    RecordHeaders::new(keys.iter().map(|key| key.to_vec().into()).collect(), vec![])
        .map_err(invalid)
}

fn unsupported() -> Error {
    Error::Unsupported(format!("settings decisions require format v{FORMAT}"))
}

fn invalid(error: acyclic_stream::StreamError) -> Error {
    Error::Invalid(error.to_string())
}

#[cfg(all(test, not(loom)))]
mod tests;
