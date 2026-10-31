//! Compatibility state and lineage on the filesystem's original Streams provider.
use super::*;
use crate::distributed::state_record::{CompanionKind, StateRecord};
use crate::distributed::{AuthorityProvider, AuthorizedObservation};
use crate::{
    AuthorityStoreError, AuthorityWork, FilesystemCommit, FilesystemCommitOutcome,
    StreamAuthorityStore, WorkspaceLineageRecord, WorkspaceLineageStore,
};
use acyclic_stream::{CommitCondition, CommitRequest, StreamPath};

/// Durable command state shared by hosts using the same filesystem provider.
/// Each lookup reads one stream head and its last record, independent of history.
pub struct StreamGitCompatStore<P: ?Sized> {
    authority: StreamAuthorityStore<P>,
}

impl<P: ?Sized> Clone for StreamGitCompatStore<P> {
    fn clone(&self) -> Self {
        Self {
            authority: self.authority.clone(),
        }
    }
}

impl<P: ?Sized> StreamGitCompatStore<P> {
    /// Uses the same authority owner as the workspace generations.
    #[must_use]
    pub const fn new(authority: StreamAuthorityStore<P>) -> Self {
        Self { authority }
    }
}

/// Original storage and codec failures of stream-backed command state.
#[derive(Error)]
pub enum StreamGitCompatStoreError {
    /// The provider failed or could not resolve an exact append.
    #[error(transparent)]
    Authority(#[from] AuthorityStoreError),
    /// The stream primitive rejected a path or request.
    #[error(transparent)]
    Stream(#[from] acyclic_stream::StreamError),
    /// The existing compatibility codec refused state.
    #[error(transparent)]
    Codec(#[from] serde_json::Error),
    /// An unresolved publication retains its exact request and custody.
    #[error("stream-backed command publication remains unresolved: {error}")]
    Unknown {
        /// The original sealed publication; never reconstruct or re-fence it.
        commit: FilesystemCommit,
        /// Original submission failure.
        #[source]
        error: AuthorityStoreError,
    },
    /// A record cannot represent this workspace or revision.
    #[error("incompatible stream-backed command state")]
    Incompatible,
}

impl std::fmt::Debug for StreamGitCompatStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamGitCompatStoreError")
            .field("cause", &format_args!("{self}"))
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    version: u32,
    workspace: WorkspaceId,
    revision: u64,
    value: Option<T>,
}

impl<P: AuthorityProvider + ?Sized> StreamGitCompatStore<P> {
    fn path(id: WorkspaceId, family: &str) -> Result<StreamPath, StreamGitCompatStoreError> {
        Ok(StreamPath::new(format!(
            "fs/git-state-v1/{}/{family}",
            hex::encode(id.into_bytes())
        ))?)
    }

    async fn read<T: serde::de::DeserializeOwned>(
        &self,
        id: WorkspaceId,
        family: &str,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Option<Envelope<T>>, AuthorizedObservation), StreamGitCompatStoreError> {
        let observed = self
            .authority
            .observe_record(Self::path(id, family)?, work)
            .await?;
        let value = match observed.record {
            None => None,
            Some(record) => {
                let recorded = StateRecord::decode(record.value)?;
                let StateRecord::Companion {
                    kind: CompanionKind::GitRefs,
                    payload,
                } = recorded.state
                else {
                    return Err(StreamGitCompatStoreError::Incompatible);
                };
                let envelope: Envelope<T> = serde_json::from_slice(&payload)?;
                if envelope.version != 1 || envelope.workspace != id || envelope.revision == 0 {
                    return Err(StreamGitCompatStoreError::Incompatible);
                }
                Some(envelope)
            }
        };
        Ok((value, observed.observation))
    }

    async fn replace<T: Serialize + serde::de::DeserializeOwned>(
        &self,
        id: WorkspaceId,
        family: &str,
        expected: u64,
        replacement: Option<T>,
    ) -> Result<bool, StreamGitCompatStoreError> {
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let (current, observation) = self.read::<T>(id, family, &mut work).await?;
        // A tombstone is logically absent, but its exact physical head still
        // fences recreation. Old creates can never overwrite a later history.
        if current
            .as_ref()
            .filter(|entry| entry.value.is_some())
            .map_or(0, |entry| entry.revision)
            != expected
        {
            return Ok(false);
        }
        let revision = expected
            .checked_add(1)
            .ok_or(StreamGitCompatStoreError::Incompatible)?;
        let payload = serde_json::to_vec(&Envelope {
            version: 1,
            workspace: id,
            revision,
            value: replacement,
        })?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic-fs-stream-git-state-v1\0");
        match observation.condition() {
            CommitCondition::Absent { path } => {
                hash.update(&[0]);
                hash.update(path.as_str().as_bytes());
            }
            CommitCondition::Head {
                path,
                expected,
                commit_id,
            } => {
                hash.update(&[1]);
                hash.update(path.as_str().as_bytes());
                hash.update(&expected.to_le_bytes());
                hash.update(commit_id.as_bytes());
            }
            _ => return Err(StreamGitCompatStoreError::Incompatible),
        }
        hash.update(&payload);
        let key = acyclic_stream::IdempotencyKey::new(bytes::Bytes::copy_from_slice(
            hash.finalize().as_bytes(),
        ))?;
        let path = Self::path(id, family)?;
        let sequence = match observation.condition() {
            CommitCondition::Absent { .. } => 0,
            CommitCondition::Head { expected, .. } => *expected,
            _ => return Err(StreamGitCompatStoreError::Incompatible),
        };
        let record = StateRecord::Companion {
            kind: CompanionKind::GitRefs,
            payload: payload.into(),
        };
        let mut commit = FilesystemCommit::new(CommitRequest {
            idempotency_key: key.clone(),
            conditions: Vec::new(),
            mutations: Vec::new(),
            deadline_unix_millis: None,
        });
        self.authority.contribute_record(
            &mut commit,
            &observation,
            &record,
            crate::PublicationHold::none(),
            &mut work,
        )?;
        self.authority
            .seal_observed(&mut commit, &[], None, &mut work)?;
        match self.authority.submit(commit, &mut work).await? {
            FilesystemCommitOutcome::Committed(_) => Ok(true),
            FilesystemCommitOutcome::Rejected(_) => Ok(false),
            FilesystemCommitOutcome::Unknown { commit, error } => {
                // One exact-position observation resolves a lost reply. Failure
                // stays an error; no retry at a newer head or polling is allowed.
                match self
                    .authority
                    .reconcile_record(path, sequence, &key, &record, &mut work)
                    .await
                {
                    Ok(_) => {
                        drop(commit);
                        Ok(true)
                    }
                    Err(_) => Err(StreamGitCompatStoreError::Unknown { commit, error }),
                }
            }
        }
    }
}

impl<P: AuthorityProvider + ?Sized> GitCompatStore for StreamGitCompatStore<P> {
    type Error = StreamGitCompatStoreError;
    async fn load(&self, id: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let (value, _) = self.read::<GitCompatState>(id, "refs", &mut work).await?;
        match value {
            Some(Envelope {
                revision,
                value: Some(state),
                ..
            }) if state.revision != revision || state.version != STATE_VERSION => {
                Err(Self::Error::Incompatible)
            }
            Some(envelope) => Ok(envelope.value),
            None => Ok(None),
        }
    }
    async fn compare_and_swap(
        &self,
        id: WorkspaceId,
        expected: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        if replacement.version != STATE_VERSION
            || replacement.revision != expected.checked_add(1).ok_or(Self::Error::Incompatible)?
        {
            return Err(Self::Error::Incompatible);
        }
        self.replace(id, "refs", expected, Some(replacement)).await
    }
    async fn compare_and_delete(
        &self,
        id: WorkspaceId,
        expected: u64,
    ) -> Result<bool, Self::Error> {
        if expected == 0 {
            return Ok(false);
        }
        self.replace::<GitCompatState>(id, "refs", expected, None)
            .await
    }
}

#[cfg(test)]
mod tests;

impl<P: AuthorityProvider + ?Sized> WorkspaceLineageStore for StreamGitCompatStore<P> {
    type Error = StreamGitCompatStoreError;
    async fn load(&self, id: WorkspaceId) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let (value, _) = self
            .read::<WorkspaceLineageRecord>(id, "lineage", &mut work)
            .await?;
        match value {
            Some(Envelope {
                revision,
                value: Some(state),
                ..
            }) if state.revision != revision || state.workspace_id != id => {
                Err(Self::Error::Incompatible)
            }
            Some(envelope) => Ok(envelope.value),
            None => Ok(None),
        }
    }
    async fn compare_and_swap(
        &self,
        id: WorkspaceId,
        expected: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        if replacement.version != 1
            || replacement.workspace_id != id
            || replacement.revision != expected.checked_add(1).ok_or(Self::Error::Incompatible)?
        {
            return Err(Self::Error::Incompatible);
        }
        self.replace(id, "lineage", expected, Some(replacement))
            .await
    }
}
