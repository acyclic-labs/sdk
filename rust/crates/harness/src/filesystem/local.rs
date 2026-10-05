//! Durable local Harness storage composed from the canonical Rust providers.

use super::workspace_ref;
use super::{FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost};
use crate::{
    AgentId, Capabilities, Error, OperationId, Result,
    conversation::{
        ContentResidencyVerifier, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer, Scope},
    executor::ExecutionJournal,
    resources::ProviderRef,
};
use acyclic_fs::{Fs, LocalAuthorityBackend, LocalObjectBackend, LocalOptions};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

type LocalHost = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
type LocalJournal =
    FilesystemExecutionJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

/// Durable Harness storage for one agent, backed by the Rust local Filesystem
/// and Stream providers. The journal and private content roots are reopened
/// from disk, so a new process can resume the same operation identity.
pub struct LocalHarnessStorage {
    journal: Arc<LocalJournal>,
    volume: VolumeRef,
    scope: Scope,
    root: PathBuf,
}

impl LocalHarnessStorage {
    /// Opens (or creates) durable Harness state below `root`.
    pub async fn open(
        root: impl AsRef<Path>,
        agent: AgentId,
        maximum_file_bytes: u64,
    ) -> Result<Self> {
        if maximum_file_bytes == 0 {
            return Err(Error::Invalid(
                "local Harness file limit must be positive".into(),
            ));
        }
        let root = root.as_ref().to_path_buf();
        let provider = ProviderRef::new("local-harness", "filesystem", "2")?;
        let filesystem = Fs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(format!("local filesystem open failed: {error}")))?;
        let host: Arc<LocalHost> = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
        let volume = VolumeRef::new(
            provider,
            format!("agent-{}", agent),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        // Reopening a durable agent must be idempotent. A prior process may
        // already have materialized the workspace; resolving it keeps the
        // same authenticated volume identity without creating a fork.
        if host
            .resolve(&workspace_ref(
                host.provider.clone(),
                &volume.storage_name()?,
            )?)
            .await
            .is_err()
        {
            host.create_volume(&volume).await?;
        }

        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: format!("local-harness:{agent}"),
        };
        let key = *blake3::hash(&agent.to_string().into_bytes()).as_bytes();
        let issuer = AuthorityIssuer::new("local-harness", key, conversation);
        let scope = issuer.root_for_agent(
            agent,
            "owner",
            Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "conversation:select_context".to_owned(),
                "interaction:open".to_owned(),
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let stream_root = root.join("journal");
        let local_stream = LocalStream::open(stream_root, LocalStreamLimits::default())
            .await
            .map_err(|error| Error::Storage(format!("local journal open failed: {error}")))?;
        let stream = StreamClient::new(Arc::new(local_stream));
        let input_verifier: Arc<dyn ContentResidencyVerifier> =
            Arc::new(FilesystemContentVerifier::new(
                Arc::clone(&host),
                issuer.verifier(),
                scope.clone(),
                maximum_file_bytes,
            )?);
        let journal = Arc::new(
            FilesystemExecutionJournal::new(
                stream.clone(),
                Arc::clone(&host),
                volume.clone(),
                issuer.verifier(),
                scope.clone(),
                maximum_file_bytes,
            )?
            .with_input_verifier(input_verifier),
        );
        Ok(Self {
            journal,
            volume,
            scope,
            root,
        })
    }

    /// Returns the Rust-owned durable execution journal.
    #[must_use]
    pub fn journal(&self) -> Arc<dyn ExecutionJournal> {
        self.journal.clone()
    }

    /// Returns the durable storage root used by this handle.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns this storage's stable private volume identity.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Returns the authenticated scope bound to the journal.
    #[must_use]
    pub const fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Reopens the same durable Stream provider and checks that the journal's
    /// operation records survive the handle boundary.
    pub async fn replay(
        &self,
        operation: OperationId,
    ) -> Result<Vec<crate::executor::ExecutionRecord>> {
        self.journal.replay(operation).await
    }
}
