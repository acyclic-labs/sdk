//! Native directory sources over the canonical checkout and watcher engines.

use crate::foundation::{Digest, Epoch, Head, OperationId, ProposedCommit, Sequence};
use crate::kernel::{
    DurableSourceMode, DurableSourceState, RebaseDecision, SourceFact, SourceInvalidation,
    decode_source_fact, encode_source_fact, source_authority_id,
};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, CancellationToken, CaptureOptions,
    Checkout, CheckoutCommitOutcome, CreateAuthorityOutcome, Fs, Generation, IdempotencyKey,
    NativeWatch, NativeWatchOptions, ReplayLimit, WatchBatch, WatchInvalidationReason, WorkBudget,
    Workspace,
};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

/// Whether an attached directory is fixed or continuously reconciled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceMode {
    /// Capture one exact baseline and require explicit rescans thereafter.
    Pinned,
    /// Track bounded native hints and fail closed when continuity is lost.
    Tracking,
}

/// Exact bounded native-source configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceOptions {
    /// Source advancement behavior.
    pub mode: SourceMode,
    /// Maximum paths in one authenticated baseline or reconciliation batch.
    pub maximum_paths: u32,
    /// Maximum sparse data spans admitted per regular file.
    pub maximum_extent_spans: u32,
    /// Maximum pending native hints before continuity is invalidated.
    pub maximum_queued_changes: u32,
}

impl Default for SourceOptions {
    fn default() -> Self {
        Self {
            mode: SourceMode::Tracking,
            maximum_paths: 262_144,
            maximum_extent_spans: 65_536,
            maximum_queued_changes: 65_536,
        }
    }
}

/// Durable semantic source state exposed to callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceState {
    /// Workspace and attached directory agree at the acknowledged cursor.
    Clean,
    /// A bounded host interval is being authenticated and captured.
    PendingCapture,
    /// Native continuity was lost; automatic advancement is stopped.
    NeedsRescan(WatchInvalidationReason),
    /// Source changes overlap independent workspace publication.
    Conflict,
    /// The returned generation is independent of this source.
    Sealed,
}

/// Terminal source reconciliation result.
pub enum ReconcileOutcome<A, O> {
    /// Source changes, if any, are durably represented by this generation.
    Clean(Generation<A, O>),
    /// Continuity is lost and an explicit full rescan is required.
    NeedsRescan(WatchInvalidationReason),
    /// Concurrent workspace changes overlap source capture.
    Conflict,
}

/// Fail-closed attached-source failure.
#[derive(Debug, Error)]
pub enum SourceError {
    /// Source configuration is empty or exceeds workspace bounds.
    #[error("source options are invalid")]
    InvalidOptions,
    /// Pinned sources do not admit automatic reconciliation.
    #[error("pinned source requires an explicit rescan")]
    Pinned,
    /// An existing durable source is bound to different semantics or identity.
    #[error("source binding does not match the durable workspace source")]
    BindingMismatch,
    /// Another process changed source ownership or state first.
    #[error("source state changed concurrently")]
    Concurrent,
    /// Native watcher, capture, authority, or immutable storage failed.
    #[error("source operation failed: {0}")]
    Engine(String),
}

struct SourceSession<A, O> {
    workspace: Workspace<A, O>,
    checkout: Checkout<A, O>,
    watcher: NativeWatch,
    capture: CaptureOptions,
    mode: SourceMode,
    maximum_queued_changes: u32,
    state: SourceState,
    authority_head: Head,
}

/// Cloneable handle to one exact attached native directory.
pub struct Source<A, O> {
    inner: Arc<Mutex<SourceSession<A, O>>>,
}

impl<A, O> Clone for Source<A, O> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Source<A, O> {
    /// Returns the current fail-closed semantic state.
    pub async fn state(&self) -> SourceState {
        self.inner.lock().await.state
    }

    /// Captures one bounded contiguous watcher interval and publishes it.
    ///
    /// # Errors
    ///
    /// Rejects pinned sources and propagates exact native/canonical failures.
    pub async fn reconcile(&self) -> Result<ReconcileOutcome<A, O>, SourceError> {
        let mut session = self.inner.lock().await;
        if session.mode == SourceMode::Pinned {
            return Err(SourceError::Pinned);
        }
        let maximum_paths = session.capture.maximum_paths;
        let batch = session
            .watcher
            .poll(
                maximum_paths,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .map_err(engine)?
            .value;
        if let WatchBatch::RescanRequired { reason, .. } = batch {
            persist_session_state(&mut session, SourceState::NeedsRescan(reason)).await?;
            return Ok(ReconcileOutcome::NeedsRescan(reason));
        }
        persist_session_state(&mut session, SourceState::PendingCapture).await?;
        let capture = session.capture.clone();
        crate::capture_watch_batch(
            &mut session.checkout,
            batch,
            &capture,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?;
        publish_session(&mut session).await
    }

    /// Rebuilds one complete authenticated baseline while preserving events
    /// that arrive concurrently with the scan.
    ///
    /// # Errors
    ///
    /// Returns native, capture, authority, or storage failures without
    /// acknowledging an incomplete interval.
    pub async fn rescan(&self) -> Result<ReconcileOutcome<A, O>, SourceError> {
        let mut session = self.inner.lock().await;
        rescan_session(&mut session).await
    }

    /// Produces an immutable generation whose bytes no longer depend on the
    /// attached directory.
    ///
    /// # Errors
    ///
    /// Fails unless a complete authenticated rescan and publication succeeds.
    pub async fn seal(&self) -> Result<Generation<A, O>, SourceError> {
        let mut session = self.inner.lock().await;
        let generation = match rescan_session(&mut session).await? {
            ReconcileOutcome::Clean(generation) => generation,
            ReconcileOutcome::NeedsRescan(_) | ReconcileOutcome::Conflict => {
                return Err(SourceError::Engine(
                    "source could not reach a clean sealed state".to_owned(),
                ));
            }
        };
        persist_session_state(&mut session, SourceState::Sealed).await?;
        Ok(generation)
    }
}

async fn rescan_session<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
) -> Result<ReconcileOutcome<A, O>, SourceError> {
    persist_session_state(session, SourceState::PendingCapture).await?;
    session.watcher.begin_rescan().map_err(engine)?;
    let capture = crate::capture_baseline(
        &mut session.checkout,
        &session.capture,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await;
    if let Err(error) = capture {
        let _ = session
            .watcher
            .abort_rescan(WatchInvalidationReason::BackendError);
        let _ = persist_session_state(
            session,
            SourceState::NeedsRescan(WatchInvalidationReason::BackendError),
        )
        .await;
        return Err(engine(error));
    }
    let trailing = session.watcher.finish_rescan().map_err(engine)?;
    if let WatchBatch::RescanRequired { reason, .. } = trailing {
        persist_session_state(session, SourceState::NeedsRescan(reason)).await?;
        return Ok(ReconcileOutcome::NeedsRescan(reason));
    }
    let capture = session.capture.clone();
    crate::capture_watch_batch(
        &mut session.checkout,
        trailing,
        &capture,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .map_err(engine)?;
    publish_session(session).await
}

async fn publish_session<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
) -> Result<ReconcileOutcome<A, O>, SourceError> {
    if session.checkout.has_pending_mutations() {
        let operation_id = IdempotencyKey::new().operation_id();
        let outcome = session
            .checkout
            .commit(
                operation_id,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(engine)?
            .value;
        match outcome {
            CheckoutCommitOutcome::Committed { generation_id, .. }
            | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
                persist_session_state(session, SourceState::Clean).await?;
                return Ok(ReconcileOutcome::Clean(Generation {
                    workspace: session.workspace.clone(),
                    id: generation_id,
                }));
            }
            CheckoutCommitOutcome::Conflict { .. } => {
                let rebased = session
                    .checkout
                    .rebase_head(
                        session
                            .workspace
                            .volume
                            .config()
                            .limits
                            .maximum_checkout_dependencies,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(engine)?
                    .value;
                if matches!(rebased, RebaseDecision::Conflicted { .. }) {
                    persist_session_state(session, SourceState::Conflict).await?;
                    return Ok(ReconcileOutcome::Conflict);
                }
                let retried = session
                    .checkout
                    .commit(
                        operation_id,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(engine)?
                    .value;
                match retried {
                    CheckoutCommitOutcome::Committed { generation_id, .. }
                    | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
                        persist_session_state(session, SourceState::Clean).await?;
                        return Ok(ReconcileOutcome::Clean(Generation {
                            workspace: session.workspace.clone(),
                            id: generation_id,
                        }));
                    }
                    CheckoutCommitOutcome::Conflict { .. }
                    | CheckoutCommitOutcome::Fenced { .. } => {
                        persist_session_state(session, SourceState::Conflict).await?;
                        return Ok(ReconcileOutcome::Conflict);
                    }
                    CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                        return Err(SourceError::Engine(
                            "source publication identity conflicted".to_owned(),
                        ));
                    }
                }
            }
            CheckoutCommitOutcome::Fenced { .. } => {
                persist_session_state(session, SourceState::Conflict).await?;
                return Ok(ReconcileOutcome::Conflict);
            }
            CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                return Err(SourceError::Engine(
                    "source publication identity conflicted".to_owned(),
                ));
            }
        }
    }
    let generation = session.workspace.head().await.map_err(engine)?;
    persist_source_fact(session, SourceState::Clean, generation.id).await?;
    Ok(ReconcileOutcome::Clean(generation))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Creates or opens one workspace and attaches an exact native directory.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds, unsafe roots, unsupported host semantics, or a
    /// baseline that cannot be atomically authenticated and published.
    pub async fn attach_directory(
        &self,
        name: impl AsRef<str>,
        path: impl AsRef<Path>,
        options: SourceOptions,
    ) -> Result<Workspace<A, O>, SourceError> {
        if options.maximum_paths == 0
            || options.maximum_extent_spans == 0
            || options.maximum_queued_changes == 0
        {
            return Err(SourceError::InvalidOptions);
        }
        let workspace = self.create_workspace(name).await.map_err(engine)?;
        let watcher = NativeWatch::open(
            path.as_ref(),
            NativeWatchOptions {
                limits: workspace.volume.config().limits,
                maximum_queued_changes: options.maximum_queued_changes,
                recursive: true,
            },
        )
        .map_err(engine)?;
        let capture = CaptureOptions {
            source_root: PathBuf::from(path.as_ref()),
            expected_root_identity: watcher.root_identity(),
            maximum_paths: options.maximum_paths,
            maximum_extent_spans: options.maximum_extent_spans,
        };
        let generation = workspace.head().await.map_err(engine)?;
        let authority_head =
            attach_source_authority(&workspace, watcher.root_identity(), options, generation.id)
                .await?;
        let checkout = workspace
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(engine)?
            .value;
        let source = Source {
            inner: Arc::new(Mutex::new(SourceSession {
                workspace: workspace.clone(),
                checkout,
                watcher,
                capture,
                mode: options.mode,
                maximum_queued_changes: options.maximum_queued_changes,
                state: SourceState::NeedsRescan(WatchInvalidationReason::InitialSnapshotRequired),
                authority_head,
            })),
        };
        match source.rescan().await? {
            ReconcileOutcome::Clean(_) => {}
            ReconcileOutcome::NeedsRescan(_) | ReconcileOutcome::Conflict => {
                return Err(SourceError::Engine(
                    "initial source baseline did not become clean".to_owned(),
                ));
            }
        }
        Ok(Workspace {
            source: Some(source),
            ..workspace
        })
    }
}

async fn attach_source_authority<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    root_identity: crate::NativeRootIdentity,
    options: SourceOptions,
    generation_id: crate::GenerationId,
) -> Result<Head, SourceError> {
    let authority_id = source_authority_id(workspace.volume.id());
    let created = workspace
        .volume
        .fs
        .authority()
        .create_authority(
            authority_id,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?
        .value;
    let head = match created {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    if head.sequence != Sequence::GENESIS {
        let latest = replay_source_fact(workspace, head).await?;
        if latest.volume_id != workspace.volume.id()
            || latest.root_identity != root_identity.to_bytes()
            || latest.mode != durable_mode(options.mode)
            || latest.maximum_paths != options.maximum_paths
            || latest.maximum_extent_spans != options.maximum_extent_spans
            || latest.maximum_queued_changes != options.maximum_queued_changes
        {
            return Err(SourceError::BindingMismatch);
        }
    }
    append_source_fact(
        workspace,
        head,
        SourceFact {
            volume_id: workspace.volume.id(),
            root_identity: root_identity.to_bytes(),
            mode: durable_mode(options.mode),
            maximum_paths: options.maximum_paths,
            maximum_extent_spans: options.maximum_extent_spans,
            maximum_queued_changes: options.maximum_queued_changes,
            state: DurableSourceState::NeedsRescan(SourceInvalidation::InitialSnapshotRequired),
            generation_id,
        },
    )
    .await
}

async fn replay_source_fact<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    head: Head,
) -> Result<SourceFact, SourceError> {
    let after = Sequence::new(head.sequence.get().saturating_sub(1));
    let records = workspace
        .volume
        .fs
        .authority()
        .replay(
            source_authority_id(workspace.volume.id()),
            after,
            ReplayLimit {
                records: 1,
                payload_bytes: 4096,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?
        .value;
    let record = records.first().ok_or_else(|| {
        SourceError::Engine("source authority head has no terminal fact".to_owned())
    })?;
    decode_source_fact(&record.payload, 4096).map_err(engine)
}

async fn persist_session_state<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
) -> Result<(), SourceError> {
    let generation_id = session.checkout.generation_id();
    persist_source_fact(session, state, generation_id).await
}

async fn persist_source_fact<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
    generation_id: crate::GenerationId,
) -> Result<(), SourceError> {
    let fact = SourceFact {
        volume_id: session.workspace.volume.id(),
        root_identity: session.watcher.root_identity().to_bytes(),
        mode: durable_mode(session.mode),
        maximum_paths: session.capture.maximum_paths,
        maximum_extent_spans: session.capture.maximum_extent_spans,
        maximum_queued_changes: session.maximum_queued_changes,
        state: durable_state(state),
        generation_id,
    };
    session.authority_head =
        append_source_fact(&session.workspace, session.authority_head, fact).await?;
    session.state = state;
    Ok(())
}

async fn append_source_fact<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    expected: Head,
    fact: SourceFact,
) -> Result<Head, SourceError> {
    let authority_id = source_authority_id(workspace.volume.id());
    let payload = encode_source_fact(fact).map_err(engine)?;
    let mut fingerprint_hasher = blake3::Hasher::new();
    fingerprint_hasher.update(b"acyclic-fs-source-fingerprint-v1\0");
    fingerprint_hasher.update(&payload);
    let fingerprint = Digest::from_bytes(*fingerprint_hasher.finalize().as_bytes());
    let mut operation_hasher = blake3::Hasher::new();
    operation_hasher.update(b"acyclic-fs-source-operation-v1\0");
    operation_hasher.update(&authority_id.into_bytes());
    operation_hasher.update(&expected.epoch.get().to_le_bytes());
    operation_hasher.update(&expected.sequence.get().to_le_bytes());
    operation_hasher.update(expected.digest.as_bytes());
    operation_hasher.update(fingerprint.as_bytes());
    let mut operation_bytes = [0_u8; 16];
    operation_bytes.copy_from_slice(&operation_hasher.finalize().as_bytes()[..16]);
    let outcome = workspace
        .volume
        .fs
        .authority()
        .compare_and_append(
            authority_id,
            expected.epoch,
            expected,
            ProposedCommit {
                operation_id: OperationId::from_bytes(operation_bytes),
                fingerprint,
                payload: Bytes::from(payload),
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?
        .value;
    match outcome {
        AppendOutcome::Committed(commit) | AppendOutcome::AlreadyCommitted(commit) => Ok(Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        }),
        AppendOutcome::Conflict { .. } | AppendOutcome::Fenced { .. } => {
            Err(SourceError::Concurrent)
        }
        AppendOutcome::IdempotencyConflict { .. } => Err(SourceError::Engine(
            "source state operation identity conflicted".to_owned(),
        )),
    }
}

const fn durable_mode(mode: SourceMode) -> DurableSourceMode {
    match mode {
        SourceMode::Pinned => DurableSourceMode::Pinned,
        SourceMode::Tracking => DurableSourceMode::Tracking,
    }
}

const fn durable_state(state: SourceState) -> DurableSourceState {
    match state {
        SourceState::Clean => DurableSourceState::Clean,
        SourceState::PendingCapture => DurableSourceState::PendingCapture,
        SourceState::NeedsRescan(reason) => DurableSourceState::NeedsRescan(match reason {
            WatchInvalidationReason::InitialSnapshotRequired => {
                SourceInvalidation::InitialSnapshotRequired
            }
            WatchInvalidationReason::QueueOverflow => SourceInvalidation::QueueOverflow,
            WatchInvalidationReason::NativeRescanRequired => {
                SourceInvalidation::NativeRescanRequired
            }
            WatchInvalidationReason::BackendError => SourceInvalidation::BackendError,
            WatchInvalidationReason::UnrepresentablePath => SourceInvalidation::UnrepresentablePath,
            WatchInvalidationReason::AmbiguousRename => SourceInvalidation::AmbiguousRename,
            WatchInvalidationReason::RootChanged => SourceInvalidation::RootChanged,
        }),
        SourceState::Conflict => DurableSourceState::Conflict,
        SourceState::Sealed => DurableSourceState::Sealed,
    }
}

fn engine(error: impl std::fmt::Display) -> SourceError {
    SourceError::Engine(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn attached_directory_rescans_and_seals_through_one_workspace()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("before.txt"), b"before")?;
        let workspace =
            Box::pin(Fs::memory().attach_directory("repo", root.path(), SourceOptions::default()))
                .await?;
        assert_eq!(
            workspace.read("/before.txt", 16).await?,
            Bytes::from_static(b"before")
        );
        let source = workspace.source().ok_or("source handle missing")?;
        assert_eq!(source.state().await, SourceState::Clean);

        std::fs::write(root.path().join("before.txt"), b"after")?;
        std::fs::write(root.path().join("added.txt"), b"added")?;
        assert!(matches!(
            Box::pin(source.rescan()).await?,
            ReconcileOutcome::Clean(_)
        ));
        assert_eq!(
            workspace.read("/before.txt", 16).await?,
            Bytes::from_static(b"after")
        );
        assert_eq!(
            workspace.read("/added.txt", 16).await?,
            Bytes::from_static(b"added")
        );
        let sealed = Box::pin(workspace.seal()).await?;
        assert_eq!(source.state().await, SourceState::Sealed);
        std::fs::remove_dir_all(root.path())?;
        assert_eq!(
            sealed.read("/before.txt", 16).await?,
            Bytes::from_static(b"after")
        );
        Ok(())
    }

    #[cfg(feature = "local")]
    #[tokio::test]
    async fn durable_source_restarts_fail_closed_and_remains_gc_classifiable()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = tempfile::tempdir()?;
        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("durable.txt"), b"one")?;
        let local_options = crate::LocalOptions::new(storage.path());

        {
            let fs = Fs::local(local_options.clone()).await?;
            let workspace = Box::pin(fs.attach_directory(
                "durable",
                source_root.path(),
                SourceOptions::default(),
            ))
            .await?;
            let source = workspace.source().ok_or("source missing")?;
            assert_eq!(source.state().await, SourceState::Clean);
        }

        std::fs::write(source_root.path().join("durable.txt"), b"two")?;
        {
            let fs = Fs::local(local_options.clone()).await?;
            let workspace = Box::pin(fs.attach_directory(
                "durable",
                source_root.path(),
                SourceOptions::default(),
            ))
            .await?;
            assert_eq!(
                Box::pin(workspace.read("/durable.txt", 8)).await?,
                Bytes::from_static(b"two")
            );
            let wrong_root = tempfile::tempdir()?;
            assert!(matches!(
                Box::pin(fs.attach_directory(
                    "durable",
                    wrong_root.path(),
                    SourceOptions::default()
                ))
                .await,
                Err(SourceError::BindingMismatch)
            ));
        }

        Ok(())
    }
}
