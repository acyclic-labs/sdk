//! Native directory sources over the canonical checkout and watcher engines.

use crate::foundation::{Digest, Epoch, Head, OperationId, ProposedCommit, Sequence};
use crate::kernel::{
    DurableSourceMode, DurableSourceState, PublishGenerationRequest, RebaseDecision, SourceFact,
    SourceInvalidation, contextual_publication_fingerprint, decode_published_generation,
    decode_source_fact, encode_source_fact, source_authority_id, volume_authority_id,
};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::path::PortablePath;
use crate::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, CancellationToken, CaptureOptions,
    CapturePolicy, Checkout, CheckoutCommitOutcome, CreateAuthorityOutcome, Fs, Generation,
    IdempotencyKey, NativeWatch, NativeWatchOptions, ReplayLimit, WatchBatch,
    WatchInvalidationReason, WorkBudget, Workspace,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceOptions {
    /// Source advancement behavior.
    pub mode: SourceMode,
    /// Maximum paths in one authenticated baseline or reconciliation batch.
    pub maximum_paths: u32,
    /// Maximum sparse data spans admitted per regular file.
    pub maximum_extent_spans: u32,
    /// Maximum pending native hints before continuity is invalidated.
    pub maximum_queued_changes: u32,
    /// Canonical portable prefixes omitted from capture and deletion inference.
    pub excluded_paths: Vec<PortablePath>,
}

impl Default for SourceOptions {
    fn default() -> Self {
        Self {
            mode: SourceMode::Tracking,
            maximum_paths: 262_144,
            maximum_extent_spans: 65_536,
            maximum_queued_changes: 65_536,
            excluded_paths: Vec::new(),
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
    /// Source changes overlap independent workspace publication; reattachment
    /// is required to establish a new acknowledged base.
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
    /// Concurrent workspace changes overlap source capture; the source remains
    /// fail-closed until it is reattached.
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
    /// Source and workspace histories conflict; reattachment is required.
    #[error("source conflicts with the workspace and must be reattached")]
    Conflict,
    /// A retry identity is already bound to another source operation.
    #[error("source operation identity is already bound to another request")]
    IdempotencyConflict,
    /// Native watcher, capture, authority, or immutable storage failed.
    #[error("source operation failed: {0}")]
    Engine(String),
}

#[derive(Clone, Copy)]
enum SourceOperation {
    Reconcile,
    Rescan,
    Seal,
}

#[derive(Clone, Copy)]
enum SourceOperationStage {
    Begin,
    Terminal,
}

struct RecoveredSourceOperation {
    fact: SourceFact,
    head: Head,
}

struct SourceSession<A, O> {
    workspace: Workspace<A, O>,
    checkout: Checkout<A, O>,
    watcher: NativeWatch,
    capture: CaptureOptions,
    capture_policy: CapturePolicy,
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

    /// Re-establishes a clean source baseline after the core materializer has
    /// installed a published workspace generation into the attached root.
    ///
    /// A fresh checkout is opened at authenticated workspace HEAD and a full
    /// bounded rescan proves the resulting host tree. Any concurrent host
    /// change is captured as a later generation instead of being discarded.
    pub async fn acknowledge_materialization(
        &self,
        key: IdempotencyKey,
    ) -> Result<ReconcileOutcome<A, O>, SourceError> {
        let mut session = self.inner.lock().await;
        refresh_source_authority(&mut session).await?;
        session.checkout = session
            .workspace
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
        session.state = SourceState::NeedsRescan(WatchInvalidationReason::InitialSnapshotRequired);
        begin_source_operation(&mut session, key, SourceOperation::Rescan).await?;
        rescan_session(
            &mut session,
            key,
            SourceOperation::Rescan,
            SourceState::Clean,
        )
        .await
    }

    /// Captures one bounded contiguous watcher interval and publishes it.
    /// A clean interval still advances authority ordering to atomically prove
    /// that its acknowledged workspace generation was current.
    ///
    /// # Errors
    ///
    /// Rejects pinned sources and propagates exact native/canonical failures.
    pub async fn reconcile(&self) -> Result<ReconcileOutcome<A, O>, SourceError> {
        self.reconcile_with_key(IdempotencyKey::new()).await
    }

    /// Captures and publishes one watcher interval under a stable retry key.
    ///
    /// An exact retry returns the original durable outcome, including after a
    /// process restart. Reusing the key for another lifecycle operation is an
    /// idempotency conflict.
    ///
    /// # Errors
    ///
    /// Rejects pinned sources, conflicting retry identities, and exact
    /// native/canonical failures.
    pub async fn reconcile_with_key(
        &self,
        key: IdempotencyKey,
    ) -> Result<ReconcileOutcome<A, O>, SourceError> {
        let mut session = self.inner.lock().await;
        if let Some(outcome) =
            replay_reconcile_operation(&mut session, key, SourceOperation::Reconcile).await?
        {
            return Ok(outcome);
        }
        if session.state == SourceState::Conflict {
            return Ok(ReconcileOutcome::Conflict);
        }
        if session.mode == SourceMode::Pinned {
            return Err(SourceError::Pinned);
        }
        begin_source_operation(&mut session, key, SourceOperation::Reconcile).await?;
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
            let fact = persist_source_operation(
                &mut session,
                SourceState::NeedsRescan(reason),
                key,
                SourceOperation::Reconcile,
            )
            .await?;
            return reconcile_outcome_from_fact(&session, fact);
        }
        let capture = session.capture.clone();
        let policy = session.capture_policy.clone();
        crate::capture_watch_batch_with_policy(
            &mut session.checkout,
            batch,
            &capture,
            &policy,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?;
        publish_session(
            &mut session,
            key,
            SourceOperation::Reconcile,
            SourceState::Clean,
        )
        .await
    }

    /// Rebuilds one complete authenticated baseline while preserving events
    /// that arrive concurrently with the scan.
    ///
    /// # Errors
    ///
    /// Returns native, capture, authority, or storage failures without
    /// acknowledging an incomplete interval.
    pub async fn rescan(&self) -> Result<ReconcileOutcome<A, O>, SourceError> {
        self.rescan_with_key(IdempotencyKey::new()).await
    }

    /// Rebuilds a complete baseline under a stable retry key.
    ///
    /// # Errors
    ///
    /// Returns conflicting retry identities and exact native/canonical
    /// failures.
    pub async fn rescan_with_key(
        &self,
        key: IdempotencyKey,
    ) -> Result<ReconcileOutcome<A, O>, SourceError> {
        let mut session = self.inner.lock().await;
        if let Some(outcome) =
            replay_reconcile_operation(&mut session, key, SourceOperation::Rescan).await?
        {
            return Ok(outcome);
        }
        if session.state == SourceState::Conflict {
            return Ok(ReconcileOutcome::Conflict);
        }
        begin_source_operation(&mut session, key, SourceOperation::Rescan).await?;
        rescan_session(
            &mut session,
            key,
            SourceOperation::Rescan,
            SourceState::Clean,
        )
        .await
    }

    /// Produces an immutable generation whose bytes no longer depend on the
    /// attached directory.
    ///
    /// # Errors
    ///
    /// Fails unless a complete authenticated rescan and publication succeeds.
    pub async fn seal(&self) -> Result<Generation<A, O>, SourceError> {
        self.seal_with_key(IdempotencyKey::new()).await
    }

    /// Seals the source under a stable retry key.
    ///
    /// # Errors
    ///
    /// Returns conflicting retry identities and fails unless the original or
    /// recovered operation reached a clean sealed generation.
    pub async fn seal_with_key(
        &self,
        key: IdempotencyKey,
    ) -> Result<Generation<A, O>, SourceError> {
        let mut session = self.inner.lock().await;
        if let Some(generation) = replay_seal_operation(&mut session, key).await? {
            return Ok(generation);
        }
        if session.state == SourceState::Conflict {
            return Err(SourceError::Conflict);
        }
        begin_source_operation(&mut session, key, SourceOperation::Seal).await?;
        let generation = match rescan_session(
            &mut session,
            key,
            SourceOperation::Seal,
            SourceState::Sealed,
        )
        .await?
        {
            ReconcileOutcome::Clean(generation) => generation,
            ReconcileOutcome::Conflict => return Err(SourceError::Conflict),
            ReconcileOutcome::NeedsRescan(_) => {
                return Err(SourceError::Engine(
                    "source could not reach a clean sealed state".to_owned(),
                ));
            }
        };
        Ok(generation)
    }
}

async fn rescan_session<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    key: IdempotencyKey,
    operation: SourceOperation,
    terminal_state: SourceState,
) -> Result<ReconcileOutcome<A, O>, SourceError> {
    let trailing = {
        let mut attempt = 0;
        loop {
            session.watcher.begin_rescan().map_err(engine)?;
            let policy = session.capture_policy.clone();
            let capture = crate::capture_baseline_with_policy(
                &mut session.checkout,
                &session.capture,
                &policy,
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
                discard_unpublished_capture(session).await?;
                return Err(engine(error));
            }
            let batch = session.watcher.finish_rescan().map_err(engine)?;
            if matches!(
                batch,
                WatchBatch::RescanRequired {
                    reason: WatchInvalidationReason::NativeRescanRequired,
                    ..
                }
            ) && attempt < 3
            {
                // Native backends may deliver a coalesced hint from before
                // this scan after it completes. Never publish the partial
                // checkout; retry the baseline under the same operation key.
                discard_unpublished_capture(session).await?;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                attempt += 1;
                continue;
            }
            break batch;
        }
    };
    if let WatchBatch::RescanRequired { reason, .. } = trailing {
        let fact =
            persist_source_operation(session, SourceState::NeedsRescan(reason), key, operation)
                .await?;
        discard_unpublished_capture(session).await?;
        return reconcile_outcome_from_fact(session, fact);
    }
    let capture = session.capture.clone();
    let policy = session.capture_policy.clone();
    crate::capture_watch_batch_with_policy(
        &mut session.checkout,
        trailing,
        &capture,
        &policy,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .map_err(engine)?;
    publish_session(session, key, operation, terminal_state).await
}

async fn discard_unpublished_capture<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
) -> Result<(), SourceError> {
    let base = session.checkout.generation_id();
    session.checkout = session
        .workspace
        .volume
        .checkout(
            GenerationSelector::Exact(base),
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?
        .value;
    Ok(())
}

async fn publish_session<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    key: IdempotencyKey,
    operation: SourceOperation,
    terminal_state: SourceState,
) -> Result<ReconcileOutcome<A, O>, SourceError> {
    let had_pending_mutations = session.checkout.has_pending_mutations();
    let operation_id = source_volume_operation_id(key, operation);
    let operation_context = source_volume_operation_context(key, operation);
    let outcome = session
        .checkout
        .commit_with_context(
            operation_id,
            operation_context,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?
        .value;
    match outcome {
        CheckoutCommitOutcome::Committed { generation_id, .. }
        | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
            persist_generation_outcome(session, terminal_state, generation_id, key, operation).await
        }
        CheckoutCommitOutcome::Conflict { .. } if !had_pending_mutations => {
            persist_state_outcome(session, SourceState::Conflict, key, operation).await
        }
        CheckoutCommitOutcome::Conflict { actual } => {
            if conflict_head_reuses_checkout_base(
                &session.workspace,
                session.checkout.generation_id(),
                actual,
            )
            .await?
            {
                return persist_state_outcome(session, SourceState::Conflict, key, operation).await;
            }
            let rebased = rebase_source_checkout(session).await?;
            if matches!(rebased, RebaseDecision::Conflicted { .. }) {
                return persist_state_outcome(session, SourceState::Conflict, key, operation).await;
            }
            let retried = session
                .checkout
                .commit_with_context(
                    operation_id,
                    operation_context,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(engine)?
                .value;
            match retried {
                CheckoutCommitOutcome::Committed { generation_id, .. }
                | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
                    persist_generation_outcome(
                        session,
                        terminal_state,
                        generation_id,
                        key,
                        operation,
                    )
                    .await
                }
                CheckoutCommitOutcome::Conflict { .. } | CheckoutCommitOutcome::Fenced { .. } => {
                    persist_state_outcome(session, SourceState::Conflict, key, operation).await
                }
                CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                    Err(SourceError::IdempotencyConflict)
                }
            }
        }
        CheckoutCommitOutcome::Fenced { .. } => {
            persist_state_outcome(session, SourceState::Conflict, key, operation).await
        }
        CheckoutCommitOutcome::IdempotencyConflict { .. } => Err(SourceError::IdempotencyConflict),
    }
}

async fn conflict_head_reuses_checkout_base<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    checkout_generation: crate::GenerationId,
    actual: Head,
) -> Result<bool, SourceError> {
    let previous_sequence = actual.sequence.get().checked_sub(1).ok_or_else(|| {
        SourceError::Engine("conflicting volume head has no durable record".to_owned())
    })?;
    let records = workspace
        .volume
        .fs
        .authority()
        .replay(
            volume_authority_id(workspace.volume.id()),
            Sequence::new(previous_sequence),
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
        SourceError::Engine("conflicting volume head has no durable record".to_owned())
    })?;
    if record.epoch != actual.epoch
        || record.sequence != actual.sequence
        || record.digest != actual.digest
    {
        return Err(SourceError::Engine(
            "conflicting volume head does not match durable history".to_owned(),
        ));
    }
    let published = decode_published_generation(&record.payload, 4096).map_err(engine)?;
    if published.volume_id != workspace.volume.id() {
        return Err(SourceError::Engine(
            "conflicting volume publication belongs to another volume".to_owned(),
        ));
    }
    Ok(published.generation_root.digest == checkout_generation.digest())
}

async fn rebase_source_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
) -> Result<RebaseDecision, SourceError> {
    session
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
        .map(|receipt| receipt.value)
        .map_err(engine)
}

async fn persist_generation_outcome<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
    generation_id: crate::GenerationId,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<ReconcileOutcome<A, O>, SourceError> {
    let fact =
        persist_source_operation_with_generation(session, state, generation_id, key, operation)
            .await?;
    reconcile_outcome_from_fact(session, fact)
}

async fn persist_state_outcome<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<ReconcileOutcome<A, O>, SourceError> {
    let fact = persist_source_operation(session, state, key, operation).await?;
    reconcile_outcome_from_fact(session, fact)
}

fn reconcile_outcome_from_fact<A, O>(
    session: &SourceSession<A, O>,
    fact: SourceFact,
) -> Result<ReconcileOutcome<A, O>, SourceError> {
    match fact.state {
        DurableSourceState::Clean | DurableSourceState::Sealed => {
            Ok(ReconcileOutcome::Clean(Generation {
                workspace: session.workspace.clone(),
                id: fact.generation_id,
            }))
        }
        DurableSourceState::NeedsRescan(reason) => {
            Ok(ReconcileOutcome::NeedsRescan(watch_invalidation(reason)))
        }
        DurableSourceState::Conflict => Ok(ReconcileOutcome::Conflict),
        DurableSourceState::PendingCapture => Err(SourceError::Engine(
            "source operation has no terminal outcome".to_owned(),
        )),
    }
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
        let watcher = NativeWatch::open_with_profile(
            path.as_ref(),
            workspace.volume.config().profile,
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
        let capture_policy = CapturePolicy::excluding(
            options
                .excluded_paths
                .iter()
                .map(|path| {
                    crate::kernel::NamespacePath::from_portable_in_profile(
                        path,
                        workspace.volume.config().profile,
                        workspace.volume.config().limits,
                    )
                    .map_err(engine)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(engine)?;
        let generation = workspace.head().await.map_err(engine)?;
        let authority_head = attach_source_authority(
            &workspace,
            watcher.root_identity(),
            &options,
            capture_policy.fingerprint(),
            generation.id,
        )
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
                capture_policy,
                mode: options.mode,
                maximum_queued_changes: options.maximum_queued_changes,
                state: SourceState::NeedsRescan(WatchInvalidationReason::InitialSnapshotRequired),
                authority_head,
            })),
        };
        if !matches!(source.rescan().await?, ReconcileOutcome::Clean(_)) {
            return Err(SourceError::Engine(
                "initial source baseline did not become clean".to_owned(),
            ));
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
    options: &SourceOptions,
    capture_policy: Digest,
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
            || latest.capture_policy != capture_policy
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
            capture_policy,
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

async fn refresh_source_authority<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
) -> Result<(), SourceError> {
    let head = session
        .workspace
        .volume
        .fs
        .authority()
        .head(
            source_authority_id(session.workspace.volume.id()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?
        .value;
    if head == session.authority_head {
        return Ok(());
    }
    let fact = replay_source_fact(&session.workspace, head).await?;
    validate_source_fact_binding(session, &fact)?;
    session.authority_head = head;
    let state = source_state(fact.state);
    session.state = if fact.generation_id == session.checkout.generation_id() {
        state
    } else {
        SourceState::Conflict
    };
    Ok(())
}

async fn recover_published_operation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
    generation_id: crate::GenerationId,
    begin_head: Head,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<SourceFact, SourceError> {
    refresh_source_authority(session).await?;
    let operation_still_current = (session.state == SourceState::PendingCapture
        && session.authority_head == begin_head)
        || (session.state == SourceState::Clean
            && session.checkout.generation_id() == generation_id);
    let recovered_state = if operation_still_current {
        state
    } else {
        SourceState::Conflict
    };
    persist_source_operation_with_generation(
        session,
        recovered_state,
        generation_id,
        key,
        operation,
    )
    .await
}

async fn replay_reconcile_operation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<Option<ReconcileOutcome<A, O>>, SourceError> {
    let fact = if let Some(recovered) =
        find_source_operation(session, key, operation, SourceOperationStage::Terminal).await?
    {
        let fact = recovered.fact;
        refresh_source_authority(session).await?;
        fact
    } else {
        let Some(begin) =
            find_source_operation(session, key, operation, SourceOperationStage::Begin).await?
        else {
            if published_generation_for_key(session, key, operation)
                .await?
                .is_some()
            {
                return Err(SourceError::IdempotencyConflict);
            }
            return Ok(None);
        };
        let Some(generation_id) = published_generation_for_key(session, key, operation).await?
        else {
            return Ok(None);
        };
        recover_published_operation(
            session,
            SourceState::Clean,
            generation_id,
            begin.head,
            key,
            operation,
        )
        .await?
    };
    let outcome = match fact.state {
        DurableSourceState::Clean => ReconcileOutcome::Clean(Generation {
            workspace: session.workspace.clone(),
            id: fact.generation_id,
        }),
        DurableSourceState::NeedsRescan(reason) => {
            ReconcileOutcome::NeedsRescan(watch_invalidation(reason))
        }
        DurableSourceState::Conflict => ReconcileOutcome::Conflict,
        DurableSourceState::PendingCapture | DurableSourceState::Sealed => {
            return Err(SourceError::Engine(
                "durable source operation has an invalid terminal state".to_owned(),
            ));
        }
    };
    Ok(Some(outcome))
}

async fn replay_seal_operation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    key: IdempotencyKey,
) -> Result<Option<Generation<A, O>>, SourceError> {
    let fact = if let Some(recovered) = find_source_operation(
        session,
        key,
        SourceOperation::Seal,
        SourceOperationStage::Terminal,
    )
    .await?
    {
        let fact = recovered.fact;
        refresh_source_authority(session).await?;
        fact
    } else {
        let Some(begin) = find_source_operation(
            session,
            key,
            SourceOperation::Seal,
            SourceOperationStage::Begin,
        )
        .await?
        else {
            if published_generation_for_key(session, key, SourceOperation::Seal)
                .await?
                .is_some()
            {
                return Err(SourceError::IdempotencyConflict);
            }
            return Ok(None);
        };
        let Some(generation_id) =
            published_generation_for_key(session, key, SourceOperation::Seal).await?
        else {
            return Ok(None);
        };
        recover_published_operation(
            session,
            SourceState::Sealed,
            generation_id,
            begin.head,
            key,
            SourceOperation::Seal,
        )
        .await?
    };
    match fact.state {
        DurableSourceState::Sealed => Ok(Some(Generation {
            workspace: session.workspace.clone(),
            id: fact.generation_id,
        })),
        DurableSourceState::Conflict => Err(SourceError::Conflict),
        DurableSourceState::NeedsRescan(_) => Err(SourceError::Engine(
            "source could not reach a clean sealed state".to_owned(),
        )),
        DurableSourceState::Clean | DurableSourceState::PendingCapture => Err(SourceError::Engine(
            "durable seal operation has an invalid terminal state".to_owned(),
        )),
    }
}

async fn find_source_operation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    key: IdempotencyKey,
    operation: SourceOperation,
    stage: SourceOperationStage,
) -> Result<Option<RecoveredSourceOperation>, SourceError> {
    let authority_id = source_authority_id(session.workspace.volume.id());
    let recovered = session
        .workspace
        .volume
        .fs
        .authority()
        .find_operation(
            authority_id,
            source_stage_operation_id(key, stage),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?
        .value;
    let Some(commit) = recovered else {
        return Ok(None);
    };
    let fact = decode_source_fact(&commit.payload, 4096).map_err(engine)?;
    if commit.fingerprint != source_operation_fingerprint(operation, &fact)? {
        return Err(SourceError::IdempotencyConflict);
    }
    validate_source_fact_binding(session, &fact)?;
    let head = Head {
        epoch: commit.epoch,
        sequence: commit.sequence,
        digest: commit.digest,
    };
    if head.epoch == session.authority_head.epoch
        && head.sequence >= session.authority_head.sequence
    {
        session.authority_head = head;
        session.state = source_state(fact.state);
    }
    Ok(Some(RecoveredSourceOperation { fact, head }))
}

fn validate_source_fact_binding<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &SourceSession<A, O>,
    fact: &SourceFact,
) -> Result<(), SourceError> {
    if fact.volume_id != session.workspace.volume.id()
        || fact.root_identity != session.watcher.root_identity().to_bytes()
        || fact.mode != durable_mode(session.mode)
        || fact.maximum_paths != session.capture.maximum_paths
        || fact.maximum_extent_spans != session.capture.maximum_extent_spans
        || fact.maximum_queued_changes != session.maximum_queued_changes
        || fact.capture_policy != session.capture_policy.fingerprint()
    {
        return Err(SourceError::BindingMismatch);
    }
    Ok(())
}

async fn published_generation_for_key<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<Option<crate::GenerationId>, SourceError> {
    let receipt = session
        .workspace
        .volume
        .fs
        .observe_volume_operation(
            session.workspace.volume.id(),
            source_volume_operation_id(key, operation),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?;
    let Some(commit) = receipt.value else {
        return Ok(None);
    };
    let published = decode_published_generation(&commit.payload, 4096).map_err(engine)?;
    if published.volume_id != session.workspace.volume.id() {
        return Err(SourceError::IdempotencyConflict);
    }
    let Some(previous_sequence) = commit.sequence.get().checked_sub(1) else {
        return Err(SourceError::IdempotencyConflict);
    };
    let expected = Head {
        epoch: commit.epoch,
        sequence: Sequence::new(previous_sequence),
        digest: commit.previous_digest,
    };
    let request = PublishGenerationRequest {
        authority_id: volume_authority_id(session.workspace.volume.id()),
        volume_id: session.workspace.volume.id(),
        epoch: commit.epoch,
        expected,
        operation_id: source_volume_operation_id(key, operation),
        generation_root: published.generation_root,
    };
    if commit.fingerprint
        != contextual_publication_fingerprint(
            request,
            source_volume_operation_context(key, operation),
        )
    {
        return Err(SourceError::IdempotencyConflict);
    }
    Ok(Some(crate::GenerationId::new(
        published.generation_root.digest,
    )))
}

async fn begin_source_operation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<(), SourceError> {
    if find_source_operation(session, key, operation, SourceOperationStage::Begin)
        .await?
        .is_some()
    {
        return Ok(());
    }
    if published_generation_for_key(session, key, operation)
        .await?
        .is_some()
    {
        return Err(SourceError::IdempotencyConflict);
    }
    let fact = source_fact(
        session,
        SourceState::PendingCapture,
        session.checkout.generation_id(),
    );
    let fingerprint = source_operation_fingerprint(operation, &fact)?;
    session.authority_head = append_source_fact_with_identity(
        &session.workspace,
        session.authority_head,
        fact,
        source_stage_operation_id(key, SourceOperationStage::Begin),
        fingerprint,
    )
    .await?;
    session.state = SourceState::PendingCapture;
    Ok(())
}

async fn persist_session_state<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
) -> Result<(), SourceError> {
    let generation_id = session.checkout.generation_id();
    persist_source_fact(session, state, generation_id).await
}

async fn persist_source_operation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<SourceFact, SourceError> {
    let generation_id = session.checkout.generation_id();
    persist_source_operation_with_generation(session, state, generation_id, key, operation).await
}

async fn persist_source_operation_with_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
    generation_id: crate::GenerationId,
    key: IdempotencyKey,
    operation: SourceOperation,
) -> Result<SourceFact, SourceError> {
    let fact = source_fact(session, state, generation_id);
    let fingerprint = source_operation_fingerprint(operation, &fact)?;
    let appended = append_source_fact_with_identity(
        &session.workspace,
        session.authority_head,
        fact,
        source_stage_operation_id(key, SourceOperationStage::Terminal),
        fingerprint,
    )
    .await;
    match appended {
        Ok(head) => {
            session.authority_head = head;
            session.state = state;
            Ok(fact)
        }
        Err(SourceError::IdempotencyConflict) => {
            let recovered =
                find_source_operation(session, key, operation, SourceOperationStage::Terminal)
                    .await?
                    .ok_or(SourceError::IdempotencyConflict)?;
            Ok(recovered.fact)
        }
        Err(error) => Err(error),
    }
}

async fn persist_source_fact<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &mut SourceSession<A, O>,
    state: SourceState,
    generation_id: crate::GenerationId,
) -> Result<(), SourceError> {
    let fact = source_fact(session, state, generation_id);
    session.authority_head =
        append_source_fact(&session.workspace, session.authority_head, fact).await?;
    session.state = state;
    Ok(())
}

fn source_fact<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    session: &SourceSession<A, O>,
    state: SourceState,
    generation_id: crate::GenerationId,
) -> SourceFact {
    SourceFact {
        volume_id: session.workspace.volume.id(),
        root_identity: session.watcher.root_identity().to_bytes(),
        mode: durable_mode(session.mode),
        maximum_paths: session.capture.maximum_paths,
        maximum_extent_spans: session.capture.maximum_extent_spans,
        maximum_queued_changes: session.maximum_queued_changes,
        capture_policy: session.capture_policy.fingerprint(),
        state: durable_state(state),
        generation_id,
    }
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
    append_source_fact_with_identity(
        workspace,
        expected,
        fact,
        OperationId::from_bytes(operation_bytes),
        fingerprint,
    )
    .await
}

async fn append_source_fact_with_identity<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    expected: Head,
    fact: SourceFact,
    operation_id: OperationId,
    fingerprint: Digest,
) -> Result<Head, SourceError> {
    let authority_id = source_authority_id(workspace.volume.id());
    let payload = encode_source_fact(fact).map_err(engine)?;
    let outcome = workspace
        .volume
        .fs
        .authority()
        .compare_and_append(
            authority_id,
            expected.epoch,
            expected,
            ProposedCommit {
                operation_id,
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
        AppendOutcome::IdempotencyConflict { .. } => Err(SourceError::IdempotencyConflict),
    }
}

fn source_operation_fingerprint(
    operation: SourceOperation,
    fact: &SourceFact,
) -> Result<Digest, SourceError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-source-command-v1\0");
    hasher.update(&[match operation {
        SourceOperation::Reconcile => 1,
        SourceOperation::Rescan => 2,
        SourceOperation::Seal => 3,
    }]);
    hasher.update(&encode_source_fact(*fact).map_err(engine)?);
    Ok(Digest::from_bytes(*hasher.finalize().as_bytes()))
}

fn source_stage_operation_id(key: IdempotencyKey, stage: SourceOperationStage) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-source-stage-v1\0");
    hasher.update(&key.into_bytes());
    hasher.update(&[match stage {
        SourceOperationStage::Begin => 1,
        SourceOperationStage::Terminal => 2,
    }]);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn source_volume_operation_id(key: IdempotencyKey, operation: SourceOperation) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-source-volume-publication-v1\0");
    hasher.update(&key.into_bytes());
    hasher.update(&[match operation {
        SourceOperation::Reconcile => 1,
        SourceOperation::Rescan => 2,
        SourceOperation::Seal => 3,
    }]);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn source_volume_operation_context(key: IdempotencyKey, operation: SourceOperation) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-source-volume-context-v1\0");
    hasher.update(&key.into_bytes());
    hasher.update(&[match operation {
        SourceOperation::Reconcile => 1,
        SourceOperation::Rescan => 2,
        SourceOperation::Seal => 3,
    }]);
    Digest::from_bytes(*hasher.finalize().as_bytes())
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

const fn source_state(state: DurableSourceState) -> SourceState {
    match state {
        DurableSourceState::Clean => SourceState::Clean,
        DurableSourceState::PendingCapture => SourceState::PendingCapture,
        DurableSourceState::NeedsRescan(reason) => {
            SourceState::NeedsRescan(watch_invalidation(reason))
        }
        DurableSourceState::Conflict => SourceState::Conflict,
        DurableSourceState::Sealed => SourceState::Sealed,
    }
}

const fn watch_invalidation(reason: SourceInvalidation) -> WatchInvalidationReason {
    match reason {
        SourceInvalidation::InitialSnapshotRequired => {
            WatchInvalidationReason::InitialSnapshotRequired
        }
        SourceInvalidation::QueueOverflow => WatchInvalidationReason::QueueOverflow,
        SourceInvalidation::NativeRescanRequired => WatchInvalidationReason::NativeRescanRequired,
        SourceInvalidation::BackendError => WatchInvalidationReason::BackendError,
        SourceInvalidation::UnrepresentablePath => WatchInvalidationReason::UnrepresentablePath,
        SourceInvalidation::AmbiguousRename => WatchInvalidationReason::AmbiguousRename,
        SourceInvalidation::RootChanged => WatchInvalidationReason::RootChanged,
    }
}

fn engine(error: impl std::fmt::Display) -> SourceError {
    SourceError::Engine(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU8, Ordering};

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum AppendGatePhase {
        Before = 1,
        After = 2,
    }

    struct AppendGate {
        target: std::sync::Mutex<Option<OperationId>>,
        phase: AtomicU8,
        arrived: tokio::sync::watch::Sender<usize>,
        open: tokio::sync::watch::Sender<bool>,
    }

    impl AppendGate {
        fn new() -> Self {
            let (arrived, _) = tokio::sync::watch::channel(0);
            let (open, _) = tokio::sync::watch::channel(true);
            Self {
                target: std::sync::Mutex::new(None),
                phase: AtomicU8::new(0),
                arrived,
                open,
            }
        }

        fn arm(&self, target: OperationId, phase: AppendGatePhase) {
            let mut configured = match self.target.lock() {
                Ok(configured) => configured,
                Err(poisoned) => poisoned.into_inner(),
            };
            *configured = Some(target);
            self.phase.store(phase as u8, Ordering::Release);
            self.arrived.send_replace(0);
            self.open.send_replace(false);
        }

        fn matches(&self, operation_id: OperationId, phase: AppendGatePhase) -> bool {
            let target = match self.target.lock() {
                Ok(target) => target,
                Err(poisoned) => poisoned.into_inner(),
            };
            !*self.open.borrow()
                && self.phase.load(Ordering::Acquire) == phase as u8
                && *target == Some(operation_id)
        }

        async fn wait(&self) {
            let mut open = self.open.subscribe();
            self.arrived.send_modify(|arrived| *arrived += 1);
            while !*open.borrow_and_update() {
                if open.changed().await.is_err() {
                    return;
                }
            }
        }

        async fn wait_for_arrivals(&self, expected: usize) {
            let mut arrived = self.arrived.subscribe();
            while *arrived.borrow_and_update() < expected {
                if arrived.changed().await.is_err() {
                    return;
                }
            }
        }

        fn release(&self) {
            self.open.send_replace(true);
        }
    }

    #[derive(Clone)]
    struct GatedAuthorityStore {
        inner: Arc<crate::memory::MemoryAuthorityStore>,
        gate: Arc<AppendGate>,
    }

    impl AsyncAuthorityStore for GatedAuthorityStore {
        async fn fork_generation_authority(
            &self,
            source: crate::GenerationForkSource,
            destination_authority: crate::foundation::AuthorityId,
            operation_id: crate::foundation::OperationId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> crate::AuthorityResult<CreateAuthorityOutcome> {
            self.inner
                .fork_generation_authority(
                    source,
                    destination_authority,
                    operation_id,
                    budget,
                    cancellation,
                )
                .await
        }

        async fn create_authority(
            &self,
            authority_id: crate::foundation::AuthorityId,
            genesis_epoch: Epoch,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> crate::AuthorityResult<CreateAuthorityOutcome> {
            self.inner
                .create_authority(authority_id, genesis_epoch, budget, cancellation)
                .await
        }

        async fn head(
            &self,
            authority_id: crate::foundation::AuthorityId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> crate::AuthorityResult<Head> {
            self.inner.head(authority_id, budget, cancellation).await
        }

        async fn compare_and_append(
            &self,
            authority_id: crate::foundation::AuthorityId,
            epoch: Epoch,
            expected: Head,
            commit: ProposedCommit,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> crate::AuthorityResult<AppendOutcome> {
            if self
                .gate
                .matches(commit.operation_id, AppendGatePhase::Before)
            {
                self.gate.wait().await;
            }
            let operation_id = commit.operation_id;
            let result = self
                .inner
                .compare_and_append(authority_id, epoch, expected, commit, budget, cancellation)
                .await;
            if self.gate.matches(operation_id, AppendGatePhase::After) {
                self.gate.wait().await;
            }
            result
        }

        async fn replay(
            &self,
            authority_id: crate::foundation::AuthorityId,
            after: Sequence,
            limit: ReplayLimit,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> crate::AuthorityResult<Vec<crate::foundation::DurableCommit>> {
            self.inner
                .replay(authority_id, after, limit, budget, cancellation)
                .await
        }

        async fn fence(
            &self,
            authority_id: crate::foundation::AuthorityId,
            expected: Head,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> crate::AuthorityResult<crate::FenceOutcome> {
            self.inner
                .fence(authority_id, expected, budget, cancellation)
                .await
        }

        async fn find_operation(
            &self,
            authority_id: crate::foundation::AuthorityId,
            operation_id: OperationId,
            budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> crate::AuthorityResult<Option<crate::foundation::DurableCommit>> {
            self.inner
                .find_operation(authority_id, operation_id, budget, cancellation)
                .await
        }
    }

    fn next_authority_append<A, O>(
        simulation: &crate::Simulation<A, O>,
    ) -> Result<u64, crate::SimulationError> {
        Ok(simulation
            .trace()?
            .iter()
            .filter(|entry| entry.operation == crate::SimulationOperation::AuthorityAppend)
            .map(|entry| entry.occurrence)
            .max()
            .unwrap_or(0)
            .saturating_add(1))
    }

    fn clean_generation<A, O>(
        outcome: ReconcileOutcome<A, O>,
    ) -> Result<Generation<A, O>, &'static str> {
        match outcome {
            ReconcileOutcome::Clean(generation) => Ok(generation),
            ReconcileOutcome::NeedsRescan(_) | ReconcileOutcome::Conflict => {
                Err("source did not become clean")
            }
        }
    }

    async fn publish_without_terminal<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        source: &Source<A, O>,
        key: IdempotencyKey,
        operation: SourceOperation,
    ) -> Result<crate::GenerationId, Box<dyn std::error::Error>> {
        let mut session = source.inner.lock().await;
        begin_source_operation(&mut session, key, operation).await?;
        session.watcher.begin_rescan()?;
        let capture = session.capture.clone();
        let policy = session.capture_policy.clone();
        crate::capture_baseline_with_policy(
            &mut session.checkout,
            &capture,
            &policy,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let trailing = session.watcher.finish_rescan()?;
        crate::capture_watch_batch_with_policy(
            &mut session.checkout,
            trailing,
            &capture,
            &policy,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        match session
            .checkout
            .commit_with_context(
                source_volume_operation_id(key, operation),
                source_volume_operation_context(key, operation),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. }
            | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => Ok(generation_id),
            _ => Err("source publication did not commit".into()),
        }
    }

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

    #[tokio::test]
    async fn attached_directory_excludes_private_prefixes_from_all_rescans()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join(".git"))?;
        std::fs::write(root.path().join(".git").join("config"), b"private")?;
        std::fs::write(root.path().join("visible.txt"), b"visible")?;
        let options = SourceOptions {
            excluded_paths: vec![PortablePath::parse(
                "/.git",
                crate::model::VolumeLimits::default(),
            )?],
            ..SourceOptions::default()
        };
        let workspace =
            Box::pin(Fs::memory().attach_directory("filtered", root.path(), options)).await?;
        assert_eq!(
            workspace.read("/visible.txt", 16).await?,
            Bytes::from_static(b"visible")
        );
        assert!(workspace.read("/.git/config", 16).await.is_err());

        std::fs::write(root.path().join(".git").join("config"), b"changed")?;
        let source = workspace.source().ok_or("source handle missing")?;
        assert!(matches!(
            Box::pin(source.rescan()).await?,
            ReconcileOutcome::Clean(_)
        ));
        assert!(workspace.read("/.git/config", 16).await.is_err());
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

    #[tokio::test]
    async fn keyed_source_retry_recovers_exact_outcome_and_rejects_another_kind()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("value.txt"), b"one")?;
        let workspace =
            Box::pin(Fs::memory().attach_directory("keyed", root.path(), SourceOptions::default()))
                .await?;
        let source = workspace.source().ok_or("source missing")?;
        std::fs::write(root.path().join("value.txt"), b"two")?;
        let key = IdempotencyKey::from_bytes([41; 16]);
        let first = clean_generation(Box::pin(source.rescan_with_key(key)).await?)?;
        std::fs::write(root.path().join("value.txt"), b"three")?;
        let retried = clean_generation(Box::pin(source.rescan_with_key(key)).await?)?;
        assert_eq!(retried.id(), first.id());
        assert_eq!(
            retried.read("/value.txt", 8).await?,
            Bytes::from_static(b"two")
        );
        assert!(matches!(
            Box::pin(source.seal_with_key(key)).await,
            Err(SourceError::IdempotencyConflict)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn no_op_reconcile_conflicts_after_an_independent_workspace_advance()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let workspace = Box::pin(Fs::memory().attach_directory(
            "no-op-stale",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source = workspace.source().ok_or("source missing")?;
        let acknowledged = source.inner.lock().await.checkout.generation_id();

        let mut unrelated = workspace
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .value;
        let advanced = match unrelated
            .commit_with_context(
                IdempotencyKey::from_bytes([62; 16]).operation_id(),
                Digest::from_bytes([62; 32]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. }
            | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
            CheckoutCommitOutcome::Conflict { .. }
            | CheckoutCommitOutcome::Fenced { .. }
            | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                return Err("independent workspace authority advance was rejected".into());
            }
        };
        assert_eq!(advanced, acknowledged);

        let key = IdempotencyKey::from_bytes([63; 16]);
        match Box::pin(source.reconcile_with_key(key)).await? {
            ReconcileOutcome::Conflict => assert!(matches!(
                Box::pin(source.reconcile_with_key(key)).await?,
                ReconcileOutcome::Conflict
            )),
            ReconcileOutcome::NeedsRescan(WatchInvalidationReason::NativeRescanRequired)
                if cfg!(target_os = "macos") =>
            {
                assert!(matches!(
                    Box::pin(source.reconcile_with_key(key)).await?,
                    ReconcileOutcome::NeedsRescan(WatchInvalidationReason::NativeRescanRequired)
                ));
                assert!(matches!(
                    Box::pin(source.rescan_with_key(IdempotencyKey::from_bytes([71; 16]))).await?,
                    ReconcileOutcome::Conflict
                ));
            }
            _ => return Err("source did not preserve conflict or rescan state".into()),
        }
        assert!(matches!(
            Box::pin(source.reconcile_with_key(IdempotencyKey::from_bytes([72; 16]))).await?,
            ReconcileOutcome::Conflict
        ));
        assert!(matches!(
            Box::pin(source.rescan_with_key(IdempotencyKey::from_bytes([73; 16]))).await?,
            ReconcileOutcome::Conflict
        ));
        assert!(matches!(
            Box::pin(source.seal_with_key(IdempotencyKey::from_bytes([74; 16]))).await,
            Err(SourceError::Conflict)
        ));
        assert_eq!(source.state().await, SourceState::Conflict);
        assert_eq!(workspace.head().await?.id(), advanced);
        Ok(())
    }

    #[tokio::test]
    async fn pending_source_capture_conflicts_after_an_authority_only_advance()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("source.txt"), b"source")?;
        let workspace = Box::pin(Fs::memory().attach_directory(
            "pending-source-authority-fence",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source = workspace.source().ok_or("source missing")?;
        let key = IdempotencyKey::from_bytes([77; 16]);
        {
            let mut session = source.inner.lock().await;
            begin_source_operation(&mut session, key, SourceOperation::Reconcile).await?;
            let limits = session.checkout.volume_config().limits;
            let portable = crate::path::PortablePath::parse("/source.txt", limits)?;
            let path = crate::kernel::NamespacePath::from_portable(&portable, limits)?;
            session
                .checkout
                .write_file(
                    path,
                    0,
                    Bytes::from_static(b"captured"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await?;
            assert!(session.checkout.has_pending_mutations());
        }

        let mut unrelated = workspace
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .value;
        let advanced = match unrelated
            .commit_with_context(
                IdempotencyKey::from_bytes([78; 16]).operation_id(),
                Digest::from_bytes([78; 32]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .value
        {
            CheckoutCommitOutcome::Committed { generation_id, .. }
            | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
            CheckoutCommitOutcome::Conflict { .. }
            | CheckoutCommitOutcome::Fenced { .. }
            | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                return Err("independent workspace authority advance was rejected".into());
            }
        };

        let outcome = {
            let mut session = source.inner.lock().await;
            assert_eq!(advanced, session.checkout.generation_id());
            publish_session(
                &mut session,
                key,
                SourceOperation::Reconcile,
                SourceState::Clean,
            )
            .await?
        };
        assert!(matches!(outcome, ReconcileOutcome::Conflict));
        assert!(matches!(
            Box::pin(source.reconcile_with_key(key)).await?,
            ReconcileOutcome::Conflict
        ));
        assert_eq!(source.state().await, SourceState::Conflict);
        assert_eq!(workspace.head().await?.id(), advanced);
        Ok(())
    }

    #[tokio::test]
    async fn pending_source_capture_rebases_over_a_disjoint_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("source.txt"), b"source")?;
        let workspace = Box::pin(Fs::memory().attach_directory(
            "pending-source-disjoint-rebase",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source = workspace.source().ok_or("source missing")?;
        let key = IdempotencyKey::from_bytes([79; 16]);
        let acknowledged = {
            let mut session = source.inner.lock().await;
            begin_source_operation(&mut session, key, SourceOperation::Reconcile).await?;
            let acknowledged = session.checkout.generation_id();
            let limits = session.checkout.volume_config().limits;
            let portable = crate::path::PortablePath::parse("/source.txt", limits)?;
            let path = crate::kernel::NamespacePath::from_portable(&portable, limits)?;
            session
                .checkout
                .write_file(
                    path,
                    0,
                    Bytes::from_static(b"captured"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await?;
            assert!(session.checkout.has_pending_mutations());
            acknowledged
        };

        let mut unrelated = workspace
            .begin_transaction(IdempotencyKey::from_bytes([80; 16]))
            .await?;
        unrelated
            .write("/unrelated.txt", Bytes::from_static(b"unrelated"))
            .await?;
        let advanced = match unrelated.commit().await? {
            crate::TransactionCommit::Committed(generation)
            | crate::TransactionCommit::AlreadyCommitted(generation) => generation,
            crate::TransactionCommit::Conflict { .. }
            | crate::TransactionCommit::Fenced
            | crate::TransactionCommit::IdempotencyConflict => {
                return Err("disjoint workspace advance was rejected".into());
            }
        };
        assert_ne!(advanced.id(), acknowledged);

        let reconciled = {
            let mut session = source.inner.lock().await;
            clean_generation(
                publish_session(
                    &mut session,
                    key,
                    SourceOperation::Reconcile,
                    SourceState::Clean,
                )
                .await?,
            )?
        };
        assert_eq!(
            reconciled.read("/source.txt", 16).await?,
            Bytes::from_static(b"captured")
        );
        assert_eq!(
            reconciled.read("/unrelated.txt", 16).await?,
            Bytes::from_static(b"unrelated")
        );
        assert_eq!(
            clean_generation(Box::pin(source.reconcile_with_key(key)).await?)?.id(),
            reconciled.id()
        );
        assert_eq!(source.state().await, SourceState::Clean);
        Ok(())
    }

    #[tokio::test]
    async fn no_op_reconcile_conflicts_when_an_advance_wins_before_its_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let gate = Arc::new(AppendGate::new());
        let authority = GatedAuthorityStore {
            inner: Arc::new(crate::memory::MemoryAuthorityStore::default()),
            gate: Arc::clone(&gate),
        };
        let fs = Fs::new(
            authority,
            Arc::new(crate::memory::MemoryObjectStore::default()),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let workspace = Box::pin(fs.attach_directory(
            "no-op-before-fence",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source_key = IdempotencyKey::from_bytes([66; 16]);
        let acknowledged = workspace
            .source()
            .ok_or("source missing")?
            .inner
            .lock()
            .await
            .checkout
            .generation_id();
        gate.arm(
            source_volume_operation_id(source_key, SourceOperation::Reconcile),
            AppendGatePhase::Before,
        );
        let source = workspace.source().ok_or("source missing")?.clone();
        let operation = tokio::spawn(async move { source.reconcile_with_key(source_key).await });
        gate.wait_for_arrivals(1).await;

        let mut unrelated = workspace
            .begin_transaction(IdempotencyKey::from_bytes([67; 16]))
            .await?;
        unrelated
            .write("/unrelated.txt", Bytes::from_static(b"unrelated"))
            .await?;
        let advanced = match unrelated.commit().await? {
            crate::TransactionCommit::Committed(generation)
            | crate::TransactionCommit::AlreadyCommitted(generation) => generation,
            crate::TransactionCommit::Conflict { .. }
            | crate::TransactionCommit::Fenced
            | crate::TransactionCommit::IdempotencyConflict => {
                return Err("concurrent workspace advance was rejected".into());
            }
        };
        assert_ne!(advanced.id(), acknowledged);
        gate.release();

        assert!(matches!(operation.await??, ReconcileOutcome::Conflict));
        let source = workspace.source().ok_or("source missing")?;
        assert!(matches!(
            Box::pin(source.reconcile_with_key(source_key)).await?,
            ReconcileOutcome::Conflict
        ));
        assert_eq!(source.state().await, SourceState::Conflict);
        assert_eq!(workspace.head().await?.id(), advanced.id());
        Ok(())
    }

    #[tokio::test]
    async fn no_op_reconcile_linearizes_before_an_advance_between_fence_and_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let gate = Arc::new(AppendGate::new());
        let authority = GatedAuthorityStore {
            inner: Arc::new(crate::memory::MemoryAuthorityStore::default()),
            gate: Arc::clone(&gate),
        };
        let fs = Fs::new(
            authority,
            Arc::new(crate::memory::MemoryObjectStore::default()),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let workspace = Box::pin(fs.attach_directory(
            "no-op-concurrent",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source_key = IdempotencyKey::from_bytes([64; 16]);
        let acknowledged = workspace
            .source()
            .ok_or("source missing")?
            .inner
            .lock()
            .await
            .checkout
            .generation_id();
        gate.arm(
            source_stage_operation_id(source_key, SourceOperationStage::Terminal),
            AppendGatePhase::Before,
        );
        let source = workspace.source().ok_or("source missing")?.clone();
        let operation = tokio::spawn(async move { source.reconcile_with_key(source_key).await });
        gate.wait_for_arrivals(1).await;

        let mut unrelated = workspace
            .begin_transaction(IdempotencyKey::from_bytes([65; 16]))
            .await?;
        unrelated
            .write("/unrelated.txt", Bytes::from_static(b"unrelated"))
            .await?;
        let advanced = match unrelated.commit().await? {
            crate::TransactionCommit::Committed(generation)
            | crate::TransactionCommit::AlreadyCommitted(generation) => generation,
            crate::TransactionCommit::Conflict { .. }
            | crate::TransactionCommit::Fenced
            | crate::TransactionCommit::IdempotencyConflict => {
                return Err("concurrent workspace advance was rejected".into());
            }
        };
        assert_ne!(advanced.id(), acknowledged);
        gate.release();

        let reconciled = clean_generation(operation.await??)?;
        assert_eq!(reconciled.id(), acknowledged);
        let source = workspace.source().ok_or("source missing")?;
        assert_eq!(
            clean_generation(Box::pin(source.reconcile_with_key(source_key)).await?)?.id(),
            acknowledged
        );
        assert_eq!(source.state().await, SourceState::Clean);
        assert_eq!(workspace.head().await?.id(), advanced.id());
        Ok(())
    }

    #[tokio::test]
    async fn published_retry_preserves_a_later_fail_closed_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("source.txt"), b"source")?;
        let workspace = Box::pin(Fs::memory().attach_directory(
            "published-retry-terminal-race",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source = workspace.source().ok_or("source missing")?;
        let published_key = IdempotencyKey::from_bytes([68; 16]);
        let other_key = IdempotencyKey::from_bytes([69; 16]);

        let (published_generation, published_begin_head, other_begin, other_terminal) = {
            let mut session = source.inner.lock().await;
            begin_source_operation(&mut session, published_key, SourceOperation::Reconcile).await?;
            let published_begin_head = session.authority_head;
            let publication = session
                .checkout
                .commit_with_context(
                    source_volume_operation_id(published_key, SourceOperation::Reconcile),
                    source_volume_operation_context(published_key, SourceOperation::Reconcile),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await?
                .value;
            let published_generation = match publication {
                CheckoutCommitOutcome::Committed { generation_id, .. }
                | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
                CheckoutCommitOutcome::Conflict { .. }
                | CheckoutCommitOutcome::Fenced { .. }
                | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                    return Err("source volume publication was rejected".into());
                }
            };
            (
                published_generation,
                published_begin_head,
                source_fact(&session, SourceState::PendingCapture, published_generation),
                source_fact(
                    &session,
                    SourceState::NeedsRescan(WatchInvalidationReason::BackendError),
                    published_generation,
                ),
            )
        };

        let other_begin_head = append_source_fact_with_identity(
            &workspace,
            published_begin_head,
            other_begin,
            source_stage_operation_id(other_key, SourceOperationStage::Begin),
            source_operation_fingerprint(SourceOperation::Reconcile, &other_begin)?,
        )
        .await?;
        let other_terminal_head = append_source_fact_with_identity(
            &workspace,
            other_begin_head,
            other_terminal,
            source_stage_operation_id(other_key, SourceOperationStage::Terminal),
            source_operation_fingerprint(SourceOperation::Reconcile, &other_terminal)?,
        )
        .await?;

        assert!(matches!(
            Box::pin(source.reconcile_with_key(published_key)).await?,
            ReconcileOutcome::Conflict
        ));
        assert!(matches!(
            Box::pin(source.reconcile_with_key(published_key)).await?,
            ReconcileOutcome::Conflict
        ));
        let mut session = source.inner.lock().await;
        let recovered = find_source_operation(
            &mut session,
            published_key,
            SourceOperation::Reconcile,
            SourceOperationStage::Terminal,
        )
        .await?
        .ok_or("recovered terminal missing")?;
        let recovered_head = session.authority_head;
        assert_eq!(recovered.fact.generation_id, published_generation);
        assert_eq!(recovered.fact.state, DurableSourceState::Conflict);
        assert!(recovered_head.sequence > other_terminal_head.sequence);
        assert_eq!(session.state, SourceState::Conflict);
        Ok(())
    }

    #[tokio::test]
    async fn published_retry_conflicts_after_another_source_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("source.txt"), b"one")?;
        let fs = Fs::memory();
        let first_workspace = Box::pin(fs.attach_directory(
            "published-retry-generation-race",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let first = first_workspace.source().ok_or("first source missing")?;
        let published_key = IdempotencyKey::from_bytes([75; 16]);
        std::fs::write(root.path().join("source.txt"), b"two")?;
        let published_generation =
            publish_without_terminal(first, published_key, SourceOperation::Rescan).await?;

        std::fs::write(root.path().join("source.txt"), b"three")?;
        let second_workspace = Box::pin(fs.attach_directory(
            "published-retry-generation-race",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let second = second_workspace.source().ok_or("second source missing")?;
        let advanced = second_workspace.head().await?;
        assert_ne!(advanced.id(), published_generation);
        assert_eq!(second.state().await, SourceState::Clean);

        assert!(matches!(
            Box::pin(first.rescan_with_key(published_key)).await?,
            ReconcileOutcome::Conflict
        ));
        {
            let mut session = first.inner.lock().await;
            let recovered = find_source_operation(
                &mut session,
                published_key,
                SourceOperation::Rescan,
                SourceOperationStage::Terminal,
            )
            .await?
            .ok_or("conflict terminal missing")?;
            assert_eq!(recovered.fact.generation_id, published_generation);
            assert_eq!(recovered.fact.state, DurableSourceState::Conflict);
            assert_eq!(session.state, SourceState::Conflict);
        }
        assert_eq!(first.state().await, SourceState::Conflict);
        assert!(matches!(
            Box::pin(first.seal_with_key(IdempotencyKey::from_bytes([76; 16]))).await,
            Err(SourceError::Conflict)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn terminal_retry_refreshes_a_later_source_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("source.txt"), b"source")?;
        let workspace = Box::pin(Fs::memory().attach_directory(
            "terminal-retry-refresh",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source = workspace.source().ok_or("source missing")?;
        let completed_key = IdempotencyKey::from_bytes([70; 16]);
        let later_key = IdempotencyKey::from_bytes([71; 16]);

        let (completed_generation, completed_terminal_head, later_begin, later_terminal) = {
            let mut session = source.inner.lock().await;
            begin_source_operation(&mut session, completed_key, SourceOperation::Reconcile).await?;
            let publication = session
                .checkout
                .commit_with_context(
                    source_volume_operation_id(completed_key, SourceOperation::Reconcile),
                    source_volume_operation_context(completed_key, SourceOperation::Reconcile),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await?
                .value;
            let completed_generation = match publication {
                CheckoutCommitOutcome::Committed { generation_id, .. }
                | CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => generation_id,
                CheckoutCommitOutcome::Conflict { .. }
                | CheckoutCommitOutcome::Fenced { .. }
                | CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                    return Err("source volume publication was rejected".into());
                }
            };
            persist_source_operation_with_generation(
                &mut session,
                SourceState::Clean,
                completed_generation,
                completed_key,
                SourceOperation::Reconcile,
            )
            .await?;
            (
                completed_generation,
                session.authority_head,
                source_fact(&session, SourceState::PendingCapture, completed_generation),
                source_fact(&session, SourceState::Conflict, completed_generation),
            )
        };

        let later_begin_head = append_source_fact_with_identity(
            &workspace,
            completed_terminal_head,
            later_begin,
            source_stage_operation_id(later_key, SourceOperationStage::Begin),
            source_operation_fingerprint(SourceOperation::Reconcile, &later_begin)?,
        )
        .await?;
        let later_terminal_head = append_source_fact_with_identity(
            &workspace,
            later_begin_head,
            later_terminal,
            source_stage_operation_id(later_key, SourceOperationStage::Terminal),
            source_operation_fingerprint(SourceOperation::Reconcile, &later_terminal)?,
        )
        .await?;

        assert_eq!(
            clean_generation(Box::pin(source.reconcile_with_key(completed_key)).await?)?.id(),
            completed_generation
        );
        let session = source.inner.lock().await;
        assert_eq!(session.authority_head, later_terminal_head);
        assert_eq!(session.state, SourceState::Conflict);
        Ok(())
    }

    #[tokio::test]
    async fn two_source_sessions_replay_one_canonical_terminal_outcome()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("value.txt"), b"one")?;
        let gate = Arc::new(AppendGate::new());
        let authority = GatedAuthorityStore {
            inner: Arc::new(crate::memory::MemoryAuthorityStore::default()),
            gate: Arc::clone(&gate),
        };
        let fs = Fs::new(
            authority,
            Arc::new(crate::memory::MemoryObjectStore::default()),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let first_workspace =
            Box::pin(fs.attach_directory("two-sessions", root.path(), SourceOptions::default()))
                .await?;
        let second_workspace =
            Box::pin(fs.attach_directory("two-sessions", root.path(), SourceOptions::default()))
                .await?;
        let first = first_workspace.source().ok_or("first source missing")?;
        let second = second_workspace.source().ok_or("second source missing")?;
        let key = IdempotencyKey::from_bytes([52; 16]);
        {
            let mut session = second.inner.lock().await;
            begin_source_operation(&mut session, key, SourceOperation::Rescan).await?;
        }
        {
            let mut session = first.inner.lock().await;
            begin_source_operation(&mut session, key, SourceOperation::Rescan).await?;
        }
        gate.arm(
            source_stage_operation_id(key, SourceOperationStage::Terminal),
            AppendGatePhase::Before,
        );
        let first = first.clone();
        let first_terminal = tokio::spawn(async move {
            let mut session = first.inner.lock().await;
            let generation_id = session.checkout.generation_id();
            persist_source_operation_with_generation(
                &mut session,
                SourceState::Clean,
                generation_id,
                key,
                SourceOperation::Rescan,
            )
            .await
        });
        let second = second.clone();
        let second_terminal = tokio::spawn(async move {
            let mut session = second.inner.lock().await;
            let generation_id = session.checkout.generation_id();
            persist_source_operation_with_generation(
                &mut session,
                SourceState::Conflict,
                generation_id,
                key,
                SourceOperation::Rescan,
            )
            .await
        });
        gate.wait_for_arrivals(2).await;
        gate.release();
        let first_fact = first_terminal.await??;
        let second_fact = second_terminal.await??;
        assert_eq!(first_fact, second_fact);
        let first_session = first_workspace
            .source()
            .ok_or("first source missing")?
            .inner
            .lock()
            .await;
        let second_session = second_workspace
            .source()
            .ok_or("second source missing")?
            .inner
            .lock()
            .await;
        assert_eq!(first_session.authority_head, second_session.authority_head);
        assert_eq!(first_session.state, second_session.state);
        assert_eq!(first_session.state, source_state(first_fact.state));
        Ok(())
    }

    #[tokio::test]
    async fn dropped_source_future_recovers_after_each_durable_append_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        {
            let root = tempfile::tempdir()?;
            std::fs::write(root.path().join("value.txt"), b"one")?;
            let gate = Arc::new(AppendGate::new());
            let authority = GatedAuthorityStore {
                inner: Arc::new(crate::memory::MemoryAuthorityStore::default()),
                gate: Arc::clone(&gate),
            };
            let fs = Fs::new(
                authority.clone(),
                Arc::new(crate::memory::MemoryObjectStore::default()),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let workspace = Box::pin(fs.attach_directory(
                "drop-after-begin",
                root.path(),
                SourceOptions::default(),
            ))
            .await?;
            let key = IdempotencyKey::from_bytes([59; 16]);
            gate.arm(
                source_stage_operation_id(key, SourceOperationStage::Begin),
                AppendGatePhase::After,
            );
            std::fs::write(root.path().join("value.txt"), b"two")?;
            let source = workspace.source().ok_or("source missing")?.clone();
            let operation = tokio::spawn(async move { source.rescan_with_key(key).await });
            gate.wait_for_arrivals(1).await;
            operation.abort();
            assert!(operation.await.is_err_and(|error| error.is_cancelled()));
            gate.release();

            let source = workspace.source().ok_or("source missing")?;
            let recovered = clean_generation(Box::pin(source.rescan_with_key(key)).await?)?;
            assert_eq!(
                recovered.read("/value.txt", 8).await?,
                Bytes::from_static(b"two")
            );
            std::fs::write(root.path().join("value.txt"), b"three")?;
            let current = clean_generation(Box::pin(source.rescan()).await?)?;
            let current_head = workspace.head().await?;
            assert_eq!(current_head.id(), current.id());
            assert_eq!(
                current.read("/value.txt", 8).await?,
                Bytes::from_static(b"three")
            );
            assert_eq!(
                clean_generation(Box::pin(source.rescan_with_key(key)).await?)?.id(),
                recovered.id()
            );
            assert_eq!(workspace.head().await?.id(), current.id());
            assert_eq!(source.state().await, SourceState::Clean);
            let session = source.inner.lock().await;
            let durable_head = authority
                .inner
                .head(
                    source_authority_id(workspace.volume.id()),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await?
                .value;
            assert_eq!(session.authority_head, durable_head);
        }

        for terminal_append in [false, true] {
            let root = tempfile::tempdir()?;
            std::fs::write(root.path().join("value.txt"), b"one")?;
            let gate = Arc::new(AppendGate::new());
            let authority = GatedAuthorityStore {
                inner: Arc::new(crate::memory::MemoryAuthorityStore::default()),
                gate: Arc::clone(&gate),
            };
            let objects = Arc::new(crate::memory::MemoryObjectStore::default());
            let name = if terminal_append {
                "drop-after-terminal"
            } else {
                "drop-after-volume"
            };
            let fs = Fs::new(
                authority.clone(),
                Arc::clone(&objects),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let workspace =
                Box::pin(fs.attach_directory(name, root.path(), SourceOptions::default())).await?;
            let key = IdempotencyKey::from_bytes([60 + u8::from(terminal_append); 16]);
            let target = if terminal_append {
                source_stage_operation_id(key, SourceOperationStage::Terminal)
            } else {
                source_volume_operation_id(key, SourceOperation::Rescan)
            };
            gate.arm(target, AppendGatePhase::After);
            std::fs::write(root.path().join("value.txt"), b"two")?;
            let source = workspace.source().ok_or("source missing")?.clone();
            let operation = tokio::spawn(async move { source.rescan_with_key(key).await });
            gate.wait_for_arrivals(1).await;
            operation.abort();
            assert!(operation.await.is_err_and(|error| error.is_cancelled()));
            gate.release();

            std::fs::write(root.path().join("value.txt"), b"three")?;
            drop(workspace);
            drop(fs);
            let fs = Fs::new(
                authority.clone(),
                objects,
                crate::EmbeddedCapabilities::MEMORY,
            );
            let workspace =
                Box::pin(fs.attach_directory(name, root.path(), SourceOptions::default())).await?;
            let source = workspace.source().ok_or("source missing")?;
            let current = workspace.head().await?;
            assert_eq!(
                current.read("/value.txt", 8).await?,
                Bytes::from_static(b"three")
            );
            let outcome = Box::pin(source.rescan_with_key(key)).await?;
            if terminal_append {
                assert_eq!(
                    clean_generation(outcome)?.read("/value.txt", 8).await?,
                    Bytes::from_static(b"two")
                );
                assert_eq!(source.state().await, SourceState::Clean);
            } else {
                assert!(matches!(outcome, ReconcileOutcome::Conflict));
                assert_eq!(source.state().await, SourceState::Conflict);
            }
            assert_eq!(workspace.head().await?.id(), current.id());
            let session = source.inner.lock().await;
            let durable_head = authority
                .inner
                .head(
                    source_authority_id(workspace.volume.id()),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await?
                .value;
            assert_eq!(session.authority_head, durable_head);
        }
        Ok(())
    }

    #[tokio::test]
    async fn source_volume_identity_rejects_a_deliberately_aliased_generic_operation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("source.txt"), b"one")?;
        let fs = Fs::memory();
        let workspace = Box::pin(fs.attach_directory(
            "domain-separated",
            root.path(),
            SourceOptions::default(),
        ))
        .await?;
        let source = workspace.source().ok_or("source missing")?;
        let key = IdempotencyKey::from_bytes([42; 16]);
        {
            let mut session = source.inner.lock().await;
            begin_source_operation(&mut session, key, SourceOperation::Rescan).await?;
        }
        let forged_key = IdempotencyKey::from_bytes(
            source_volume_operation_id(key, SourceOperation::Rescan).into_bytes(),
        );
        let mut unrelated = workspace.begin_transaction(forged_key).await?;
        unrelated
            .write("/unrelated.txt", Bytes::from_static(b"generic"))
            .await?;
        let unrelated = match unrelated.commit().await? {
            crate::TransactionCommit::Committed(generation)
            | crate::TransactionCommit::AlreadyCommitted(generation) => generation,
            crate::TransactionCommit::Conflict { .. }
            | crate::TransactionCommit::Fenced
            | crate::TransactionCommit::IdempotencyConflict => {
                return Err("unexpected generic transaction rejection".into());
            }
        };
        std::fs::write(root.path().join("source.txt"), b"two")?;
        assert!(matches!(
            Box::pin(source.rescan_with_key(key)).await,
            Err(SourceError::IdempotencyConflict)
        ));
        assert_eq!(workspace.head().await?.id(), unrelated.id());
        assert_eq!(source.state().await, SourceState::PendingCapture);
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_volume_and_terminal_appends_recover_the_exact_source_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        for terminal_fault in [false, true] {
            let simulation = crate::Simulation::default();
            let (authority, objects) = simulation.stores();
            let fs = Fs::new(
                authority.clone(),
                objects.clone(),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let root = tempfile::tempdir()?;
            std::fs::write(root.path().join("value.txt"), b"one")?;
            let workspace = Box::pin(fs.attach_directory(
                if terminal_fault {
                    "ambiguous-terminal"
                } else {
                    "ambiguous-volume"
                },
                root.path(),
                SourceOptions::default(),
            ))
            .await?;
            let key = IdempotencyKey::from_bytes([50 + u8::from(terminal_fault); 16]);
            {
                let source = workspace.source().ok_or("source missing")?;
                let mut session = source.inner.lock().await;
                begin_source_operation(&mut session, key, SourceOperation::Rescan).await?;
            }
            std::fs::write(root.path().join("value.txt"), b"two")?;
            simulation.schedule(crate::ScheduledSimulationFault {
                operation: crate::SimulationOperation::AuthorityAppend,
                occurrence: next_authority_append(&simulation)? + u64::from(terminal_fault),
                fault: crate::SimulationFault::AmbiguousAuthorityAppend,
            })?;
            {
                let source = workspace.source().ok_or("source missing")?;
                assert!(matches!(
                    Box::pin(source.rescan_with_key(key)).await,
                    Err(SourceError::Engine(_))
                ));
            }
            std::fs::write(root.path().join("value.txt"), b"three")?;
            drop(workspace);
            drop(fs);
            let fs = Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
            let workspace = Box::pin(fs.attach_directory(
                if terminal_fault {
                    "ambiguous-terminal"
                } else {
                    "ambiguous-volume"
                },
                root.path(),
                SourceOptions::default(),
            ))
            .await?;
            let source = workspace.source().ok_or("source missing")?;
            let current = workspace.head().await?;
            assert_eq!(
                current.read("/value.txt", 8).await?,
                Bytes::from_static(b"three")
            );
            let outcome = Box::pin(source.rescan_with_key(key)).await?;
            if terminal_fault {
                assert_eq!(
                    clean_generation(outcome)?.read("/value.txt", 8).await?,
                    Bytes::from_static(b"two")
                );
                assert_eq!(source.state().await, SourceState::Clean);
            } else {
                assert!(matches!(outcome, ReconcileOutcome::Conflict));
                assert_eq!(source.state().await, SourceState::Conflict);
            }
            assert_eq!(workspace.head().await?.id(), current.id());
        }
        Ok(())
    }

    #[cfg(feature = "local")]
    #[tokio::test]
    async fn restart_recovers_publication_missing_its_terminal_source_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = tempfile::tempdir()?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("value.txt"), b"one")?;
        let options = crate::LocalOptions::new(storage.path());
        let key = IdempotencyKey::from_bytes([43; 16]);
        {
            let fs = Fs::local(options.clone()).await?;
            let workspace =
                Box::pin(fs.attach_directory("recover", root.path(), SourceOptions::default()))
                    .await?;
            let source = workspace.source().ok_or("source missing")?;
            std::fs::write(root.path().join("value.txt"), b"two")?;
            publish_without_terminal(source, key, SourceOperation::Rescan).await?;
        }

        std::fs::write(root.path().join("value.txt"), b"three")?;
        {
            let fs = Fs::local(options.clone()).await?;
            let workspace =
                Box::pin(fs.attach_directory("recover", root.path(), SourceOptions::default()))
                    .await?;
            let source = workspace.source().ok_or("source missing")?;
            let current = workspace.head().await?;
            assert_eq!(
                current.read("/value.txt", 8).await?,
                Bytes::from_static(b"three")
            );
            assert!(matches!(
                Box::pin(source.rescan_with_key(key)).await?,
                ReconcileOutcome::Conflict
            ));
            assert_eq!(workspace.head().await?.id(), current.id());
            assert_eq!(source.state().await, SourceState::Conflict);
        }

        let seal_key = IdempotencyKey::from_bytes([44; 16]);
        {
            let fs = Fs::local(options.clone()).await?;
            let workspace =
                Box::pin(fs.attach_directory("recover", root.path(), SourceOptions::default()))
                    .await?;
            let source = workspace.source().ok_or("source missing")?;
            std::fs::write(root.path().join("value.txt"), b"four")?;
            publish_without_terminal(source, seal_key, SourceOperation::Seal).await?;
        }

        std::fs::write(root.path().join("value.txt"), b"five")?;
        {
            let fs = Fs::local(options.clone()).await?;
            let workspace =
                Box::pin(fs.attach_directory("recover", root.path(), SourceOptions::default()))
                    .await?;
            let source = workspace.source().ok_or("source missing")?;
            let current = workspace.head().await?;
            assert!(matches!(
                Box::pin(source.seal_with_key(seal_key)).await,
                Err(SourceError::Conflict)
            ));
            assert_eq!(workspace.head().await?.id(), current.id());
            assert_eq!(source.state().await, SourceState::Conflict);
        }

        let fs = Fs::local(options).await?;
        let workspace =
            Box::pin(fs.attach_directory("recover", root.path(), SourceOptions::default())).await?;
        let source = workspace.source().ok_or("source missing")?;
        assert_eq!(
            workspace.read("/value.txt", 8).await?,
            Bytes::from_static(b"five")
        );
        assert!(matches!(
            Box::pin(source.rescan_with_key(key)).await?,
            ReconcileOutcome::Conflict
        ));
        assert!(matches!(
            Box::pin(source.seal_with_key(seal_key)).await,
            Err(SourceError::Conflict)
        ));
        assert_eq!(
            workspace.read("/value.txt", 8).await?,
            Bytes::from_static(b"five")
        );
        assert_eq!(source.state().await, SourceState::Clean);
        Ok(())
    }
}
