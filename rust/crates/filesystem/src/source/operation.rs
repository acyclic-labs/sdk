//! One source operation retaining the caller's freshly admitted workspace.
use super::*;

/// Exclusive operation on the original watcher with one admitted storage context.
/// The lock prevents another request from replacing storage during publication.
pub struct BoundSource<'a, A: 'static, O: 'static> {
    pub(super) session: tokio::sync::MutexGuard<'a, SourceSession<A, O>>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> BoundSource<'_, A, O> {
    /// Current fail-closed state of the retained source.
    pub fn state(&self) -> SourceState {
        self.session.state
    }

    /// Capture and publish one retained watcher interval under its retry identity.
    pub async fn reconcile_with_key(
        &mut self,
        key: IdempotencyKey,
    ) -> Result<ReconcileOutcome<A, O>, SourceError> {
        let session = &mut *self.session;
        if let Some(outcome) =
            replay_reconcile_operation(session, key, SourceOperation::Reconcile).await?
        {
            return Ok(outcome);
        }
        if session.state == SourceState::Conflict {
            return Ok(ReconcileOutcome::Conflict);
        }
        if session.mode == SourceMode::Pinned {
            return Err(SourceError::Pinned);
        }
        begin_source_operation(session, key, SourceOperation::Reconcile).await?;
        let maximum_paths = session.capture.maximum_paths.unwrap_or(1024);
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
                session,
                SourceState::NeedsRescan(reason),
                key,
                SourceOperation::Reconcile,
            )
            .await?;
            return reconcile_outcome_from_fact(session, fact);
        }
        let capture = session.capture.clone();
        let policy = session.capture_policy.clone();
        crate::capture_watch_batch(
            &mut session.checkout,
            batch,
            &capture,
            &policy,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(engine)?;
        publish_session(session, key, SourceOperation::Reconcile, SourceState::Clean).await
    }

    /// Rebuild the authenticated baseline without replacing the watcher.
    pub async fn rescan_with_key(
        &mut self,
        key: IdempotencyKey,
    ) -> Result<ReconcileOutcome<A, O>, SourceError> {
        let session = &mut *self.session;
        if let Some(outcome) =
            replay_reconcile_operation(session, key, SourceOperation::Rescan).await?
        {
            return Ok(outcome);
        }
        if session.state == SourceState::Conflict {
            return Ok(ReconcileOutcome::Conflict);
        }
        begin_source_operation(session, key, SourceOperation::Rescan).await?;
        rescan_session(session, key, SourceOperation::Rescan, SourceState::Clean).await
    }

    /// Seal the retained source under its original retry identity.
    pub async fn seal_with_key(
        &mut self,
        key: IdempotencyKey,
    ) -> Result<Generation<A, O>, SourceError> {
        let session = &mut *self.session;
        if let Some(generation) = replay_seal_operation(session, key).await? {
            return Ok(generation);
        }
        if session.state == SourceState::Conflict {
            return Err(SourceError::Conflict);
        }
        begin_source_operation(session, key, SourceOperation::Seal).await?;
        let generation =
            match rescan_session(session, key, SourceOperation::Seal, SourceState::Sealed).await? {
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
