use super::*;
use crate::filesystem::{PreparationIo, fork_preparer_io::failure, task_runtime::WorkerContext};
use crate::wire::ErrorCode;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn preparation_source(
        &self,
        context: &WorkerContext,
        token: &TaskResourcePreparation,
        cancellation: &CancellationToken,
        reconciliation_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<RetainedSourceBinding<A, O>> {
        if io.and_then(PreparationIo::original_native_work).is_some() {
            check_cancellation(cancellation, io)?;
        }
        let reader = context.context();
        let preparation = token.request();
        crate::executor::with_history_io(io, |work| preparation.validate(work))?;
        let caller_task = preparation.child.parent.ok_or_else(|| {
            failure(
                io,
                ErrorCode::Unauthorized,
                "configured preparation has no original caller",
            )
        })?;
        if reader.durable_task_id() != Some(token.origin().preparation_task)
            || token.origin().parent_task != caller_task
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "source restore lacks the actual sealed helper and original caller",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            reader.owned_task_binding_with_native_work(work)?;
            if reconciliation_only {
                token.verify_reconciliation_with_native_work(work)?.await?;
            } else {
                reader.require_original_native_dispatch(work)?;
                token.verify_creation_with_native_work(work)?.await?;
            }
        }
        let caller = self.caller(caller_task, io)?;
        self.check_caller_allocation(caller, io).await?;
        let request: ForkRequest = match io {
            Some(io) => crate::filesystem::retain_decoded(
                io.value_decode_preparation(&preparation.payload, ErrorCode::Storage)?,
            ),
            None => serde_json::from_value(preparation.payload.clone()).map_err(invalid_json)?,
        };
        if request.authorized_source.as_ref()
            != token
                .project_source()
                .map(ProjectSourceReadHandle::authorization)
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "preparation changed its retained sealed Project source",
            ));
        }
        let reducer = if let Some(checkpoint) = &preparation.source_checkpoint {
            let bytes = 'read: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                    break 'read reader.read_file_with_native_work(checkpoint, work).await?;
                }
                reader
                    .read_file_measured(checkpoint, self.config.content_budget, cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value
            };
            let capture: SourceCheckpoint = match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) => crate::filesystem::retain_decoded(io.decode(
                    std::marker::PhantomData,
                    &bytes,
                    ErrorCode::Storage,
                )?),
                _ => serde_json::from_slice(&bytes).map_err(invalid_json)?,
            };
            if capture.request != request || capture.caller != caller_task {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "configured preparation checkpoint changed original caller/request",
                ));
            }
            let snapshot = capture.snapshot;
            #[cfg(not(target_arch = "wasm32"))]
            let snapshot = if let Some(io) = io {
                let custody = io.take_native_custody(snapshot.predecoded_native_owned_bytes()?)?;
                snapshot.adopt_predecoded_native_custody(custody)?
            } else {
                snapshot
            };
            self.restore_snapshot(&caller.owner, &request, snapshot, cancellation, io)
                .await?
        } else if let Some(original) = self
            .config
            .original_sources
            .iter()
            .find(|original| original.request.operation_id == request.operation_id)
        {
            if original.caller != caller_task || original.request != request {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "trusted original record differs from the sealed preparation request",
                ));
            }
            let snapshot =
                crate::executor::with_history_io(io, |work| original.snapshot.copy(work))?;
            self.restore_snapshot(&caller.owner, &request, snapshot, cancellation, io)
                .await?
        } else {
            let ordinary;
            let observer = match io {
                Some(io) => io,
                None => {
                    ordinary =
                        PreparationIo::from_budget(self.config.content_budget, cancellation)?;
                    &ordinary
                }
            };
            self.open(&caller.owner, Some(request.parent_revision), Some(observer))
                .await?
                .into_reducer()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let WorkerContext::Native(shared) = context {
            let io = io.ok_or_else(|| {
                failure(
                    io,
                    ErrorCode::Unsupported,
                    "native source requires original IO",
                )
            })?;
            let work = io
                .original_native_work()
                .ok_or_else(|| io.invalid("native source requires original work"))?;
            let owner = self.source_owner(reader, cancellation, Some(io)).await?;
            let resolver =
                crate::runtime::content_residency_verifier(Arc::clone(shared), owner, Some(io))?;
            let verifier = crate::executor::with_history_io(Some(io), |work| {
                caller.owner.issuer().verifier(work)
            })?;
            let request = crate::executor::with_history_io(Some(io), |work| request.copy(work))?;
            let custody = io.take_native_custody(request.1)?;
            let source =
                crate::filesystem::recursive::restore_recursive_fork_source_with_native_work(
                    Arc::clone(&self.filesystem),
                    &self.stream,
                    reducer,
                    &verifier,
                    &caller.owner.scope,
                    &caller.project,
                    &self.config.stream_provider,
                    (request.0, custody),
                    self.config.history_limits,
                    token.project_source(),
                    resolver,
                    cancellation,
                    work,
                )
                .await?;
            io.absorb_native_custody(source.1)?;
            if reconciliation_only {
                token.verify_reconciliation_with_native_work(work)?.await?;
            } else {
                token.verify_creation_with_native_work(work)?.await?;
            }
            check_cancellation(cancellation, Some(io))?;
            return Ok(source.0);
        }
        let _ = reconciliation_only;
        restore_recursive_fork_source(
            Arc::clone(&self.filesystem),
            &self.stream,
            &reducer,
            verifier(&caller.owner)?,
            caller.owner.scope.clone(),
            caller.project.clone(),
            self.config.stream_provider.clone(),
            request,
            self.config.history_limits,
            token.project_source().cloned(),
            crate::runtime::content_residency_verifier(
                Arc::new(reader.clone()),
                self.source_owner(reader, cancellation, None).await?,
                None,
            )?,
            cancellation,
        )
        .await
    }
    pub(super) async fn restore_snapshot(
        &self,
        binding: &ConfiguredRecursiveAuthorityBinding,
        request: &ForkRequest,
        snapshot: Snapshot,
        cancellation: &CancellationToken,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Reducer> {
        use crate::filesystem::fork_preparer_io::failure;
        use crate::wire::ErrorCode;
        if snapshot.authority != request.parent
            || snapshot.revision != request.parent_revision
            || &snapshot.authority != binding.authority()
            || snapshot.revision == 0
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "configured checkpoint differs from original caller cut",
            ));
        }
        check_cancellation(cancellation, io)?;
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut observer = history.as_mut().map(|work| work as &mut dyn HistoryIo);
        let verifier = binding.issuer().verifier(&mut observer)?;
        let expected = snapshot.events.last().ok_or_else(|| {
            failure(
                io,
                ErrorCode::Unauthorized,
                "checkpoint has no canonical head",
            )
        })?;
        let head_hold;
        let (head, _) = crate::store::operations::bounded_operation_with_retained_quota(
            &self.stream,
            binding.authority(),
            &verifier,
            expected.operation_id,
            self.config.history_limits.maximum_bytes,
            true,
            &mut observer,
        )
        .await?;
        head_hold = io
            .map(|io| io.guard_owned(head.as_ref().map_or(0, |(_, retained)| *retained)))
            .transpose()?;
        let head = (head, head_hold);
        check_cancellation(cancellation, io)?;
        if head.0.as_ref().map(|(event, _)| event) != Some(expected) {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "checkpoint head differs from immutable canonical Stream event",
            ));
        }
        drop(head);
        Reducer::restore_with_work(snapshot, verifier, SchemaRegistry::new(), &mut observer)
    }
}
