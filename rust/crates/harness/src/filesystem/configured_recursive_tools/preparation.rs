use super::*;
use crate::filesystem::{PreparationIo, task_runtime::WorkerContext};
use crate::executor::stock_boundary;
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
            stock_boundary(
                io,
                ErrorCode::Unauthorized,
                "configured preparation has no original caller",
            )
        })?;
        if reader.durable_task_id() != Some(token.origin().preparation_task)
            || token.origin().parent_task != caller_task
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Unauthorized,
                "source restore lacks the actual sealed helper and original caller",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            reader.owned_task_binding_with_native_work(work)?;
            if reconciliation_only {
                token.verify(true, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?.await?;
            } else {
                reader.require_original_native_dispatch(work)?;
                token.verify(false, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?.await?;
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
            return Err(stock_boundary(
                io,
                ErrorCode::Unauthorized,
                "preparation changed its retained sealed Project source",
            ));
        }
        if let Some(checkpoint) = &preparation.source_checkpoint {
            let ordinary;
            let content_io = match io {
                Some(io) => io,
                None => {
                    ordinary = PreparationIo::from_budget(self.config.content_budget, cancellation)?;
                    &ordinary
                }
            };
            let bytes = reader.read_file(checkpoint, Some(content_io)).await?;
            let record: RecursiveForkSourceIntent = match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) => crate::filesystem::retain_decoded(io.decode(
                    std::marker::PhantomData,
                    &bytes,
                    ErrorCode::Storage,
                )?),
                _ => serde_json::from_slice(&bytes).map_err(invalid_json)?,
            };
            if record.request != request
                || record.caller != caller_task
                || record.child_operation_id != preparation.child.operation_id
            {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Unauthorized,
                    "configured preparation checkpoint changed original caller/request",
                ));
            }
        }
        let ordinary;
        let observer = match io {
            Some(io) => io,
            None => {
                ordinary = PreparationIo::from_budget(self.config.content_budget, cancellation)?;
                &ordinary
            }
        };
        let reducer = self
            .open(&caller.owner, Some(request.parent_revision), Some(observer))
            .await?
            .into_reducer();
        #[cfg(not(target_arch = "wasm32"))]
        if let WorkerContext::Native(shared) = context {
            let io = io.ok_or_else(|| {
                stock_boundary(
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
                token.verify(true, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?.await?;
            } else {
                token.verify(false, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?.await?;
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
}
