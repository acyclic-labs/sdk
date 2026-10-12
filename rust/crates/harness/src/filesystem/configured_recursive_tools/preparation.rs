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
        if let Some(checkpoint) = &preparation.source_checkpoint {
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
                return Err(failure(
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
        if matches!(context, WorkerContext::Native(_))
            && io.and_then(PreparationIo::original_native_work).is_none()
        {
            return Err(failure(
                io,
                ErrorCode::Unsupported,
                "native source requires original IO",
            ));
        }
        let owner = self.source_owner(reader, cancellation, io).await?;
        let shared = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(shared) => Arc::clone(shared),
            WorkerContext::Ordinary(reader) => Arc::new(reader.clone()),
        };
        let resolver = crate::runtime::content_residency_verifier(shared, owner, io)?;
        let verifier =
            crate::executor::with_history_io(io, |work| caller.owner.issuer().verifier(work))?;
        let source = restore_recursive_fork_source(
            Arc::clone(&self.filesystem),
            &self.stream,
            reducer,
            &verifier,
            &caller.owner.scope,
            &caller.project,
            &self.config.stream_provider,
            request,
            self.config.history_limits,
            token.project_source(),
            resolver,
            cancellation,
            io,
        )
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            if reconciliation_only {
                token.verify_reconciliation_with_native_work(work)?.await?;
            } else {
                token.verify_creation_with_native_work(work)?.await?;
            }
            check_cancellation(cancellation, io)?;
        }
        Ok(source)
    }
}
