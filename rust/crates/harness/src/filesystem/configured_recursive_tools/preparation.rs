use super::*;
use crate::executor::stock_boundary;
use crate::filesystem::PreparationIo;
use crate::wire::ErrorCode;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn preparation(
        &self,
        context: &Arc<TaskContext>,
        token: &TaskResourcePreparation,
        cancellation: &CancellationToken,
        _native_reconciliation_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<RetainedSourceBinding<A, O>> {
        native_io(io)?;
        if io.and_then(PreparationIo::original_native_work).is_some() {
            check_cancellation(cancellation, io)?;
        }
        let reader = context.as_ref();
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
            reader.owned_task_binding(Some(&crate::executor::PreparationIo::from_native_work(
                work,
            )?))?;
            if !_native_reconciliation_only {
                reader.require_original_native_dispatch(work)?;
            }
            token
                .verify(
                    _native_reconciliation_only,
                    Some(&PreparationIo::from_native_work(work)?),
                )?
                .await?;
        }
        let caller = self.caller(caller_task, io)?;
        self.check_caller_allocation(caller, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            io.check_json_byte_bound(
                &preparation.payload,
                acyclic_stream::MAX_RECORD_BYTES as u64,
            )?;
        }
        let request: ForkRequest = match io {
            Some(io) => crate::filesystem::retain_decoded(
                io.value_decode_preparation(&preparation.payload, ErrorCode::Storage)?,
            )?,
            None => serde_json::from_value(preparation.payload.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?,
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
        let ordinary;
        let observer = if let Some(io) = io {
            io
        } else {
            ordinary = PreparationIo::from_budget(self.config.content_budget, cancellation)?;
            &ordinary
        };
        if let Some(checkpoint) = &preparation.source_checkpoint {
            let bytes = reader.read_file(checkpoint, Some(observer)).await?;
            let record: RecursiveForkSourceIntent = match io {
                #[cfg(not(target_arch = "wasm32"))]
                Some(io) => crate::filesystem::retain_decoded(io.decode(
                    std::marker::PhantomData,
                    &bytes,
                    ErrorCode::Storage,
                )?)?,
                _ => serde_json::from_slice(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
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
        let reducer = self
            .open(&caller.owner, Some(request.parent_revision), Some(observer))
            .await?
            .into_reducer();
        let owner = self.source_owner(reader, cancellation, io).await?;
        let resolver = crate::runtime::content_residency_verifier(Arc::clone(context), owner, io)?;
        let verifier =
            crate::executor::with_history_io(io, |work| caller.owner.issuer.verifier(work))?;
        let input = PreparationIo::temporary(io)?;
        let source = restore_recursive_fork_source(
            Arc::clone(&self.filesystem),
            &self.stream,
            (reducer, input),
            &verifier,
            &caller.owner.scope,
            &caller.project,
            &self.config.stream_provider,
            request,
            self.config.history_limits,
            token.project_source(),
            resolver,
            cancellation,
        )
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            token
                .verify(
                    _native_reconciliation_only,
                    Some(&PreparationIo::from_native_work(work)?),
                )?
                .await?;
            check_cancellation(cancellation, io)?;
        }
        Ok(source)
    }
}
