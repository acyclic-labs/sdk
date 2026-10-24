//! The latest response ordinal is part of the authenticated journal cut.
use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn latest_response_step(
        &self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<u32>> {
        self.model_dispatch_state(operation, io)
            .await
            .map(|state| state.0)
    }

    pub(in crate::filesystem) async fn model_was_dispatched(
        &self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        self.model_dispatch_state(operation, io)
            .await
            .map(|state| state.1)
    }

    async fn model_dispatch_state(
        &self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<(Option<u32>, bool)> {
        let scratch = crate::filesystem::PreparationScope::new(io)?;
        let step = {
            let io = scratch.io();
            self.require_operation(operation, io)?;
            crate::executor::with_history_io(io, |work| {
                ContentGrant::authorize_volume(
                    &self.verifier,
                    &self.scope,
                    &self.volume,
                    VolumeOperation::Read,
                    self.maximum_payload_bytes,
                    work,
                )
            })?;
            let mut cache = self.verified.lock().await;
            self.refresh_cached_summary(operation, &mut cache, io)
                .await?;
            (
                cache.summary().latest_response,
                cache.summary().model_dispatched,
            )
        };
        scratch.finish(0)?;
        Ok(step)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
