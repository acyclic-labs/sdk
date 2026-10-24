//! One explicit durability cut across the journal's independently selected stores.
use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn can_defer_durability(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.stream.supports_deferred_durability()
                && self.host.filesystem.supports_deferred_durability()
                && self
                    .owner
                    .as_ref()
                    .is_none_or(|(owner, _)| Arc::ptr_eq(&self.stream, &owner.stream()))
                && self
                    .task_files
                    .as_ref()
                    .is_none_or(|files| files.inner.shares_provider(&self.stream))
        }
        #[cfg(target_arch = "wasm32")]
        false
    }

    pub(super) async fn seal_phase(&self, io: Option<&PreparationIo>) -> Result<()> {
        if !self.can_defer_durability() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "execution stores cannot seal deferred durability",
            ));
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            self.host
                .filesystem
                .durability_barrier(work, &cancellation)
                .await?;
            let result = self
                .stream
                .native_durability_barrier(work.runtime_meter())
                .await;
            crate::distributed::native_call_result(work, result)?.map_err(|source| {
                Error::NativeStream {
                    code: crate::wire::ErrorCode::Storage,
                    source,
                    custody: None,
                }
            })?;
            return Ok(());
        }
        let budget = io
            .map(PreparationIo::remaining)
            .transpose()?
            .unwrap_or(acyclic_fs::WorkBudget::UNBOUNDED);
        let result = self
            .host
            .filesystem
            .durability_barrier(budget, &cancellation)
            .await;
        if let Some(io) = io {
            io.receive(result)?;
            // Admission precedes the stream's physical flush, including failures.
            io.charge(acyclic_fs::WorkCounters {
                durability_operations: 1,
                ..Default::default()
            })?;
        } else {
            result.map_err(|failure| Error::from(failure.error))?;
        }
        self.stream.durability_barrier().await.map_err(Into::into)
    }
}
