//! One explicit durability cut across the journal's independently selected stores.
use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn can_defer_durability(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.stream.supports_deferred_durability()
                && self.host.filesystem.supports_deferred_durability()
        }
        #[cfg(target_arch = "wasm32")]
        false
    }

    pub(super) fn prepare_phase_seal<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
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
            let mut authority = acyclic_fs::AuthorityWork::native(work, &cancellation);
            let authority = self
                .host
                .filesystem
                .prepare_durability_barrier(&mut authority)
                .map_err(acyclic_fs::FsError::from)?;
            let stream = self
                .stream
                .prepare_native_durability_barrier(work.runtime_meter())
                .map_err(|error| Error::from(work.runtime_error(error)))?;
            return crate::conversation::BoundedContentFuture::new(io, || async move {
                authority.await.map_err(acyclic_fs::FsError::from)?;
                crate::distributed::native_call_result(work, stream.await)?.map_err(|source| {
                    Error::NativeStream {
                        code: crate::wire::ErrorCode::Storage,
                        source,
                        custody: None,
                    }
                })?;
                Ok(())
            });
        }
        let budget = io
            .map(PreparationIo::remaining)
            .transpose()?
            .unwrap_or(acyclic_fs::WorkBudget::UNBOUNDED);
        let mut authority = acyclic_fs::AuthorityWork::new(budget, &cancellation);
        let barrier = self
            .host
            .filesystem
            .prepare_durability_barrier(&mut authority)
            .map_err(acyclic_fs::FsError::from)?;
        if let Some(io) = io {
            // Reserve the authority and journal flushes before accepting any
            // deferred observation. Completion needs no cancelled-token check.
            io.charge(
                authority
                    .spent()
                    .checked_add(acyclic_fs::WorkCounters {
                        durability_operations: 1,
                        ..Default::default()
                    })
                    .map_err(acyclic_fs::FsError::from)?,
            )?;
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            barrier.await.map_err(acyclic_fs::FsError::from)?;
            self.stream.durability_barrier().await.map_err(Into::into)
        })
    }
}
