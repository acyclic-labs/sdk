//! Interfaces.
use super::*;

impl<P: StreamProvider> DurableTaskHost for CoordinatorTaskHost<P> {
    fn verify_dispatch_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, false, &mut None).await })
    }

    fn verify_execution_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, true, &mut None).await })
    }

    fn claim_model_dispatch<'a>(
        &'a self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.claim_model_dispatch_with_work(
                task_id,
                attempt_id,
                step,
                request_digest,
                &fence,
                &mut None,
            )
            .await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        Ok(true)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn claim_model_dispatch_with_native_work<'a>(
        &'a self,
        task: TaskId,
        attempt: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: &'a crate::scheduler::LeaseFence,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?;
            self.claim_model_dispatch_with_work(
                task,
                attempt,
                step,
                request_digest,
                fence,
                &mut Some(&mut io),
            )
            .await
        })
    }
}
