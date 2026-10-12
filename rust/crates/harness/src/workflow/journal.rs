use super::*;

/// Atomic durable checkpoint and command journal.
pub trait WorkflowJournal: acyclic_stream::ProviderPlatform {
    /// Task lease fencing every publication.
    fn task_binding(&self) -> Option<(crate::TaskId, &crate::scheduler::LeaseFence)> { None }
    /// Borrowed admitted identity of a bound owner.
    fn original_machine_identity(&self) -> Option<&MachineIdentity> { None }
    /// Atomically admits one exact workflow instance.
    fn admit<'a>(&'a self, admission: WorkflowAdmission) -> BoxFuture<'a, Result<WorkflowAdmission>>;
    /// Reads the retained admission.
    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>>;
    /// Reads at most `maximum` transitions after the given revision.
    fn replay(&self, after: u64, maximum: u32) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>>;
    /// Atomically commits one checkpoint and its outbox with CAS and idempotency.
    fn commit<'a>(&'a self, expected_revision: u64, idempotency_key: IdempotencyKey, record: WorkflowRecord)
        -> BoxFuture<'a, Result<WorkflowCommitOutcome>>;
    /// Whether native callbacks share the original fenced journal.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_work(&self) -> bool { false }
    /// Reads admission with original-ledger decode custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admission_with_native_work<'a>(&'a self, _: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (Option<WorkflowAdmission>, acyclic_fs::NativeAllocation)>> {
        Err(unsupported("journal has no native admission callback"))
    }
    /// Publishes admission through the original fenced engine.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_with_native_work<'a>(&'a self, _: WorkflowAdmission, _: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (WorkflowAdmission, acyclic_fs::NativeAllocation)>> {
        Err(unsupported("journal has no native admission publication callback"))
    }
    /// Reads a bounded page with original-ledger custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn replay_with_native_work<'a>(&'a self, _: u64, _: u32, _: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (Vec<WorkflowRecord>, acyclic_fs::NativeAllocation)>> {
        Err(unsupported("journal has no native replay callback"))
    }
    /// Commits with the original CAS, cancellation and lease fence.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn commit_with_native_work<'a>(&'a self, _: u64, _: IdempotencyKey, _: WorkflowRecord, _: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (WorkflowCommitOutcome, acyclic_fs::NativeAllocation)>> {
        Err(unsupported("journal has no native commit callback"))
    }
}

pub(super) async fn admission(journal: &dyn WorkflowJournal, io: Option<&PreparationIo>) -> Result<(Option<WorkflowAdmission>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| unsupported("native admission requires original work"))?;
        let (value, custody) = journal.admission_with_native_work(work)?.await?;
        if !work.shares_ledger(custody.work()) {
            drop(value);
            return Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: Some(custody) });
        }
        return Ok((value, Some(custody)));
    }
    let _ = io;
    Ok((journal.admission().await?, None))
}

pub(super) async fn page(journal: &dyn WorkflowJournal, after: u64, maximum: u32, io: Option<&PreparationIo>)
    -> Result<(Vec<WorkflowRecord>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| unsupported("native replay requires original work"))?;
        let (records, custody) = journal.replay_with_native_work(after, maximum, work)?.await?;
        if !work.shares_ledger(custody.work()) {
            drop(records);
            return Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: Some(custody) });
        }
        return Ok((records, Some(custody)));
    }
    let _ = io;
    Ok((journal.replay(after, maximum).await?, None))
}
