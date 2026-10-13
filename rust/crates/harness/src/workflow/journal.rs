use super::*;

/// Atomic durable checkpoint and command journal.
pub trait WorkflowJournal: acyclic_stream::ProviderPlatform {
    /// Task lease fencing every publication.
    fn task_binding(&self) -> Option<(crate::TaskId, &crate::scheduler::LeaseFence)> {
        None
    }
    /// Borrowed admitted identity of a bound owner.
    fn original_machine_identity(&self) -> Option<&MachineIdentity> {
        None
    }
    /// Atomically admits one exact workflow instance.
    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>>;
    /// Reads the retained admission.
    fn admission<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<
        crate::store::history::HistoryContentFuture<'a, (Option<WorkflowAdmission>, NativeCustody)>,
    >;
    /// Reads at most `maximum` transitions after the given revision.
    fn replay<'a>(
        &'a self,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, (Vec<WorkflowRecord>, NativeCustody)>>;
    /// Atomically commits one checkpoint and its outbox with CAS and idempotency.
    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowRecord>>;
    /// Whether native callbacks share the original fenced journal.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_work(&self) -> bool {
        false
    }
    /// Publishes admission through the original fenced engine.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_with_native_work<'a>(
        &'a self,
        _: WorkflowAdmission,
        _: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (WorkflowAdmission, acyclic_fs::NativeAllocation)>>
    {
        Err(unsupported(
            "journal has no native admission publication callback",
        ))
    }
    /// Commits with the original CAS, cancellation and lease fence.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn commit_with_native_work<'a>(
        &'a self,
        _: u64,
        _: IdempotencyKey,
        _: WorkflowRecord,
        _: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (WorkflowRecord, acyclic_fs::NativeAllocation)>> {
        Err(unsupported("journal has no native commit callback"))
    }
}

pub(super) async fn admission(
    journal: &dyn WorkflowJournal,
    io: Option<&PreparationIo>,
) -> Result<(Option<WorkflowAdmission>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native = io
        .map(|io| {
            io.original_native_work()
                .ok_or_else(|| unsupported("native admission requires original work"))
        })
        .transpose()?;
    let observed = journal.admission(io)?.await?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = native {
        if observed
            .1
            .as_ref()
            .is_none_or(|custody| !work.shares_ledger(custody.work()))
        {
            drop(observed.0);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: observed.1,
            });
        }
    }
    Ok(observed)
}

pub(super) async fn page(
    journal: &dyn WorkflowJournal,
    after: u64,
    maximum: u32,
    io: Option<&PreparationIo>,
) -> Result<(Vec<WorkflowRecord>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native = io
        .map(|io| {
            io.original_native_work()
                .ok_or_else(|| unsupported("native replay requires original work"))
        })
        .transpose()?;
    let observed = journal.replay(after, maximum, io)?.await?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = native {
        if observed
            .1
            .as_ref()
            .is_none_or(|custody| !work.shares_ledger(custody.work()))
        {
            drop(observed.0);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: observed.1,
            });
        }
    }
    Ok(observed)
}
