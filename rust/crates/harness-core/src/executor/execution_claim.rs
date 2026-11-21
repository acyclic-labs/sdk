//! Live execution ownership, separate from durable replay observations.
use super::*;

/// Shared exclusion for all handles of the same execution journal.
#[derive(Default)]
pub struct ExecutionClaims(acyclic_fs::OperationClaims);

type Storage = Option<crate::filesystem::PreparationCustody>;

/// Owns a live attempt until its execution future has finished or is dropped.
pub struct ExecutionClaim<'a> {
    _claim: acyclic_fs::OperationClaim<'a>,
}

/// Original producer custody, or its exact-generation terminal observation.
pub enum ExecutionAttempt<'a> {
    /// Owns the one operation that may inspect replay and dispatch work.
    Owned(ExecutionClaim<'a>),
    /// Pins the existing producer without permitting another dispatch.
    Observed(acyclic_fs::OperationObservation),
}

impl<'a> ExecutionAttempt<'a> {
    /// Ordinary execution rejects contention; summary consumers retain the
    /// observation instead and wait for that producer's physical retirement.
    pub fn into_owner(self, io: Option<&PreparationIo>) -> Result<ExecutionClaim<'a>> {
        match self {
            Self::Owned(owner) => Ok(owner),
            Self::Observed(_) => Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "operation already has a live executor",
            )),
        }
    }
}

impl ExecutionClaim<'_> {
    /// Pins the shared operation event before handing custody to model work.
    pub fn observe(&self) -> acyclic_fs::OperationObservation {
        self._claim.observe()
    }

    /// Retires the original shared claim after its result has been committed.
    pub fn release(self) {
        self._claim.release();
    }
}

/// The tool calls whose start this live owner recorded and has not yet seen
/// settle. The set is process-local and belongs to one provider owner, so a
/// reopened owner begins with none: a call that was started but unsettled when
/// it took the actor's delivery was started by a predecessor, whose producer
/// that acquisition fenced. No later result can come from that producer, so
/// the call's executor decides it. An entry keeps the funding of the start
/// that recorded it until its call settles; the set holds only open calls.
#[derive(Default)]
pub struct StartedCalls(crate::sync::SyncMutex<std::collections::BTreeMap<OperationId, Storage>>);

impl StartedCalls {
    fn calls(
        &self,
    ) -> impl std::ops::DerefMut<Target = std::collections::BTreeMap<OperationId, Storage>> + '_
    {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records a start before it is published, so a start of this owner is
    /// never mistaken for a fenced predecessor's.
    pub(crate) fn start(&self, call: OperationId, io: Option<&PreparationIo>) -> Result<()> {
        let mut calls = self.calls();
        if let std::collections::btree_map::Entry::Vacant(entry) = calls.entry(call) {
            let bytes = std::mem::size_of::<(OperationId, Storage)>() as u64;
            entry.insert(fund(io, bytes, 1)?);
        }
        Ok(())
    }

    /// Retires a call whose settlement this owner observed.
    pub(crate) fn settle(&self, call: OperationId) {
        self.calls().remove(&call);
    }

    /// Whether a recorded start belongs to a producer this owner fenced.
    pub(crate) fn fenced(&self, call: OperationId) -> bool {
        !self.calls().contains_key(&call)
    }
}

/// The started-call set of the provider owner that `claims` belongs to.
pub(crate) fn started_calls(
    claims: &acyclic_fs::OperationClaims,
    io: Option<&PreparationIo>,
) -> Result<std::sync::Arc<StartedCalls>> {
    claims.shared_resource(
        &mut (),
        |()| crate::model::charge_items(io, 1),
        |bytes, ()| fund(io, bytes, 1),
        |()| Ok(std::sync::Arc::new(StartedCalls::default())),
    )
}

/// Funds owner-retained bytes from the caller's original work.
fn fund(io: Option<&PreparationIo>, bytes: u64, allocations: u64) -> Result<Storage> {
    io.map(|io| {
        for _ in 0..allocations {
            io.allocation_attempt(bytes)?;
        }
        io.retain_owned(bytes)?;
        io.take_owned_custody(bytes)
    })
    .transpose()
}

impl ExecutionClaims {
    /// The started-call set shared by every handle of this journal's owner.
    pub fn started_calls(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<std::sync::Arc<StartedCalls>> {
        started_calls(&self.0, io)
    }

    /// Claims an operation before inspecting its replay observations.
    pub fn acquire<'a>(
        &'a self,
        namespace: [u8; 32],
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<ExecutionAttempt<'a>> {
        acquire(&self.0, namespace, operation, io)
    }
}

pub(crate) fn acquire<'a>(
    claims: &'a acyclic_fs::OperationClaims,
    namespace: [u8; 32],
    operation: OperationId,
    io: Option<&PreparationIo>,
) -> Result<ExecutionAttempt<'a>> {
    let mut key = [0; 48];
    key[..32].copy_from_slice(&namespace);
    key[32..].copy_from_slice(&operation.into_bytes());
    let claim = claims.claim(
        key,
        || crate::model::charge_items(io, 1),
        |bytes, allocations| fund(io, bytes, allocations),
    )?;
    Ok(match claim {
        acyclic_fs::OperationAttempt::Owned(claim) => {
            ExecutionAttempt::Owned(ExecutionClaim { _claim: claim })
        }
        acyclic_fs::OperationAttempt::Observed(observation) => {
            ExecutionAttempt::Observed(observation)
        }
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn claim_nodes_keep_original_native_funding_after_the_preparation_frame_drops() -> Result<()> {
        let native = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs_work::performance::work_budget(1_000_000, 1_000_000),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = native.live_allocation_bytes()?;
        let claims = ExecutionClaims::default();
        let io = PreparationIo::from_native_work(&native)?;
        let first = claims
            .acquire([1; 32], OperationId::from_bytes([1; 16]), Some(&io))?
            .into_owner(Some(&io))?;
        let second = claims
            .acquire([1; 32], OperationId::from_bytes([2; 16]), Some(&io))?
            .into_owner(Some(&io))?;
        drop(io);
        assert!(native.live_allocation_bytes()? > baseline);
        drop(first);
        assert!(native.live_allocation_bytes()? > baseline);
        drop(second);
        assert_eq!(native.live_allocation_bytes()?, baseline);
        Ok(())
    }
}
