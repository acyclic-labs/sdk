//! Shared synchronous driver and machine contract for resumable point frontiers.

use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{ObjectFailure, ObjectId, ObjectRead, ObjectReceipt, ObjectStore};

#[derive(Clone, Copy)]
pub(crate) struct ReadRequest {
    pub(crate) page: ObjectId,
    pub(crate) maximum_bytes: u64,
    pub(crate) remaining: WorkBudget,
    pub(crate) prospective: WorkCounters,
}

pub(crate) trait Machine {
    type Output;
    type Failure;

    fn complete(&mut self) -> Result<Option<Self::Output>, Self::Failure>;
    fn prepare_read(&mut self) -> Result<ReadRequest, Self::Failure>;
    fn accept(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<Self::Output>, Self::Failure>;
    fn storage_failure(&self, prospective: WorkCounters, failure: ObjectFailure) -> Self::Failure;
    fn cancelled(&self) -> Self::Failure;
}

pub(crate) fn drive_sync<S, M>(store: &S, machine: &mut M) -> Result<M::Output, M::Failure>
where
    S: ObjectStore,
    M: Machine,
{
    loop {
        if let Some(output) = machine.complete()? {
            return Ok(output);
        }
        let request = machine.prepare_read()?;
        let receipt = ObjectStore::read(
            store,
            request.page,
            request.maximum_bytes,
            request.remaining,
        )
        .map_err(|failure| machine.storage_failure(request.prospective, failure))?;
        if let Some(output) = machine.accept(request.prospective, &receipt)? {
            return Ok(output);
        }
    }
}
