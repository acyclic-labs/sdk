use acyclic_fs_work::performance::{MeasuredResult, OperationReceipt, WorkBudget, WorkCounters};

/// Authorizes a publication without charging work to the authorization callback.
pub fn allow<E>(_: WorkBudget) -> std::future::Ready<MeasuredResult<OperationReceipt<()>, E>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}
