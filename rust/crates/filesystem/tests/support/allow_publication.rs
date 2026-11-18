use acyclic_fs_work::performance::{MeasuredResult, OperationReceipt, WorkBudget, WorkCounters};

pub(crate) fn allow<E>(
    _: WorkBudget,
) -> std::future::Ready<MeasuredResult<OperationReceipt<()>, E>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}
