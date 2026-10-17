use super::*;

pub(super) async fn publish<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    status: ChildStatus,
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
) -> Result<bool> {
    #[cfg(acyclic_harness_loom)]
    if crate::task_volume::model_checking::enabled(
        crate::task_volume::model_checking::Fault::SkipChildInputAck,
    ) {
        return Ok(true);
    }
    super::publish(files, reference, status, values, generation).await
}

#[cfg(all(test, acyclic_harness_loom, acyclic_loom))]
mod models;
