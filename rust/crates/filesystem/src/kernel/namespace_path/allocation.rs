use super::super::codec::ReadAllocationStep;
use super::*;
pub(crate) enum NamespacePathStep<'a> {
    Allocation(ReadAllocationStep),
    Admit(&'a WorkCounters),
    Charge(&'a WorkCounters),
    Performed(&'a WorkCounters),
}

pub(crate) fn observe_path(
    owned: &mut super::super::file_read::OriginalReadAccounting<'_, '_>,
    step: NamespacePathStep<'_>,
) -> Result<(), crate::FsError> {
    match step {
        NamespacePathStep::Allocation(step) => owned.observe(step),
        NamespacePathStep::Admit(delta) => owned.admit(*delta),
        NamespacePathStep::Charge(delta) => {
            owned.admit(*delta)?;
            owned.record_completed(*delta)
        }
        NamespacePathStep::Performed(delta) => owned.record_completed(*delta),
    }
}

pub(crate) fn finish_owned<T>(
    owned: super::super::file_read::OriginalReadAccounting<'_, '_>,
    result: Result<T, crate::FsError>,
) -> Result<(T, KernelOwnedAllocation), crate::FsError> {
    match result {
        Ok(value) => Ok((value, owned.into_allocation())),
        Err(error) => {
            owned.release_failed()?;
            Err(error)
        }
    }
}
