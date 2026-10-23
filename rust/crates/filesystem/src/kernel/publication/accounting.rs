use super::*;

pub(super) struct PublicationStepFailure<E> {
    pub(super) cause: E,
    pub(super) work: Option<WorkCounters>,
}

pub(super) fn accept_backend(
    accounting: &mut KernelAccounting<'_>,
    more: WorkCounters,
) -> Result<(), crate::FsError> {
    let (allocations, work, budget, _) =
        accounting.standalone_parts().ok_or(WorkError::Overflow)?;
    *work = work.with_backend(more, allocations.live_bytes())?;
    work.verify(budget)?;
    Ok(())
}

pub(super) fn accept_step<T, E: From<crate::FsError>>(
    accounting: &mut KernelAccounting<'_>,
    result: Result<(T, Option<WorkCounters>), PublicationStepFailure<E>>,
) -> Result<T, E> {
    match result {
        Ok((value, work)) => {
            if let Some(work) = work {
                accept_backend(accounting, work).map_err(E::from)?;
            }
            Ok(value)
        }
        Err(failure) => {
            if let Some(work) = failure.work {
                let _ = accept_backend(accounting, work);
            }
            Err(failure.cause)
        }
    }
}

pub(super) fn retain(
    accounting: &mut KernelAccounting<'_>,
    bytes: u64,
) -> Result<(), crate::FsError> {
    accounting.check()?;
    if let Some((allocations, work, budget, _)) = accounting.standalone_parts() {
        allocations
            .record_owned_bytes(bytes, work)
            .map_err(|error| {
                error.into_work_or(crate::FsError::Decode(
                    CanonicalDecodeError::AllocationFailed,
                ))
            })?;
        work.verify(budget)?;
    }
    Ok(())
}
