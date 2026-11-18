use super::*;

pub(super) struct PublicationStepFailure<E> {
    pub(super) cause: E,
    pub(super) work: Result<Option<WorkCounters>, crate::WorkObservationError>,
}

impl<E> From<OperationFailure<E>> for PublicationStepFailure<E> {
    fn from(failure: OperationFailure<E>) -> Self {
        Self {
            cause: failure.error,
            work: failure.work.map(|work| Some(*work)),
        }
    }
}

pub(super) fn accept_step<T, E: From<crate::FsError>>(
    accounting: &mut KernelAccounting<'_>,
    result: Result<(T, Option<WorkCounters>), PublicationStepFailure<E>>,
) -> Result<T, E> {
    match result {
        Ok((value, work)) => {
            if let Some(work) = work {
                accounting.accept_backend(work).map_err(E::from)?;
            }
            Ok(value)
        }
        Err(failure) => {
            match failure.work {
                Ok(Some(work)) => accounting.accept_backend(work).map_err(E::from)?,
                Ok(None) => {}
                Err(observation) => accounting.invalidate_receipt(observation),
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

#[cfg(test)]
#[test]
fn unavailable_child_receipt_preserves_cause_and_fences_parent() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let observation = crate::WorkObservationError::Poisoned;
    let failure =
        OperationFailure::without_receipt(crate::FsError::AccountingHandoffActive, observation);

    let result = accept_step::<(), _>(&mut accounting, Err(failure.into()));

    assert!(matches!(
        result,
        Err(crate::FsError::AccountingHandoffActive)
    ));
    assert_eq!(accounting.spent(), Err(observation));
    assert!(accounting.check().is_err());
    assert!(accounting.accept_backend(WorkCounters::UNCHARGED).is_err());
}
