use super::*;
use std::sync::Mutex;

/// In-process workflow journal for local authoring.
#[derive(Default)]
pub struct MemoryWorkflowJournal {
    pub(super) state: Mutex<MemoryWorkflowState>,
}
#[derive(Default)]
pub(super) struct MemoryWorkflowState {
    admission: Option<WorkflowAdmission>,
    pub(super) records: Vec<WorkflowRecord>,
}
impl WorkflowJournal for MemoryWorkflowJournal {
    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>> {
        Box::pin(async move {
            admission.validate(None)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            match &state.admission {
                Some(existing) if existing == &admission => Ok(existing.clone()),
                Some(_) => Err(Error::Conflict(
                    "workflow admission identity is already bound".into(),
                )),
                None => {
                    state.admission = Some(admission.clone());
                    Ok(admission)
                }
            }
        })
    }
    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>> {
        Box::pin(async move {
            self.state
                .lock()
                .map(|state| state.admission.clone())
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))
        })
    }
    fn replay(&self, after: u64, maximum: u32) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
        Box::pin(async move {
            if maximum == 0 {
                return Err(Error::Invalid("workflow page bounds are invalid".into()));
            }
            let state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            let after = usize::try_from(after)
                .map_err(|_| Error::Invalid("workflow revision is not portable".into()))?;
            let records = state
                .records
                .iter()
                .skip(after)
                .take(maximum as usize)
                .cloned()
                .collect::<Vec<_>>();
            for record in &records {
                record.validate(None)?;
            }
            Ok(records)
        })
    }
    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowRecord>> {
        Box::pin(async move {
            validate_workflow_commit(expected_revision, &key, &record, None)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Storage("workflow journal poisoned".into()))?;
            if let Some(existing) = state
                .records
                .iter()
                .find(|record| record.idempotency_key == key)
            {
                return if existing == &record {
                    Ok(existing.clone())
                } else {
                    Err(Error::Conflict(
                        "workflow retry identity is already bound".into(),
                    ))
                };
            }
            if state
                .records
                .last()
                .is_some_and(|last| !matches!(&last.transition.status, MachineStatus::Suspended))
            {
                return Err(Error::Conflict(
                    "workflow cannot advance after a terminal transition".into(),
                ));
            }
            let admission = state.admission.as_ref().ok_or_else(|| {
                Error::Conflict("workflow transition has no retained admission".into())
            })?;
            if &record.prior
                != state
                    .records
                    .last()
                    .map_or(&admission.initial, |last| &last.next)
            {
                return Err(Error::Conflict(
                    "workflow transition differs from admission".into(),
                ));
            }
            if state.records.iter().any(|existing| {
                existing
                    .operation_ids()
                    .any(|id| record.operation_ids().any(|requested| requested == id))
            }) {
                return Err(Error::Conflict(
                    "workflow command or transition identity is already bound".into(),
                ));
            }
            state.records.push(record.clone());
            Ok(record)
        })
    }
}
