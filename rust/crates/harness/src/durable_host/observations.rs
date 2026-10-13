//! Task observations reuse canonical retained records and payloads.
use super::*;
impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn task_outcome(
        &self,
        task: TaskId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Outcome<Value>>> {
        let mut history = io.map(|io| io.history_io());
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(
            &mut work,
            std::mem::size_of::<Option<Outcome<Value>>>() as u64,
        )?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(quota.work()).await?;
        let state = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
            quota.work(),
        )?;
        let observed = match &state.outcome {
            Some(Outcome::Succeeded(file)) => {
                Some(Outcome::Succeeded(file.clone_in_quota(&mut quota)?))
            }
            Some(Outcome::Failed { message }) => Some(Outcome::Failed {
                message: crate::core::copy_text(message, &mut quota)?,
            }),
            Some(Outcome::Cancelled) => Some(Outcome::Cancelled),
            Some(Outcome::Indeterminate { operation_id }) => Some(Outcome::Indeterminate {
                operation_id: *operation_id,
            }),
            None => None,
        };
        drop(coordinator);
        match observed {
            Some(Outcome::Succeeded(file)) => {
                let value = self.read_json(&file, io).await?;
                let retained = crate::executor::json::json_value_logical_owned_bytes(&value)?;
                drop(file);
                quota.replace(std::mem::size_of::<Option<Outcome<Value>>>() as u64)?;
                quota.adopt(retained)?;
                quota.transfer();
                Ok(Some(Outcome::Succeeded(value)))
            }
            value => {
                quota.transfer();
                Ok(value)
            }
        }
    }
}
