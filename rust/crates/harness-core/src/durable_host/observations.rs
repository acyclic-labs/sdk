//! Task observations reuse canonical retained records and payloads.
use super::*;
impl<P: StreamProvider> CoordinatorTaskHost<P> {
    // Lifecycle/control decisions need the authenticated outcome variant, not
    // the immutable result body. Keep this separate from the payload API so a
    // status read cannot trigger result IO, decoding or canonical re-encoding.
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "filesystem-local", all(test, feature = "filesystem"))
    ))]
    #[doc(hidden)]
    pub async fn terminal_status(&self, task: TaskId) -> Result<Option<Outcome<()>>> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        let state = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
            &mut None,
        )?;
        Ok(state.outcome.as_ref().map(project_terminal))
    }

    pub(super) async fn task_outcome(
        &self,
        task: TaskId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<Outcome<Value>>> {
        #[cfg(feature = "filesystem")]
        let mut history = io.map(|io| io.history_io());
        #[cfg(feature = "filesystem")]
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn crate::store::HistoryIo> = None;
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
        let observed = state
            .outcome
            .as_ref()
            .map(|outcome| {
                let copied = outcome.clone_with_work(quota.work())?;
                quota.adopt(copied.logical_owned_bytes()?)?;
                Ok::<_, Error>(copied)
            })
            .transpose()?;
        drop(coordinator);
        let outcome = match observed {
            Some(Outcome::Succeeded(file)) => {
                let value = self.read_json(&file, io).await?;
                let retained = crate::executor::json::json_value_logical_owned_bytes(&value)?;
                drop(file);
                quota.replace(std::mem::size_of::<Option<Outcome<Value>>>() as u64)?;
                quota.adopt(retained)?;
                Some(Outcome::Succeeded(value))
            }
            Some(Outcome::Failed { message }) => Some(Outcome::Failed { message }),
            Some(Outcome::Cancelled) => Some(Outcome::Cancelled),
            Some(Outcome::Indeterminate {
                operation_id,
                cause,
            }) => Some(Outcome::Indeterminate {
                operation_id,
                cause,
            }),
            None => None,
        };
        quota.transfer();
        Ok(outcome)
    }
}

#[cfg(any(test, all(feature = "filesystem-local", not(target_arch = "wasm32"))))]
fn project_terminal<T>(outcome: &Outcome<T>) -> Outcome<()> {
    match outcome {
        Outcome::Succeeded(_) => Outcome::Succeeded(()),
        Outcome::Failed { message } => Outcome::Failed {
            message: message.clone(),
        },
        Outcome::Cancelled => Outcome::Cancelled,
        Outcome::Indeterminate {
            operation_id,
            cause,
        } => Outcome::Indeterminate {
            operation_id: *operation_id,
            cause: cause.clone(),
        },
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

#[cfg(all(
    any(test, feature = "test-support"),
    not(acyclic_loom),
    feature = "filesystem-local",
    not(target_arch = "wasm32")
))]
impl<P: StreamProvider> CoordinatorTaskHost<P> {
    #[doc(hidden)]
    pub async fn test_coordinator_barrier(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) {
        let _coordinator = self.coordinator.lock().await;
        entered.send(()).unwrap();
        let _ = release.await;
    }
}
