//! Control.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Cancels under the original owner and retained retry identity.
    pub async fn cancel(&self, task_id: TaskId, native: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = native
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = io.as_mut().map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work: Option<&mut dyn crate::store::HistoryIo> = None;
        let _ = native;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        let operation_id = OperationId::from_bytes(task_id.into_bytes());
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(48)?;
            io.reserve_text(&mut key, 48)?;
            io.copy_bytes(48)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-cancel:{operation_id}").expect("writing into String");
        self.coordinator
            .lock()
            .await
            .cancel_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation_id,
                IdempotencyKey::new(key)?,
                true,
                None,
                &mut work,
            )
            .await?;
        Ok(())
    }

    /// Authenticates operation controls against this task owner's trust root.
    pub async fn authorize_operation_control(
        &self,
        request: &crate::wire_api::OperationControlRequest,
    ) -> Result<()> {
        request.authenticate(&self.verifier)?;
        if &request.owner != self.verifier.audience() {
            return Err(Error::Unauthorized(
                "operation control belongs to another owner".into(),
            ));
        }
        if !request
            .scope
            .capabilities()
            .contains(capability::OPERATION_OBSERVE)
            && !request
                .scope
                .capabilities()
                .contains(capability::OPERATION_CANCEL)
        {
            return Err(Error::Unauthorized(
                "operation control capability is missing".into(),
            ));
        }
        Ok(())
    }

    /// Observes one exact operation through the existing coordinator.
    pub async fn observe_control(
        &self,
        request: &crate::wire_api::OperationControlRequest,
    ) -> Result<crate::scheduler::OperationState> {
        self.authorize_operation_control(request).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        coordinator
            .observe_operation(
                &request.owner,
                &request.scope,
                &self.verifier,
                request.operation_id,
                &mut None,
            )
            .cloned()
    }

    /// Cancels one operation with the caller's retained retry identity.
    pub async fn cancel_control(
        &self,
        request: &crate::wire_api::CancelControlRequest,
    ) -> Result<crate::scheduler::OperationState> {
        self.authorize_operation_control(&request.control).await?;
        let mut coordinator = self.coordinator.lock().await;
        let (_, state) = coordinator
            .cancel_operation(
                &request.control.owner,
                &request.control.scope,
                &self.verifier,
                request.control.operation_id,
                request.idempotency_key.clone(),
                request.recursive,
                None,
                &mut None,
            )
            .await?;
        Ok(state)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Reads a task outcome on its original native ledger.
    pub async fn outcome_with_native_work(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<Outcome<Value>>, acyclic_fs::NativeAllocation)> {
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        let metadata = std::mem::size_of::<Option<Outcome<Value>>>() as u64;
        crate::store::history::HistoryIo::retain_owned(&mut io, metadata)?;
        let (outcome, owned) = self.outcome_with_work(task, &mut Some(&mut io)).await?;
        let custody =
            io.take_native_custody(owned.checked_add(metadata).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task outcome custody overflows",
            })?)?;
        Ok((outcome, custody))
    }

    pub(super) async fn outcome_with_work(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Option<Outcome<Value>>, u64)> {
        let observed = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            let operation = coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(task.into_bytes()),
                work,
            )?;
            operation
                .outcome
                .as_ref()
                .map(|outcome| outcome.clone_with_work(work))
                .transpose()?
        };
        match observed {
            Some(Outcome::Succeeded(reference)) => {
                let (value, owned) = self.read_json(&reference, work).await?;
                Ok((Some(Outcome::Succeeded(value)), owned))
            }
            Some(Outcome::Failed { message }) => {
                let owned = if work.is_some() {
                    message.capacity() as u64
                } else {
                    0
                };
                Ok((Some(Outcome::Failed { message }), owned))
            }
            Some(Outcome::Cancelled) => Ok((Some(Outcome::Cancelled), 0)),
            Some(Outcome::Indeterminate { operation_id }) => {
                Ok((Some(Outcome::Indeterminate { operation_id }), 0))
            }
            None => Ok((None, 0)),
        }
    }

    /// Requests a direct child's entire subtree to stop under the caller's retained lease.
    #[cfg(feature = "filesystem")]
    pub async fn cancel_child_owned(
        &self,
        parent: TaskId,
        fence: &crate::scheduler::LeaseFence,
        child: TaskId,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::scheduler::OperationState> {
        self.verify_owner(parent, fence, false, work).await?;
        let admitted = self
            .admission_with_work(OperationId::from_bytes(parent.into_bytes()), work)
            .await?;
        if !admitted.grants.contains(capability::OPERATION_CANCEL)
            || self
                .admission_with_work(OperationId::from_bytes(child.into_bytes()), work)
                .await?
                .parent
                != Some(parent)
        {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "cancellation requires an authorized direct child",
            ));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let parent_state = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(parent.into_bytes()),
            work,
        )?;
        crate::scheduler::require_execution_owner(parent_state, fence, false, work)?;
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(91)?;
            io.reserve_text(&mut key, 91)?;
            io.copy_bytes(91)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-child-cancel:{parent}:{operation}").expect("writing into String");
        coordinator
            .cancel_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(child.into_bytes()),
                IdempotencyKey::new(key)?,
                true,
                Some((OperationId::from_bytes(parent.into_bytes()), fence)),
                work,
            )
            .await
            .map(|(_, state)| state)
    }
}
