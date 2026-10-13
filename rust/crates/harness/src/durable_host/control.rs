//! Cancellation keeps the original reservation and authenticated parent guard.
use super::*;
use crate::store::HistoryIo;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Returns the host clock used for pinned task deadlines.
    pub fn now_unix_millis(&self) -> u64 {
        self.clock.now_unix_millis()
    }

    /// Observes cancellation on the existing IO context.
    pub async fn cancellation_requested(
        &self,
        task: TaskId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        let mut history = io.map(|io| io.history_io());
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        Ok(coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(task.into_bytes()),
                &mut work,
            )?
            .cancellation_requested)
    }

    /// Cancels only after the original admitted deadline expires.
    pub async fn expire_task_deadline(
        &self,
        task: TaskId,
        fence: Option<&crate::scheduler::LeaseFence>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = native
            .as_ref()
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<crate::executor::PreparationIo> = None;
        if work.is_some() && io.is_none() {
            return Err(Error::Unsupported(
                "deadline observation requires original IO".into(),
            ));
        }
        let admission = self.admission(operation, io.as_ref()).await?;
        let deadline = admission
            .run_limits
            .deadline_epoch_ms
            .ok_or_else(|| Error::Invalid("task has no admitted deadline".into()))?;
        drop(admission);
        if self.now_unix_millis() < deadline {
            return Err(Error::Conflict("task deadline has not elapsed".into()));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let state = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation,
            work,
        )?;
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| !matches!(outcome, Outcome::Indeterminate { .. }))
        {
            return Ok(());
        }
        if let Some(fence) = fence {
            if state.reservation.as_ref().is_none_or(|lease| {
                lease.id != fence.reservation_id || lease.placement != fence.placement
            }) {
                return Err(Error::Conflict(
                    "deadline observer does not own current reservation".into(),
                ));
            }
        }
        if state.cancellation_requested {
            return Ok(());
        }
        let text =
            crate::executor::preparation_format(format_args!("task-cancel:{operation}"), work)?;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, text.capacity() as u64);
        let key = IdempotencyKey::new(text)?;
        let (_, state) = coordinator
            .cancel_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation,
                key,
                true,
                None,
                quota.work(),
            )
            .await?;
        let retained = [
            state.spec.logical_owned_bytes()?,
            state
                .reservation
                .as_ref()
                .map(|value| value.logical_owned_bytes())
                .transpose()?
                .unwrap_or(0),
            state
                .checkpoint
                .as_ref()
                .map(|value| value.logical_owned_bytes())
                .transpose()?
                .unwrap_or(0),
            state
                .workflow
                .as_ref()
                .map(|value| value.logical_owned_bytes())
                .transpose()?
                .unwrap_or(0),
            state
                .outcome
                .as_ref()
                .map(|value| value.logical_owned_bytes())
                .transpose()?
                .unwrap_or(0),
        ]
        .into_iter()
        .try_fold(0u64, |sum, bytes| {
            sum.checked_add(bytes)
                .ok_or_else(|| Error::Invalid("cancelled state custody overflows".into()))
        })?;
        quota.adopt(retained)?;
        drop(state);
        Ok(())
    }

    /// Cancels the direct child's subtree under its original parent lease.
    #[cfg(feature = "filesystem")]
    pub async fn cancel_child_owned(
        &self,
        parent: TaskId,
        fence: &crate::scheduler::LeaseFence,
        child: TaskId,
        operation: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<crate::scheduler::OperationState> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned();
        #[cfg(not(target_arch = "wasm32"))]
        let io = native
            .as_ref()
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<crate::executor::PreparationIo> = None;
        if work.is_some() && io.is_none() {
            return Err(Error::Unsupported(
                "child cancellation requires original IO".into(),
            ));
        }
        let admitted = self
            .admission(OperationId::from_bytes(parent.into_bytes()), io.as_ref())
            .await?;
        let target = self
            .admission(OperationId::from_bytes(child.into_bytes()), io.as_ref())
            .await?;
        if !admitted.grants.contains(capability::OPERATION_CANCEL) || target.parent != Some(parent)
        {
            return Err(Error::Unauthorized(
                "cancellation requires an authorized direct child".into(),
            ));
        }
        drop((admitted, target));
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let parent_op = OperationId::from_bytes(parent.into_bytes());
        crate::scheduler::require_execution_owner(
            coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                parent_op,
                work,
            )?,
            fence,
            false,
            work,
        )?;
        let text = crate::executor::preparation_format(
            format_args!("task-child-cancel:{parent}:{operation}"),
            work,
        )?;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, text.capacity() as u64);
        let (_, state) = coordinator
            .cancel_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(child.into_bytes()),
                IdempotencyKey::new(text)?,
                true,
                Some((parent_op, fence)),
                quota.work(),
            )
            .await?;
        Ok(state)
    }
}
