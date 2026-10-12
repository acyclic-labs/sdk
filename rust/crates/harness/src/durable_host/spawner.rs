//! Spawner.
use super::*;

impl<P: StreamProvider> crate::runtime::TaskSpawner for CoordinatorTaskHost<P> {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        TaskStateProvider::policy_identity(self)
    }
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        TaskStateProvider::execution_identity(self)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        TaskStateProvider::policy_identity_with_native_work(self, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execution_identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        TaskStateProvider::execution_identity_with_native_work(self, work)
    }
    fn admit_owned<'a>(
        &'a self,
        request: TaskAdmissionRecord,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move {
            let parent = request
                .parent
                .ok_or_else(|| Error::Invalid("owned child admission has no parent".into()))?;
            self.verify_owner(parent, &fence, false, &mut None).await?;
            self.admit_record_with_work(request, Some(fence), &mut None)
                .await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_owned_with_native_work<'a>(
        &'a self,
        request: (TaskAdmissionRecord, acyclic_fs::NativeAllocation),
        fence: (crate::scheduler::LeaseFence, acyclic_fs::NativeAllocation),
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Admission<TaskId>, acyclic_fs::NativeAllocation)>>
    {
        work.charge(acyclic_fs::WorkCounters::default())?;
        let record_bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(request.0.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native child admission ownership overflows",
            })?;
        let fence_bytes = (std::mem::size_of::<crate::scheduler::LeaseFence>() as u64)
            .checked_add(fence.0.reservation_id.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(fence.0.placement.capacity() as u64))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native child fence ownership overflows",
            })?;
        if !request.1.work().shares_allocation_context(work)
            || request.1.bytes() < record_bytes
            || !fence.1.work().shares_allocation_context(work)
            || fence.1.bytes() < fence_bytes
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "child admission or fence differs from its original custody",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            use crate::store::history::HistoryIo;
            let mut io = crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?;
            let (record, record_guard) = request;
            let (fence, fence_guard) = fence;
            io.absorb_native_custody(record_guard)?;
            io.absorb_native_custody(fence_guard)?;
            io.retain_owned(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            let parent = record.parent.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "owned child admission has no original parent",
            })?;
            self.verify_owner(parent, &fence, false, &mut Some(&mut io))
                .await?;
            let admitted = self
                .admit_record_with_work(record, Some(fence), &mut Some(&mut io))
                .await?;
            let custody = io.take_native_custody(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            Ok((admitted, custody))
        })
    }

    fn supports_batch_cancellation(&self) -> bool {
        true
    }

    fn reconcile_admission<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        Box::pin(async move {
            match self.admission_with_work(operation_id, &mut None).await {
                Ok(admission) => Ok(Some((admission.task_id(), admission))),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_admission_with_native_work<'a>(
        &'a self,
        operation: OperationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                Option<(TaskId, TaskAdmissionRecord)>,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            use crate::store::history::HistoryIo;
            let mut io = crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?;
            let extra = std::mem::size_of::<Option<(TaskId, TaskAdmissionRecord)>>()
                - std::mem::size_of::<TaskAdmissionRecord>();
            io.retain_owned(extra as u64)?;
            let record = match self
                .read_retained_admission_counted(operation, &mut Some(&mut io))
                .await
                .map(|(record, _)| record)
            {
                Ok(record) => record,
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::NotFound,
                    ..
                }) => {
                    io.retain_owned(std::mem::size_of::<TaskAdmissionRecord>() as u64)?;
                    let custody = io.take_native_custody(std::mem::size_of::<
                        Option<(TaskId, TaskAdmissionRecord)>,
                    >() as u64)?;
                    return Ok((None, custody));
                }
                Err(error) => return Err(error),
            };
            let (scope, _) = self
                .authenticated_scope_with_work(&record, &mut Some(&mut io))
                .await?;
            drop(scope);
            let owned = (std::mem::size_of::<Option<(TaskId, TaskAdmissionRecord)>>() as u64)
                .checked_add(record.logical_owned_bytes()?)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "reconciled admission ownership overflows",
                })?;
            let custody = io.take_native_custody(owned)?;
            Ok((
                Some((TaskId::from_bytes(operation.into_bytes()), record)),
                custody,
            ))
        })
    }

    fn reconcile_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        Box::pin(async move {
            self.validate_batch(&request).await?;
            let Some(retained) = self.retained_batch(request.batch_id).await? else {
                return Ok(None);
            };
            if retained != request.canonical_value() {
                return Err(Error::Conflict(
                    "batch identity belongs to another request".into(),
                ));
            }
            let cancelled = self.retained_batch_cancellation(request.batch_id).await?;
            if cancelled
                .as_ref()
                .is_some_and(|declaration| declaration.group_id != request.group_id)
            {
                return Err(Error::Conflict(
                    "batch cancellation belongs to another group".into(),
                ));
            }
            let mut entries = Vec::with_capacity(request.inputs.len());
            for index in 0..request.inputs.len() {
                let operation_id = request.operation_id(index);
                let expected = request.member_admission(index)?;
                match self.admission_with_work(operation_id, &mut None).await {
                    Ok(admission) if admission == expected => entries.push(Admission::Accepted(
                        TaskId::from_bytes(operation_id.into_bytes()),
                    )),
                    Ok(_) => {
                        return Err(Error::Conflict(
                            "batch member belongs to another admission".into(),
                        ));
                    }
                    Err(Error::NotFound(_)) if cancelled.is_some() => {
                        entries.push(Admission::Rejected {
                            reason: "batch cancellation preceded member admission".into(),
                        });
                    }
                    Err(Error::NotFound(_)) => {
                        entries.push(Admission::Indeterminate { operation_id });
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(Some(entries))
        })
    }

    fn load_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        Box::pin(async move {
            let Some(value) = self.retained_batch(batch_id).await? else {
                return Ok(None);
            };
            let request = DurableBatchRequest::from_canonical_value(value)?;
            if request.batch_id != batch_id {
                return Err(Error::Conflict(
                    "batch manifest has another identity".into(),
                ));
            }
            self.validate_batch(&request).await?;
            Ok(Some(request))
        })
    }

    fn admit_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        Box::pin(async move {
            let parent_fence = if request.scope.resource_proofs().is_empty() {
                None
            } else {
                let parent = request
                    .parent
                    .ok_or_else(|| Error::Unauthorized("created batch has no parent".into()))?;
                Some(request.scope.current_creation_parent_fence(parent).await?)
            };
            self.validate_batch(&request).await?;
            self.validate_parent_scope_with_work(request.parent, &request.scope, &mut None)
                .await?;
            self.commit_batch(&request).await?;
            let observed = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| Error::Storage("committed batch disappeared".into()))?;
            let mut entries = Vec::with_capacity(observed.len());
            for (index, admission) in observed.into_iter().enumerate() {
                match admission {
                    Admission::Indeterminate { operation_id } => {
                        let member = request.member_admission(index)?;
                        if member.operation_id != operation_id {
                            return Err(Error::Conflict("batch member operation changed".into()));
                        }
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                        {
                            entries.push(Admission::Rejected {
                                reason: "batch cancellation preceded member admission".into(),
                            });
                            continue;
                        }
                        let admitted = self
                            .admit_record_with_work(member, parent_fence.clone(), &mut None)
                            .await?;
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                            && let Admission::Accepted(task_id) = &admitted
                        {
                            self.cancel(*task_id, None).await?;
                            entries.push(admitted);
                            continue;
                        }
                        entries.push(admitted);
                    }
                    other => entries.push(other),
                }
            }
            Ok(entries)
        })
    }

    fn cancel_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        Box::pin(async move {
            let Some(request) = self.load_batch(batch_id).await? else {
                return Ok(None);
            };
            self.declare_batch_cancellation(&request).await?;
            let admissions = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| {
                    Error::Storage("retained batch disappeared during cancellation".into())
                })?;
            let mut entries = Vec::with_capacity(admissions.len());
            for (index, admission) in admissions.into_iter().enumerate() {
                let status = match admission {
                    Admission::Accepted(task_id) => match self.cancel(task_id, None).await {
                        Ok(()) => BatchCancellationStatus::Requested,
                        Err(Error::Indeterminate(operation_id)) => {
                            BatchCancellationStatus::Indeterminate { operation_id }
                        }
                        Err(error) => BatchCancellationStatus::Unresolved {
                            message: error.to_string(),
                        },
                    },
                    Admission::Rejected { reason } => {
                        BatchCancellationStatus::NotAdmitted { reason }
                    }
                    Admission::Indeterminate { operation_id } => {
                        BatchCancellationStatus::Indeterminate { operation_id }
                    }
                };
                entries.push((InputKey { batch_id, index }, status));
            }
            Ok(Some(BatchCancellationReport {
                group_id: request.group_id,
                batch_id,
                entries,
            }))
        })
    }

    fn admit<'a>(
        &'a self,
        admission: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move {
            self.admit_record_with_work(admission, None, &mut None)
                .await
        })
    }
}
