//! Admission.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) async fn admission_bytes_match(
        &self,
        reference: &FileRef,
        bytes: &[u8],
        retained: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let loaded = self.read_json(reference, work).await;
        let (value, held) = match loaded {
            Err(Error::NotFound(_)) if retained => {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "committed task admission content is missing",
                ));
            }
            other => other?,
        };
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, held);
        drop(value);
        if reference.descriptor().byte_length() != bytes.len() as u64 {
            return Ok(false);
        }
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(bytes.len() as u64)?;
        }
        use sha2::Digest as _;
        Ok(sha2::Sha256::digest(bytes).as_slice() == reference.descriptor().sha256())
    }

    /// Admits a registered root using its original native input/metadata custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn admit_with_native_work<'a>(
        &'a self,
        request: (TaskAdmissionRecord, acyclic_fs::NativeAllocation),
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Admission<TaskId>, acyclic_fs::NativeAllocation)>>
    {
        work.charge(Default::default())?;
        if request.0.parent.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native root admission cannot replace an owned child fence",
            });
        }
        let required = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(request.0.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native root admission ownership overflows",
            })?;
        if !request.1.work().shares_allocation_context(work) || request.1.bytes() < required {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "root admission differs from its original native custody",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            use crate::store::history::HistoryIo;
            let mut io = crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?;
            let (record, custody) = request;
            io.absorb_native_custody(custody)?;
            io.retain_owned(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            let admitted = self
                .admit_record_with_work(record, None, &mut Some(&mut io))
                .await?;
            let custody = io.take_native_custody(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            Ok((admitted, custody))
        })
    }

    pub(super) async fn admit_record_with_work(
        &self,
        admission: TaskAdmissionRecord,
        parent_fence: Option<crate::scheduler::LeaseFence>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Admission<TaskId>> {
        admission.validate(work)?;
        if !admission.resource_proofs.is_empty()
            && parent_fence.is_none()
            && self
                .retained_declaration_with_work(admission.operation_id, work)
                .await?
                .is_none()
        {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "created resource admission requires the current parent execution fence",
            ));
        }
        let operation_id = admission.operation_id;
        let parent = admission.parent;
        let (scope, _) = self.authenticated_scope_with_work(&admission, work).await?;
        self.validate_parent_scope_with_work(parent, &scope, work)
            .await?;
        if parent.is_some() {
            let mut granted = false;
            for grant in scope.grants().iter() {
                if let Some(io) = work.as_deref_mut() {
                    io.check_cancellation()?;
                    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if grant
                    .strip_prefix("task:spawn:")
                    .and_then(|grant| grant.strip_prefix(admission.task.name.as_str()))
                    .and_then(|grant| grant.strip_prefix('@'))
                    == Some(admission.task.version.as_str())
                {
                    granted = true;
                    break;
                }
            }
            if !granted {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parent scope lacks the exact descendant grant",
                ));
            }
        }
        if let Some(extensions) = scope.extensions() {
            extensions.validate(work)?;
        }
        if scope.extensions() != self.root_scope.extensions() {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "task extensions differ from owner bindings",
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            TaskStateProvider::policy_identity_with_native_work(self, native)?;
        }
        let matches = if work.is_some() {
            admission.policy.as_ref() == self.policy_binding.as_ref()
        } else {
            admission.policy == self.policy.as_ref().map(|policy| policy.identity())
        };
        if !matches {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "task admission policy differs from owner",
            ));
        }
        if admission.execution.is_some() {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unsupported,
                "local durable host cannot admit a remote execution route",
            ));
        }
        if self.machines.resolve(&admission.machine, work)?.is_none() {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "task and machine identities disagree",
            ));
        }
        self.tasks.validate_durable_admission(
            &admission.task,
            &admission.machine,
            Some(&admission.input_schema),
            &admission.output_schema,
            Some(&admission.input),
            work,
        )?;
        let bytes = crate::store::history::canonical_bytes_with_work(
            &admission.canonical_payload(),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        if bytes.len() as u64 > scope.limits().file_bytes {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "durable task admission exceeds its file limit",
            ));
        }
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(51)?;
            io.reserve_text(&mut key, 51)?;
            io.copy_bytes(51)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-admission:{operation_id}").expect("writing into String");
        let (state, owner_scope) = if let Some(retained) = self
            .retained_declaration_with_work(operation_id, work)
            .await?
        {
            if !self
                .admission_bytes_match(&retained.state, &bytes, true, work)
                .await?
            {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "task admission changed on retry",
                ));
            }
            (retained.state, retained.owner_scope)
        } else {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let native = work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .cloned();
            let state = self
                .stage_payload_with_work(operation_id, &key, &bytes, work)
                .await?;
            state.descriptor().verify(&bytes, work)?;
            if !self
                .admission_bytes_match(&state, &bytes, false, work)
                .await?
            {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "staged task admission changed before publication",
                ));
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let owner_scope = if native.is_some() {
                self.owner_scope.copy(work)?
            } else {
                self.owner_scope.clone()
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let owner_scope = self.owner_scope.clone();
            (state, owner_scope)
        };
        let byte_capacity = bytes.capacity() as u64;
        drop(bytes);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(byte_capacity)?;
        }
        let parent = if let Some(parent) = parent {
            let mut buffer = [0; 36];
            let slot = uuid::Uuid::from_bytes(operation_id.into_bytes())
                .hyphenated()
                .encode_lower(&mut buffer);
            Some(ParentLink {
                operation_id: OperationId::from_bytes(parent.into_bytes()),
                slot: match work.as_deref_mut() {
                    Some(io) => io.clone_text(slot)?,
                    None => slot.to_owned(),
                },
            })
        } else {
            None
        };
        let authority = Authority {
            kind: self.verifier.audience().kind,
            id: match work.as_deref_mut() {
                Some(io) => io.clone_text(&self.verifier.audience().id)?,
                None => self.verifier.audience().id.clone(),
            },
        };
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(std::mem::size_of::<OperationSpec>() as u64)?;
        }
        let spec = OperationSpec {
            operation_id,
            owner_scope,
            parent,
            owner: DurableOwner::Attached { authority },
            entrypoint: EntrypointRef {
                name: admission.task.name,
                version: admission.task.version,
                digest: admission.task.digest,
                result_schema: admission.output_schema,
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: BTreeMap::new(),
            orchestration: Orchestration::Leaf,
            state,
        };
        let key = IdempotencyKey::new(key)?;
        let outcome = if let Some(fence) = parent_fence {
            self.coordinator
                .lock()
                .await
                .declare_operation(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    key,
                    Some(fence),
                    work,
                )
                .await
        } else {
            self.declare_task_with_work(spec, key, work).await
        };
        match outcome {
            Ok(_) => Ok(Admission::Accepted(TaskId::from_bytes(
                operation_id.into_bytes(),
            ))),
            Err(Error::Indeterminate(_)) => Ok(Admission::Indeterminate { operation_id }),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn declare_task_with_work(
        &self,
        spec: OperationSpec,
        key: IdempotencyKey,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(limits) = self.session_limits.filter(|_| spec.parent.is_none()) {
            coordinator
                .declare_session(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    limits,
                    key,
                    work,
                )
                .await
        } else {
            coordinator
                .declare_operation(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    key,
                    None,
                    work,
                )
                .await
        }
    }

    pub(crate) async fn validate_parent_scope_with_work(
        &self,
        parent: Option<TaskId>,
        scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        let Some(parent) = parent else {
            return Ok(());
        };
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (admitted, held) = self
            .read_retained_admission_counted(
                OperationId::from_bytes(parent.into_bytes()),
                quota.work(),
            )
            .await?;
        quota.adopt(held)?;
        let (ancestor, held) = self
            .authenticated_scope_with_work(&admitted, quota.work())
            .await?;
        quota.adopt(held)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned()
        {
            // Created grants are checked below; the existing scope checks every scalar ceiling.
            ancestor.validate_narrowing_with_native_work(
                &Capabilities::default(),
                &scope.limits(),
                &native,
            )?;
            ancestor.validate_run_limit_attenuation(&scope.run_limits(), quota.work())?;
            for grant in scope.grants().iter() {
                native.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                if !admitted.grants.contains(grant)
                    && !scope
                        .created_grants()
                        .is_some_and(|created| created.contains(grant))
                {
                    return Err(guarded_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Unauthorized,
                        "runtime scope cannot widen ancestor authority",
                    ));
                }
            }
            return Ok(());
        }
        drop(ancestor);
        quota.relinquish(held)?;
        let allowed = Capabilities::new(
            admitted.grants.iter().chain(
                scope
                    .created_grants()
                    .into_iter()
                    .flat_map(Capabilities::iter),
            ),
        );
        RuntimeScope::new(allowed, admitted.limits)?
            .with_run_limits(admitted.run_limits, &mut None)?
            .narrow(scope.grants().clone(), scope.limits())?
            .with_run_limits(scope.run_limits(), &mut None)?;
        Ok(())
    }
}
