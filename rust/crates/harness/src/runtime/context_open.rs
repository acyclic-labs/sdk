use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) type NativeCustody = acyclic_fs::NativeAllocation;

#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(crate) type NativeCustody = ();

pub(crate) enum ContextSource<'a> {
    Ordinary(std::marker::PhantomData<&'a ()>),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native {
        work: &'a acyclic_fs::NativeMaterializationWork,
        purpose: NativeContextPurpose<'a>,
    },
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) enum NativeContextPurpose<'a> {
    Read,
    Dispatch {
        fence: &'a crate::scheduler::LeaseFence,
        mailbox: Arc<dyn OwnedTaskMailbox>,
    },
    Reconciliation {
        fence: &'a crate::scheduler::LeaseFence,
        mailbox: Arc<dyn OwnedTaskMailbox>,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum IssuedContextPurpose {
    Read,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Dispatch,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Reconciliation,
}

impl AgentHarness {
    pub(super) async fn admit_scoped<I, O>(
        self: &Arc<Self>,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        parent: Option<TaskId>,
        scope: RuntimeScope,
    ) -> Result<Admission<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        self.assert_durable_policy_bindings(None)?;
        let registered = self.tasks.get_version::<I, O>(
            &definition.identity.name,
            &definition.identity.version,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            None,
        )?;
        if !Arc::ptr_eq(&registered, definition) {
            return Err(Error::Conflict(
                "task definition is not the registered version".into(),
            ));
        }
        if parent.is_some() {
            require_task_spawn(&scope, definition)?;
        }
        let TaskImplementation::Resumable(machine) = &definition.implementation else {
            return Ok(Admission::Rejected {
                reason: "live tasks are local-only".into(),
            });
        };
        let Some(host) = &self.state else {
            return Ok(Admission::Rejected {
                reason: "durable task state is not bound".into(),
            });
        };
        let spawner = self
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task spawner is not bound".into()))?;
        let value =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        crate::executor::validate_json_schema_value(
            &definition.input_schema,
            &value,
            "task input",
            &mut None,
        )?;
        let mut expected = TaskAdmissionRecord::from_parts(
            operation_id,
            &definition.identity.name,
            &definition.identity.version,
            value,
            definition.input_schema.clone(),
            definition.output_schema.clone(),
            &definition.requirements,
            &machine.identity().digest,
            parent,
            scope.grants().clone(),
            scope.limits(),
            scope.run_limits(),
            self.policy_identity.clone(),
            scope.extensions().cloned(),
            None,
        )?
        .with_resource_proofs(scope.resource_proofs().to_vec())?;
        if let Some(admission) = self
            .reconcile_scoped_admission(operation_id, definition, &expected, &scope, host, spawner)
            .await?
        {
            return Ok(admission);
        }
        if let Some(execution) = &self.execution {
            let placement = execution.qualify(&expected).await?;
            placement.validate(&mut None)?;
            if placement.provider != execution.identity() {
                return Err(Error::Conflict(
                    "execution qualifier returned another provider".into(),
                ));
            }
            expected.execution = Some(placement);
        }
        let extension_leases = self.retain_task_extensions(definition, &scope)?;
        let admitted = if expected.resource_proofs.is_empty() {
            spawner.admit(expected.clone()).await?
        } else {
            let parent = parent.ok_or_else(|| {
                Error::Unauthorized("created child admission has no parent".into())
            })?;
            let fence = scope.current_creation_parent_fence(parent).await?;
            spawner.admit_owned(expected.clone(), fence).await?
        };
        Ok(match admitted {
            Admission::Accepted(task_id) => {
                self.verify_spawner_admission(task_id, &expected).await?;
                Admission::Accepted(RuntimeTask::Durable {
                    task_id,
                    host: Arc::clone(host),
                    output_schema: definition.output_schema.clone(),
                    extensions: extension_leases,
                })
            }
            Admission::Rejected { reason } => Admission::Rejected { reason },
            Admission::Indeterminate { operation_id } => Admission::Indeterminate { operation_id },
        })
    }

    /// Reconstructs a pinned task context after the host resumes its state machine.
    pub async fn durable_context(
        self: &Arc<Self>,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> Result<TaskContext> {
        self.open_durable_context(
            task_id,
            operation_id,
            ContextSource::Ordinary(std::marker::PhantomData),
        )
        .await
        .map(|(context, _)| context)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Restores the admitted context on the original work.
    pub async fn durable_context_with_native_work(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskContext, acyclic_fs::NativeAllocation)> {
        let (context, custody) = self
            .open_durable_context(
                task,
                operation,
                ContextSource::Native {
                    work,
                    purpose: NativeContextPurpose::Read,
                },
            )
            .await?;
        Ok((
            context,
            custody.expect("native context source transfers its original custody"),
        ))
    }

    pub(crate) async fn open_durable_context(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        source: ContextSource<'_>,
    ) -> Result<(TaskContext, Option<NativeCustody>)> {
        let host = match &self.state {
            Some(host) => host,
            None => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if matches!(&source, ContextSource::Native { .. }) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "durable task state is not bound",
                    });
                }
                return Err(Error::Unsupported("durable task state is not bound".into()));
            }
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_work = match &source {
            ContextSource::Native { work, .. } => Some(*work),
            ContextSource::Ordinary(_) => None,
        };
        let (scope, descendants, model_steps, owned_mailbox, custody, issued_purpose) = match source
        {
            ContextSource::Ordinary(_) => {
                let admitted = host.resume_scope(task, operation).await?;
                let scope = self.scope.resume_sealed(admitted, operation)?;
                let group = self
                    .live
                    .child(scope.concurrency_bound(self.concurrency), None)?;
                let descendants = group.child(scope.concurrency_bound(self.concurrency), None)?;
                (
                    scope,
                    descendants,
                    Arc::new((AtomicUsize::new(0), None)),
                    None,
                    None,
                    IssuedContextPurpose::Read,
                )
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            ContextSource::Native { work, purpose } => {
                work.charge(acyclic_fs::WorkCounters::default())
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: None,
                    })?;
                let issued_purpose = match &purpose {
                    NativeContextPurpose::Read => IssuedContextPurpose::Read,
                    NativeContextPurpose::Dispatch { .. } => IssuedContextPurpose::Dispatch,
                    NativeContextPurpose::Reconciliation { .. } => {
                        IssuedContextPurpose::Reconciliation
                    }
                };
                let mailbox = match purpose {
                    NativeContextPurpose::Read => None,
                    NativeContextPurpose::Dispatch { fence, mailbox } => {
                        if !mailbox.matches_task_binding(task, fence) {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unauthorized,
                                reason: "dispatch mailbox differs from the original task and fence",
                            });
                        }
                        host.verify_dispatch_owner_with_native_work(task, fence, work)?
                            .await?;
                        Some(mailbox)
                    }
                    NativeContextPurpose::Reconciliation { fence, mailbox } => {
                        if !mailbox.matches_task_binding(task, fence) {
                            return Err(Error::NativeBoundary {
                                code: crate::wire::ErrorCode::Unauthorized,
                                reason: "reconciliation mailbox differs from the original task and fence",
                            });
                        }
                        host.verify_execution_owner_with_native_work(task, fence, work)?
                            .await?;
                        Some(mailbox)
                    }
                };
                let extra_self = (std::mem::size_of::<TaskContext>()
                    - std::mem::size_of::<RuntimeScope>()) as u64;
                let mut custody = work
                    .claim(extra_self, 0, acyclic_fs::WorkCounters::default())
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: None,
                    })?;
                let mut admitted = host
                    .resume_scope_with_native_work(task, operation, work)?
                    .await?;
                if !admitted.1.work().shares_allocation_context(work) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "resumed scope differs from its original metadata ledger",
                    });
                }
                self.validate_native_resumed_scope(&admitted.0, operation, work)?;
                admitted.0.extension_runtime = self.scope.extension_runtime.clone();
                admitted.0.extension_schemas = self.scope.extension_schemas.clone();
                admitted.0.extensions_sealed = true;
                if let Some(mailbox) = &mailbox {
                    mailbox.validate_scope_with_native_work(&admitted.0, work)?;
                }
                custody
                    .absorb(&mut admitted.1)
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: None,
                    })?;
                let bound = admitted.0.concurrency_bound(self.concurrency);
                let group = self.live.child(bound, Some(work))?;
                let descendants = group.child(bound, Some(work))?;
                let model_custody = work
                    .claim(
                        std::mem::size_of::<(AtomicUsize, Option<NativeCustody>)>() as u64,
                        u64::from(
                            (std::mem::size_of::<(AtomicUsize, Option<NativeCustody>)>() as u64)
                                != 0,
                        ),
                        acyclic_fs::WorkCounters::default(),
                    )
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: None,
                    })?;
                let model_steps = Arc::new((AtomicUsize::new(0), Some(model_custody)));
                (
                    admitted.0,
                    descendants,
                    model_steps,
                    mailbox,
                    Some(custody),
                    issued_purpose,
                )
            }
        };
        let mut context = TaskContext {
            harness: Arc::clone(self),
            task_id: operation,
            durable_task: Some(task),
            descendants,
            scope,
            policy_overrides: Vec::new(),
            owned_interactions: None,
            owned_mailbox,
            output_content: None,
            model_steps,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let (Some(work), Some(custody)) = (native_work, custody.as_ref()) {
            context.mark_native_metadata_source(work, custody)?;
            context
                .native_metadata_source
                .as_mut()
                .expect("issued metadata source")
                .purpose = issued_purpose;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = issued_purpose;
        Ok((context, custody))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn validate_native_resumed_scope(
        &self,
        admitted: &RuntimeScope,
        operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        (&admitted.limits).validate(&mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )))?;
        (&admitted.run_limits).validate(&mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )))?;
        let created = admitted.created_access.as_ref();
        if let Some(access) = created {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: admitted.resource_proofs.len() as u64,
                ..Default::default()
            })
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
            if access.operation != operation || access.proofs != admitted.resource_proofs {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "created resource seal belongs to another task",
                });
            }
        } else if !admitted.resource_proofs.is_empty() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumed resource scope has no original creation seal",
            });
        }
        for grant in admitted.grants.iter() {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
            if !self.scope.grants.contains(grant)
                && !created.is_some_and(|access| access.grants.contains(grant))
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "resumed scope widens original admitted authority",
                });
            }
        }
        if !self
            .scope
            .allows_authority(&self.scope.grants, &admitted.limits)
            || admitted.extensions != self.scope.extensions
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumed scope changes original authority bounds or extensions",
            });
        }
        if !self.scope.run_limits.allows(&admitted.run_limits) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumed scope widens original task run limits",
            });
        }
        Ok(())
    }
}

impl TaskContext {
    pub(super) fn preparation_io(&self) -> Result<Option<crate::executor::PreparationIo>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_metadata_source.is_some()
            || self.native_custody.is_some()
            || self.model_steps.1.is_some()
        {
            return crate::executor::PreparationIo::from_native_work(self.native_work()?).map(Some);
        }
        Ok(None)
    }
}
