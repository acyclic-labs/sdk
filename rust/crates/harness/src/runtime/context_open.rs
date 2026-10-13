use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) type NativeCustody = acyclic_fs::NativeAllocation;

#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(crate) type NativeCustody = ();

pub(crate) enum ContextSource<'a> {
    Ordinary(std::marker::PhantomData<&'a ()>),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Read {
        work: &'a acyclic_fs::NativeMaterializationWork,
    },
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Dispatch {
        work: &'a acyclic_fs::NativeMaterializationWork,
        fence: &'a crate::scheduler::LeaseFence,
        mailbox: Arc<dyn OwnedTaskMailbox>,
    },
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Reconciliation {
        work: &'a acyclic_fs::NativeMaterializationWork,
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

    /// Reconstructs a pinned task context on the supplied IO.
    pub async fn durable_context(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Arc<TaskContext>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let source = match io {
            Some(io) => ContextSource::Read {
                work: io.original_native_work().ok_or_else(|| {
                    crate::conversation::content_error(
                        Some(io),
                        crate::wire::ErrorCode::Unsupported,
                        "context reconstruction requires original native work",
                    )
                })?,
            },
            None => ContextSource::Ordinary(std::marker::PhantomData),
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let source = {
            if io.is_some() {
                return Err(crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "context reconstruction requires original native work",
                ));
            }
            ContextSource::Ordinary(std::marker::PhantomData)
        };
        self.open_durable_context(task, operation, source).await
    }

    pub(crate) async fn open_durable_context(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        source: ContextSource<'_>,
    ) -> Result<Arc<TaskContext>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_work = match &source {
            ContextSource::Read { work }
            | ContextSource::Dispatch { work, .. }
            | ContextSource::Reconciliation { work, .. } => Some(*work),
            ContextSource::Ordinary(_) => None,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native_work = None::<&NativeWork>;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let original = native_work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let original = None::<crate::executor::PreparationIo>;
        let io = original.as_ref();
        let host = self.state.as_ref().ok_or_else(|| {
            crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "durable task state is not bound",
            )
        })?;
        let issued_purpose = match &source {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            ContextSource::Dispatch { .. } => IssuedContextPurpose::Dispatch,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            ContextSource::Reconciliation { .. } => IssuedContextPurpose::Reconciliation,
            _ => IssuedContextPurpose::Read,
        };
        let owned_mailbox = match source {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            ContextSource::Dispatch { fence, mailbox, .. }
            | ContextSource::Reconciliation { fence, mailbox, .. } => {
                if mailbox.task_binding() != (task, fence) {
                    return Err(crate::conversation::content_error(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "mailbox differs from the original task and fence",
                    ));
                }
                let verification = match issued_purpose {
                    IssuedContextPurpose::Dispatch => {
                        host.verify_dispatch_owner(task, fence, io)?
                    }
                    IssuedContextPurpose::Reconciliation => {
                        host.verify_execution_owner(task, fence, io)?
                    }
                    IssuedContextPurpose::Read => unreachable!("read context has no mailbox"),
                };
                verification.await?;
                Some(mailbox)
            }
            _ => None,
        };
        let (scope, custody) = match native_work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let extra_self = (std::mem::size_of::<TaskContext>()
                    - std::mem::size_of::<RuntimeScope>()) as u64;
                let mut custody = work.claim(extra_self, 0, acyclic_fs::WorkCounters::default())?;
                let mut admitted = host
                    .resume_scope_with_native_work(task, operation, work)?
                    .await?;
                if !admitted.1.work().shares_allocation_context(work) {
                    return Err(crate::conversation::content_error(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "resumed scope differs from its original metadata ledger",
                    ));
                }
                self.validate_native_resumed_scope(&admitted.0, operation, work)?;
                admitted.0.extension_runtime = self.scope.extension_runtime.clone();
                admitted.0.extension_schemas = self.scope.extension_schemas.clone();
                admitted.0.extensions_sealed = true;
                if let Some(mailbox) = &owned_mailbox {
                    mailbox.validate_scope(&admitted.0, io)?;
                }
                custody.absorb(&mut admitted.1)?;
                (admitted.0, Some(custody))
            }
            _ => (
                self.scope
                    .resume_sealed(host.resume_scope(task, operation).await?, operation)?,
                None::<NativeCustody>,
            ),
        };
        let bound = scope.concurrency_bound(self.concurrency);
        let descendants = self
            .live
            .child(bound, native_work)?
            .child(bound, native_work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let model_custody = native_work
            .map(|work| {
                work.claim(
                    (std::mem::size_of::<(AtomicUsize, Option<NativeCustody>)>()
                        + 2 * std::mem::size_of::<usize>()) as u64,
                    1,
                    acyclic_fs::WorkCounters::default(),
                )
            })
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let model_custody = None::<NativeCustody>;
        let context = TaskContext {
            harness: Arc::clone(self),
            task_id: operation,
            durable_task: Some(task),
            descendants,
            scope,
            policy_overrides: Vec::new(),
            owned_interactions: None,
            owned_mailbox,
            output_content: None,
            model_steps: Arc::new((AtomicUsize::new(0), model_custody)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_purpose: native_work.map(|_| issued_purpose),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: custody,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        return context.into_shared();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        {
            let _ = (custody, issued_purpose);
            Ok(Arc::new(context))
        }
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
        if self.native_custody.is_some() || self.model_steps.1.is_some() {
            return crate::executor::PreparationIo::from_native_work(self.native_work()?).map(Some);
        }
        Ok(None)
    }
}
