use super::*;
use crate::conversation::VolumeOperation;
use crate::runtime::{Bindings, ToolContext};

#[tokio::test]
async fn host_invocations_share_canonical_admission_approval_and_receipts() -> Result<()> {
    for decision in [
        ToolPolicyDecision::Allow,
        ToolPolicyDecision::Deny {
            reason: "write is denied".into(),
        },
        ToolPolicyDecision::RequireApproval {
            prompt: "approve this write".into(),
        },
    ] {
        let storage = crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536)
            .await?
            .with_approval_policy(crate::approvals::ApprovalPolicy {
                approvers: vec![crate::approvals::Approver::Human {
                    principal: "human".into(),
                }],
            })?;
        let journal = storage.journal();
        let content = storage.content_bindings();
        let scope = RuntimeScope::new(
            Capabilities::new([
                content
                    .volume
                    .capability(VolumeOperation::Read, &mut None)?,
                content
                    .volume
                    .capability(VolumeOperation::Write, &mut None)?,
                "tool:call:write".into(),
                capability::INTERACTION_ROUTE.into(),
            ]),
            Limits::default(),
        )?;
        let parent = OperationId::new();
        let mut summaries = content.clone();
        summaries.writer = None;
        let task = Bindings {
            scope,
            artifacts: Some(summaries),
            content: Some(content.clone()),
            ..Bindings::local()
        }
        .build()?
        .agent_context(&Capabilities::default(), content)?
        .with_execution_journal(journal.clone(), parent)?;
        let model = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
        let policy = Arc::new(crate::test_support::FixedToolPolicy::new(decision.clone())?);
        let mut tools = ToolRegistry::default();
        tools.register((crate::tool::files::write()?, None), &mut None, None)?;
        let unissued = StockExecutor::new(
            Model::new("fixture".into(), "unused".into(), "1".into())?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_compaction_policy(crate::context::CompactionPolicy::default())
        .with_tool_authority(task.scope().clone(), policy.clone())?;
        let executor = unissued.clone().with_task_context(&task, parent)?;
        // Tool publication has an original writer; model compaction deliberately
        // does not. A direct call must not inherit model-summary admission.
        assert!(
            StockExecution::ordinary(&executor)
                .verify_task_context_binding(parent)
                .is_err()
        );
        let binding = executor
            .tool_binding("write")?
            .ok_or(Error::NotFound("write".into()))?;
        let capability = binding;
        let binding = Arc::new(crate::tools::native::alias(
            capability.definition.clone(),
            capability.clone(),
            json!({"path":"receipt.txt","content":"accepted"}),
            None,
        )?);
        let call = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "write-receipt".into(),
            name: "write".into(),
            arguments: json!({"path":"receipt.txt","content":"accepted"}),
        };
        let reader = ToolContext::new(Arc::new(task), parent, "verify")?;
        let mut hooks = Vec::new();
        if matches!(decision, ToolPolicyDecision::Allow) {
            let mut changed_name = call.clone();
            changed_name.name.push_str("-changed");
            for (executor, operation, attempt, code) in [
                (
                    &unissued,
                    parent,
                    call.clone(),
                    crate::wire::ErrorCode::Unauthorized,
                ),
                (
                    &executor,
                    OperationId::new(),
                    call.clone(),
                    crate::wire::ErrorCode::Unauthorized,
                ),
                (
                    &executor,
                    parent,
                    changed_name,
                    crate::wire::ErrorCode::Invalid,
                ),
            ] {
                assert!(matches!(
                    executor.settle_invocation(
                        journal.as_ref(),
                        (operation, 0),
                        (attempt, binding.clone()),
                        &Value::Null,
                        &mut hooks,
                        None,
                    )?.await,
                    Err(error) if error.code() == code
                ));
            }
            for cancelled in [false, true] {
                let cancellation = acyclic_fs::CancellationToken::new();
                let budget = if cancelled {
                    acyclic_fs_work::performance::BOUNDARY_BUDGET
                } else {
                    acyclic_fs_work::performance::WorkBudget {
                        peak_allocation_bytes:
                            acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
                        ..acyclic_fs_work::performance::WorkBudget::UNBOUNDED
                    }
                };
                let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let io = PreparationIo::from_native_work(&work)?;
                if cancelled {
                    cancellation.cancel();
                }
                let refused = match executor.settle_invocation(
                    journal.as_ref(),
                    (parent, 0),
                    (call.clone(), binding.clone()),
                    &Value::Null,
                    &mut hooks,
                    Some(&io),
                ) {
                    Ok(future) => future.await,
                    Err(error) => Err(error),
                };
                if cancelled {
                    assert!(
                        matches!(refused, Err(error) if error.code() == crate::wire::ErrorCode::Cancelled)
                    );
                } else {
                    assert!(matches!(refused, Err(Error::NativeWork { .. })));
                }
            }
            assert!(journal.replay(parent, 0, 64, None)?.await?.is_empty());
            assert!(
                journal
                    .staged_ref(parent, &format!("tool:0:{}:selection", call.call_id), None)?
                    .await?
                    .is_none()
            );
            assert!(
                reader
                    .read_tool_path("receipt.txt", None, None)
                    .await
                    .is_err()
            );
            assert!(hooks.is_empty());
            assert!(model.requests().is_empty());
        }
        let owner = journal
            .acquire_effect(parent, call.operation_id, None)?
            .into_owner(None)?;
        assert!(matches!(
            executor
                .settle_invocation(
                    journal.as_ref(),
                    (parent, 0),
                    (call.clone(), binding.clone()),
                    &Value::Null,
                    &mut hooks,
                    None,
                )?
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(journal.replay(parent, 0, 64, None)?.await?.is_empty());
        assert!(
            reader
                .read_tool_path("receipt.txt", None, None)
                .await
                .is_err()
        );
        owner.release();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = if matches!(decision, ToolPolicyDecision::Allow) {
            Some(
                acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs_work::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?,
            )
        } else {
            None
        };
        let io = work
            .as_ref()
            .map(PreparationIo::from_native_work)
            .transpose()?;
        let observer = io.as_ref();
        let mut outcome = executor
            .settle_invocation(
                journal.as_ref(),
                (parent, 0),
                (call.clone(), binding.clone()),
                &Value::Null,
                &mut hooks,
                observer,
            )?
            .await?;
        if matches!(decision, ToolPolicyDecision::RequireApproval { .. }) {
            assert!(matches!(outcome, ExecutionProgress::Pending(id) if id == call.operation_id));
            assert!(
                reader
                    .read_tool_path("receipt.txt", None, None)
                    .await
                    .is_err()
            );
            let records = journal.replay(parent, 0, 64, None)?.await?;
            assert!(
                !records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }))
            );
            // Pending admission owns the original selection even before a
            // ToolStarted exists. A changed program, input or event cannot
            // replace it or create another approval for this call identity.
            let mut replacement = binding.as_ref().clone();
            replacement.definition.revision.push_str("-changed");
            let mut changed_input = call.clone();
            changed_input.arguments["content"] = "replacement".into();
            let changed_metadata = json!({"replacement": true});
            let mut changed_mapping = crate::tools::native::alias(
                capability.definition.clone(),
                capability.clone(),
                json!({"path":"receipt.txt","content":"replacement"}),
                None,
            )?;
            // A public declaration alone cannot identify the executable mapping.
            changed_mapping.definition = binding.definition.clone();
            for (attempt, selected, metadata) in [
                (call.clone(), Arc::new(replacement), &Value::Null),
                (changed_input, binding.clone(), &Value::Null),
                (call.clone(), binding.clone(), &changed_metadata),
                (call.clone(), Arc::new(changed_mapping), &Value::Null),
            ] {
                assert!(matches!(
                    executor
                        .settle_invocation(
                            journal.as_ref(),
                            (parent, 0),
                            (attempt, selected),
                            metadata,
                            &mut hooks,
                            None,
                        )?
                        .await,
                    Err(Error::Conflict(_))
                ));
            }
            assert_eq!(
                journal.replay(parent, 0, 64, None)?.await?.len(),
                records.len()
            );
            assert!(
                reader
                    .read_tool_path("receipt.txt", None, None)
                    .await
                    .is_err()
            );
            // The executor judges the canonical capability, not the alias's
            // public declaration, so the request identity must use the same
            // adapted invocation and capability definition.
            let canonical = binding.executor.canonical(&call, None)?;
            let ((approval, _), _hold) = tool_approval_request(
                TaskId::from_bytes(parent.into_bytes()),
                &policy.identity(),
                &capability.definition,
                canonical.as_deref().unwrap_or(&call),
                ("approve this write".into(), None),
                None,
                (crate::interaction::ApprovalRoute::Policy, None),
            )?;
            let approval = crate::interaction::scoped_id(parent.into_bytes(), approval, &mut None)?;
            storage
                .decide_approval(
                    approval,
                    crate::approvals::ApprovalResponder::Human("human"),
                    crate::interaction::ApprovalVerdict::Approve,
                    None,
                )
                .await?;
            outcome = executor
                .settle_invocation(
                    journal.as_ref(),
                    (parent, 0),
                    (call.clone(), binding.clone()),
                    &Value::Null,
                    &mut hooks,
                    None,
                )?
                .await?;
        }
        let ExecutionProgress::Ready(Some(settled)) = outcome else {
            return Err(Error::Invalid("host call did not settle".into()));
        };
        let denied = matches!(decision, ToolPolicyDecision::Deny { .. });
        assert_eq!(
            matches!(
                settled.content(),
                crate::model::ToolResultContent::Error { .. }
            ),
            denied
        );
        assert_eq!(settled.invocation(), &call);
        let published = reader.read_tool_path("receipt.txt", None, None).await;
        assert_eq!(published.is_ok(), !denied);
        if let Ok((_, bytes, _)) = &published {
            assert_eq!(bytes.as_ref(), b"accepted");
        }
        let records = journal.replay(parent, 0, 64, None)?.await?;
        assert!(records.len() < 64);
        if denied {
            assert!(
                !records
                    .iter()
                    .any(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }))
            );
        }
        // A funded replay uses its original ledger; explicit unbounded replay
        // must recognize the same canonical selection and terminal receipt.
        for replay_io in observer.into_iter().map(Some).chain(std::iter::once(None)) {
            let replay = executor
                .settle_invocation(
                    journal.as_ref(),
                    (parent, 0),
                    (call.clone(), binding.clone()),
                    &Value::Null,
                    &mut hooks,
                    replay_io,
                )?
                .await?;
            let ExecutionProgress::Ready(Some(replayed)) = replay else {
                return Err(Error::Invalid("host call lost its receipt".into()));
            };
            assert_eq!(replayed.content(), settled.content());
            assert_eq!(replayed.invocation(), &call);
            assert_eq!(
                journal.replay(parent, 0, 64, None)?.await?.len(),
                records.len()
            );
        }
        if let Ok((file, _, _)) = published {
            assert_eq!(
                reader.read_tool_path("receipt.txt", None, None).await?.0,
                file
            );
        }
        assert!(model.requests().is_empty());
    }
    Ok(())
}

struct StaleReplay<'a>(&'a Journal, AtomicUsize);

impl ExecutionJournal for StaleReplay<'_> {
    fn supports_deferred_durability(&self) -> bool {
        self.0.supports_deferred_durability()
    }

    fn durability_barrier<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.0.durability_barrier(io)
    }

    fn acquire_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<crate::executor::ExecutionAttempt<'a>> {
        self.0.acquire_execution(operation, io)
    }

    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        if self
            .1
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |stale| {
                stale.checked_sub(1)
            })
            .is_ok()
        {
            return crate::conversation::BoundedContentFuture::new(io, || async { Ok(Vec::new()) });
        }
        self.0.replay(operation, after, maximum, io)
    }

    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.0.append(operation, key, event, io)
    }

    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.0.append_if_tail(operation, tail, key, event, io)
    }

    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        self.0.stage(operation, key, bytes, media, io)
    }

    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::Custody>> {
        self.0.load(reference, io)
    }

    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        self.0.supports_native_execution(work)
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.0.open_interaction(id, interaction, io)
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        self.0.interaction_outcome(id, io)
    }
}

/// Two host calls may both observe an empty parent journal. They must append
/// the same parent start, not race two invocation-specific identities.
#[tokio::test]
async fn concurrent_host_starts_share_one_parent_binding() -> Result<()> {
    let journal = StaleReplay(&Journal::default(), AtomicUsize::new(2));
    let executor = uncompacted_executor(
        Model::new("test".into(), "host-start".into(), "1".into())?,
        Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let execution = StockExecution::ordinary(&executor);
    let parent = OperationId::new();
    let first = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "first".into(),
        name: "write".into(),
        arguments: json!({"path":"first.txt"}),
    };
    let second = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "second".into(),
        name: "write".into(),
        arguments: json!({"path":"second.txt"}),
    };
    execution
        .ensure_host_started(&journal, parent, &first)
        .await?;
    execution
        .ensure_host_started(&journal, parent, &second)
        .await?;
    let records = journal.0.replay(parent, 0, 64, None)?.await?;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.event, ExecutionEvent::Started { .. }))
            .count(),
        1
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn host_start_replay_releases_its_records() -> Result<()> {
    let storage =
        crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let parent = OperationId::new();
    journal
        .append(
            parent,
            "execution:started",
            &ExecutionEvent::Started {
                request_digest: [7; 32],
            },
            None,
        )?
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs_work::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let before = io.owned_bytes()?;
    let executor = uncompacted_executor(
        Model::new("test".into(), "host-start".into(), "1".into())?,
        Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let execution = StockExecution {
        io: Some(&io),
        ..StockExecution::ordinary(&executor)
    };
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "first".into(),
        name: "write".into(),
        arguments: json!({"path":"first.txt"}),
    };
    execution
        .ensure_host_started(journal.as_ref(), parent, &invocation)
        .await?;
    assert_eq!(
        io.owned_bytes()?,
        before,
        "the replay records kept their preparation charges"
    );
    Ok(())
}
