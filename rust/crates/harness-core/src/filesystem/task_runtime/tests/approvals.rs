use super::*;
use crate::{
    approvals::{ApprovalPolicy, ApprovalReviewFile, RuleVerdict},
    conversation::FileDescriptor,
    executor::ExecutionJournal as _,
    interaction::{ApprovalDecisionFile, ApprovalSubject},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskFiles},
};

struct Initialized {
    runtime: Runtime,
    files: Arc<TaskFiles<MemoryStream>>,
    responder: Scope,
    task: TaskId,
    _fixture: Fixture,
    lease: WorkLease,
    _conversation: AuthorityRoot,
}

async fn setup() -> Result<Initialized> {
    setup_with_io(None).await
}

async fn setup_with_io(io: Option<&PreparationIo>) -> Result<Initialized> {
    let mut fixture = Fixture::new().await?;
    if io.is_some() {
        fixture.machine.native.store(true, Ordering::SeqCst);
        fixture.tool.native.store(true, Ordering::SeqCst);
    }
    let grants = Capabilities::new(
        fixture
            .scope
            .grants()
            .iter()
            .chain([crate::contract::capability::INTERACTION_ROUTE]),
    );
    fixture.signed =
        fixture
            .issuer
            .bootstrap_scope("owner", &grants, fixture.signed.agent(), &mut None)?;
    fixture.scope = RuntimeScope::new(grants, Limits::default())?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let files = Arc::new(
        TaskFiles::open(
            fixture.stream.clone(),
            &fixture.volume,
            &verifier,
            &fixture.signed,
            65_536,
        )
        .await?,
    );
    let gate = Arc::new(ApprovalPolicy::default().bind()?);
    let mut runtime = fixture.runtime_with_policy(io, Some(gate)).await?;
    runtime.bind_task_files(Some(files.clone()));
    let conversation = AuthorityRoot::new(
        "approval-binding",
        [113; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "approval-binding".into(),
        },
    );
    let mut aggregate = crate::store::StreamAggregate::open(
        &fixture.stream,
        (
            conversation.audience().clone(),
            conversation.verifier(&mut None)?,
            SchemaRegistry::new(),
        ),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    aggregate
        .execute(
            crate::core::Command {
                operation_id: OperationId::from_bytes([115; 16]),
                idempotency_key: crate::IdempotencyKey::new("bind-approval-fixture")?,
                expected_revision: 0,
                scope: conversation.bootstrap_scope(
                    "binder",
                    &Capabilities::new(["conversation:bind"]),
                    None,
                    &mut None,
                )?,
                causal_parent: None,
                action: crate::core::Action::BindConversation {
                    agent: fixture.signed.agent().ok_or_else(|| {
                        Error::Invalid("approval fixture requires an agent".into())
                    })?,
                },
            },
            &mut None,
        )
        .await?;
    let owner = conversation.bootstrap_scope(
        "conversation",
        &Capabilities::new([
            crate::contract::capability::INTERACTION_OPEN.to_owned(),
            fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            fixture
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        ]),
        fixture.signed.agent(),
        &mut None,
    )?;
    runtime = runtime
        .with_interaction_owner(
            conversation.verifier(&mut None)?,
            SchemaRegistry::new(),
            owner,
            fixture.volume.clone(),
        )?
        .with_approval_authority(&conversation)?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease, &mut None).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let files = runtime.approval_files(task).await?;
    let id = crate::InteractionId::from_bytes([114; 16]);
    let responder = conversation.bootstrap_scope(
        "human",
        &Capabilities::new([
            "interaction:resolve".to_owned(),
            format!("interaction:respond:{id}"),
        ]),
        None,
        &mut None,
    )?;
    Ok(Initialized {
        runtime,
        files,
        responder,
        task,
        _fixture: fixture,
        lease,
        _conversation: conversation,
    })
}

async fn request(initialized: &Initialized) -> Result<FileValue> {
    let Initialized {
        runtime,
        task,
        files,
        lease,
        ..
    } = initialized;
    let task = *task;
    let admission = runtime.admission_observed(task, None).await?;
    let journal = runtime
        .open_execution_journal(
            task,
            LeaseFence::from(&lease.reservation),
            OperationId::from_bytes(task.into_bytes()),
            &admission,
        )
        .await?;
    let id = crate::InteractionId::from_bytes([114; 16]);
    let interaction = Interaction::approval(
        "One authenticated binding".into(),
        OperationId::from_bytes(task.into_bytes()),
        [5; 32],
        ApprovalSubject::Opaque {},
    )?;
    journal.open_interaction(id, &interaction, None)?.await?;
    let request = files
        .snapshot(
            &Query::File(FileKey::Task {
                task: Component::new(task.to_string())?,
                file: TaskFile::ApprovalRequest(Component::new(id.to_string())?),
            }),
            1,
        )
        .await?
        .files
        .remove(0);
    let digest = FileDescriptor::from_bytes(&request.bytes, "application/json", &mut None)?;
    let review = ApprovalReviewFile {
        principal: "human".into(),
        request_sha256: ApprovalDecisionFile::new("human", digest.sha256(), 1, true, None)?
            .request_sha256,
        policy_digest: runtime
            .approval_policy_identity()
            .ok_or_else(|| Error::Invalid("missing fixture policy".into()))?
            .digest,
        stage: 0,
        verdict: RuleVerdict::Ask,
        reason: None,
    };
    let file = FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string())?,
            file: TaskFile::ApprovalReview {
                request: Component::new(id.to_string())?,
                principal: Component::new(ApprovalDecisionFile::principal_file("human"))?,
            },
        },
        crate::contract::canonical_json_bytes(&review)?,
    );
    files.inner.append(files.inner.head().await?, &file).await?;
    Ok(FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string())?,
            file: TaskFile::ApprovalDecision {
                request: Component::new(id.to_string())?,
                principal: Component::new(ApprovalDecisionFile::principal_file("human"))?,
            },
        },
        crate::contract::canonical_json_bytes(&ApprovalDecisionFile::new(
            "human",
            digest.sha256(),
            1,
            true,
            None,
        )?)?,
    ))
}

#[tokio::test]
async fn native_recorded_tool_waits_for_exact_approval_and_replays_without_dispatch() -> Result<()>
{
    for approved in [false, true] {
        let work = acyclic_fs::NativeMaterializationWork::new(
            super::native_mail::BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let initialized = setup_with_io(Some(&io)).await?;
        let runtime = &initialized.runtime;
        let fixture = &initialized._fixture;
        fixture.tool.receipt_ready.store(true, Ordering::SeqCst);
        let task = initialized.task;
        let fence = LeaseFence::from(&initialized.lease.reservation);
        let context = runtime
            .task_context_with_native_work(task, &fence, &work)
            .await?;
        let admission = runtime.admission_observed(task, None).await?;
        let command: WorkflowCommand = serde_json::from_value(
            admission
                .input
                .get("command")
                .ok_or_else(|| Error::Invalid("fixture command absent".into()))?
                .clone(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let source = context.read_file(&command.payload, Some(&io)).await?;
        let payload: Value = io.decode_json(&source)?;
        let commands = runtime.commands();
        let progress = commands
            .execute_with_native_work(
                Arc::clone(&context),
                fence.clone(),
                command.clone(),
                payload,
                &work,
            )?
            .await?;
        assert!(
            matches!(progress, TaskCommandProgress::Pending),
            "unresolved policy dispatched a tool"
        );
        assert_eq!(fixture.tool.executed.load(Ordering::SeqCst), 0);
        let (journal, operation) = runtime
            .execution_journal_with_native_work(task, &fence, command.operation_id, &work)
            .await?;
        assert!(
            journal
                .replay_page(operation, 0, 4, false, Some(&io))
                .await?
                .is_empty()
        );
        let definition = &fixture
            .tools
            .get("test.deadline", Some("1"), None)?
            .ok_or_else(|| Error::Invalid("fixture tool absent".into()))?
            .definition;
        let invocation = ToolInvocation {
            operation_id: operation,
            call_id: operation.to_string(),
            name: definition.name.clone(),
            arguments: Value::Null,
        };
        let policy = runtime
            .approval_policy_identity()
            .ok_or_else(|| Error::Invalid("fixture policy absent".into()))?;
        let (approval, _) =
            crate::runtime::tool_approval_binding(task, &policy, definition, &invocation, None)?;
        let id = crate::interaction::scoped_id(task.into_bytes(), approval, &mut None)?;
        let request = runtime
            .interaction_host(None)?
            .read_request(id, None)
            .await?
            .ok_or_else(|| Error::Invalid("recorded tool approval absent".into()))?;
        assert!(
            matches!(request, Interaction::Approval { operation_id, subject: ApprovalSubject::Tool {
            route: crate::interaction::ApprovalRoute::Policy, tool, call_id, arguments,
        }, .. } if operation_id == operation && tool == invocation.name && call_id == invocation.call_id && arguments == invocation.arguments)
        );
        let responder = initialized._conversation.bootstrap_scope(
            "human",
            &Capabilities::new([
                "interaction:resolve".to_owned(),
                format!("interaction:respond:{id}"),
            ]),
            None,
            &mut None,
        )?;
        // A human responder owns a separate passive connection, never the
        // executing command's native work or its dispatch context.
        let admission = runtime.admission_observed(task, None).await?;
        let response_journal = runtime
            .open_execution_journal(task, fence.clone(), command.operation_id, &admission)
            .await?;
        response_journal
            .resolve_interaction(
                id,
                crate::interaction::InteractionResponse::Approval {
                    approved,
                    reason: None,
                },
                &responder,
            )
            .await?;
        for _ in 0..2 {
            // Resumption and replay are separate invocations. Keep their
            // original ceiling and allocation ledger, including live custody.
            let work = work.invocation_window()?;
            assert_eq!(work.budget(), super::native_mail::BUDGET);
            let io = PreparationIo::from_native_work(&work)?;
            let source = context.read_file(&command.payload, Some(&io)).await?;
            let payload: Value = io.decode_json(&source)?;
            let progress = commands
                .execute_with_native_work(
                    Arc::clone(&context),
                    fence.clone(),
                    command.clone(),
                    payload,
                    &work,
                )?
                .await?;
            let TaskCommandProgress::ReadyWithNativeCustody { value, custody } = progress else {
                return Err(Error::Invalid("recorded native tool did not settle".into()));
            };
            assert!(custody.work().shares_ledger(&work));
            if approved {
                assert_eq!(value, json!({"Succeeded":"original receipt"}));
            } else {
                assert!(
                    value.get("Failed").is_some(),
                    "declined approval did not refuse the tool: {value}"
                );
            }
            assert_eq!(
                fixture.tool.executed.load(Ordering::SeqCst),
                usize::from(approved)
            );
            assert_eq!(fixture.tool.reconciled.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn approval_submission_consumes_one_authenticated_binding_for_both_entry_points() -> Result<()>
{
    use super::super::interactions::APPROVAL_BINDINGS;
    for reviewed in [false, true] {
        let initialized = setup().await?;
        let runtime = &initialized.runtime;
        let task = initialized.task;
        let files = &initialized.files;
        let responder = &initialized.responder;
        let decision = request(&initialized).await?;
        // The interaction owner stages a typed decision in its own encoding;
        // the submitted JSON's key order is not the published file's encoding.
        let owner_bytes = serde_json::to_vec(&ApprovalDecisionFile::decode(
            &decision.bytes,
            runtime.maximum_payload_bytes,
        )?)
        .map_err(|error| Error::Invalid(error.to_string()))?;
        APPROVAL_BINDINGS.with(|count| count.set(0));
        if reviewed {
            runtime.submit_reviewed_approval(task, &decision).await?;
        } else {
            runtime
                .write_approval_decision(task, &decision, responder)
                .await?;
        }
        APPROVAL_BINDINGS.with(|count| assert_eq!(count.get(), 1));
        assert!(
            matches!(files.snapshot(&Query::File(decision.key.clone()), 1).await?.files.as_slice(), [file] if file.bytes.as_ref() == owner_bytes)
        );
        // Old shape: selecting the review opens a view, then re-entering the
        // public writer opens another. The same one-binding oracle detects it.
        APPROVAL_BINDINGS.with(|count| count.set(0));
        drop(runtime.approval_files(task).await?);
        runtime
            .write_approval_decision(task, &decision, responder)
            .await?;
        APPROVAL_BINDINGS.with(|count| assert_eq!(count.get(), 2));
        let mut forged = decision.clone();
        let mut body: ApprovalDecisionFile = crate::contract::json_from_slice(&forged.bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        body.request_sha256 = "11".repeat(32);
        forged.bytes = crate::contract::canonical_json_bytes(&body)?.into();
        assert!(
            runtime
                .submit_reviewed_approval(task, &forged)
                .await
                .is_err()
        );
        assert!(
            runtime
                .write_approval_decision(task, &forged, responder)
                .await
                .is_err()
        );
        assert!(
            matches!(files.snapshot(&Query::File(decision.key.clone()), 1).await?.files.as_slice(), [file] if file.bytes.as_ref() == owner_bytes)
        );
    }
    Ok(())
}

#[tokio::test]
async fn approval_terminal_retry_uses_the_owner_decision_value_for_both_entry_points() -> Result<()>
{
    for reviewed in [false, true] {
        let initialized = setup().await?;
        let runtime = &initialized.runtime;
        let task = initialized.task;
        let mut decision = request(&initialized).await?;
        let value = ApprovalDecisionFile::decode(&decision.bytes, runtime.maximum_payload_bytes)?;
        decision.bytes = serde_json::to_vec_pretty(&value)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .into();
        if reviewed {
            runtime.submit_reviewed_approval(task, &decision).await?;
        } else {
            runtime
                .write_approval_decision(task, &decision, &initialized.responder)
                .await?;
        }
        let retained = initialized
            .files
            .snapshot(&Query::File(decision.key.clone()), 1)
            .await?;
        let [owner] = retained.files.as_slice() else {
            panic!("owner decision absent")
        };
        // Executed negative control: the former raw-byte predicate refuses an
        // exact retry of the same bytes that the owner already admitted.
        assert_ne!(owner.bytes, decision.bytes);
        assert_eq!(
            ApprovalDecisionFile::decode(&owner.bytes, runtime.maximum_payload_bytes)?,
            value
        );
        runtime
            .task_host()
            .settle_task(
                task,
                &LeaseFence::from(&initialized.lease.reservation),
                crate::Outcome::Failed {
                    message: "terminal retry fixture".into(),
                },
                &mut None,
            )
            .await?;
        for changed in 0..6 {
            let mut retry = decision.clone();
            let mut body = value.clone();
            match changed {
                1 => body.principal = "another-human".into(),
                2 => body.request_sha256 = "11".repeat(32),
                3 => body.expected_version += 1,
                4 => body.verdict = crate::interaction::ApprovalVerdict::Deny,
                5 => body.reason = Some("changed decision".into()),
                _ => {}
            }
            retry.bytes = serde_json::to_vec_pretty(&body)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .into();
            let result = if reviewed {
                runtime.submit_reviewed_approval(task, &retry).await
            } else {
                runtime
                    .write_approval_decision(task, &retry, &initialized.responder)
                    .await
            };
            assert_eq!(
                result.is_ok(),
                changed == 0,
                "reviewed={reviewed}, changed={changed}: {result:?}"
            );
        }
        assert_eq!(
            initialized
                .files
                .snapshot(&Query::File(decision.key.clone()), 1)
                .await?
                .files,
            retained.files,
            "terminal retry changed the owner file"
        );
    }
    Ok(())
}

#[test]
fn approval_readiness_observes_request_and_outcome_at_one_cut() -> Result<()> {
    use acyclic_test_support::differential;
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for route in [
        crate::interaction::ApprovalRoute::Policy,
        crate::interaction::ApprovalRoute::Human,
    ] {
        for approved in [false, true] {
            let initialized = executor.block_on(setup())?;
            let runtime = &initialized.runtime;
            let task = initialized.task;
            let definition = &initialized
                ._fixture
                .tools
                .get("test.deadline", None, None)?
                .ok_or_else(|| Error::Invalid("fixture tool absent".into()))?
                .definition;
            let invocation = ToolInvocation::for_model_call(
                OperationId::from_bytes(task.into_bytes()),
                0,
                "readiness".into(),
                definition.name.clone(),
                serde_json::Value::Null,
                None,
            )?;
            let policy = runtime
                .approval_policy_identity()
                .ok_or_else(|| Error::Invalid("fixture policy absent".into()))?;
            let (operation, digest) = crate::runtime::tool_approval_binding(
                task,
                &policy,
                definition,
                &invocation,
                None,
            )?;
            let id = crate::interaction::scoped_id(task.into_bytes(), operation, &mut None)?;
            let context = executor.block_on(
                runtime.task_context(task, LeaseFence::from(&initialized.lease.reservation)),
            )?;
            let admission = executor.block_on(runtime.admission_observed(task, None))?;
            let journal = executor.block_on(runtime.open_execution_journal(
                task,
                LeaseFence::from(&initialized.lease.reservation),
                OperationId::from_bytes(task.into_bytes()),
                &admission,
            ))?;
            for state in 0..3 {
                if state == 1 {
                    let request = Interaction::approval(
                        "Inspect the exact action".into(),
                        invocation.operation_id,
                        digest,
                        ApprovalSubject::Tool {
                            route,
                            tool: invocation.name.clone(),
                            call_id: invocation.call_id.clone(),
                            arguments: invocation.arguments.clone(),
                        },
                    )?;
                    executor.block_on(journal.open_interaction(id, &request, None)?)?;
                } else if state == 2 {
                    let responder = initialized._conversation.bootstrap_scope(
                        "human",
                        &Capabilities::new([
                            "interaction:resolve".to_owned(),
                            format!("interaction:respond:{id}"),
                        ]),
                        None,
                        &mut None,
                    )?;
                    executor.block_on(journal.resolve_interaction(
                        id,
                        crate::interaction::InteractionResponse::Approval {
                            approved,
                            reason: None,
                        },
                        &responder,
                    ))?;
                }
                let (ready, trace) = differential::observe::<_, _, (&'static str, u64)>(
                    || (),
                    |()| {
                        executor.block_on(runtime.tool_approval_ready(
                            &context,
                            definition,
                            &invocation,
                            None,
                        ))
                    },
                );
                if route == crate::interaction::ApprovalRoute::Human && state != 0 {
                    assert!(
                        matches!(ready, Err(Error::Conflict(_))),
                        "ordinary policy readiness accepted a human-routed request"
                    );
                    continue;
                }
                assert_eq!(ready?, state == 2);
                assert_eq!(
                    trace
                        .iter()
                        .filter(|(name, _)| *name == "interaction-reconstruction")
                        .count(),
                    1,
                    "approval readiness reconstructed more than one cut"
                );
                if state != 0 {
                    // Execute the former two-read shape against the same request.
                    // Both allow and deny are terminal observations, not authority.
                    let (old_ready, old_trace) = differential::observe::<_, _, (&'static str, u64)>(
                        || (),
                        |()| {
                            executor.block_on(async {
                                let host = runtime.interaction_host(None)?;
                                assert!(host.read_request(id, None).await?.is_some());
                                Ok::<_, Error>(
                                    host.read(id, None)
                                        .await?
                                        .as_ref()
                                        .and_then(|(_, resolution)| resolution.as_ref())
                                        .is_some_and(|resolution| resolution.outcome.is_terminal()),
                                )
                            })
                        },
                    );
                    assert_eq!(old_ready?, state == 2);
                    assert_eq!(
                        old_trace
                            .iter()
                            .filter(|(name, _)| *name == "interaction-reconstruction")
                            .count(),
                        2
                    );
                }
            }
        }
    }
    Ok(())
}
