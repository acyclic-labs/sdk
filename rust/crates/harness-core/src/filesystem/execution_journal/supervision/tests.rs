//! Original journal publications, not a second observer or copied history.
use super::*;
use crate::supervision::{Action, Kind};

async fn authored_workspace<P, A, O>(
    journal: &FilesystemExecutionJournal<P, A, O>,
    issuer: &crate::core::AuthorityRoot,
    bytes: &[u8],
) -> Result<(
    crate::runtime::ContentBindings,
    crate::runtime::RuntimeScope,
    FileRef,
)>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    use crate::conversation::{ContentAccess, ContentChange, ContentPublisher, VolumeOwner};
    let VolumeOwner::Agent(agent) = journal.volume.owner() else {
        return Err(Error::Invalid(
            "authored workspace needs its original agent".into(),
        ));
    };
    let volume = VolumeRef::new(
        journal.volume.provider().clone(),
        "authored-workspace".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(*agent),
        &mut None,
    )?;
    journal.host.create_volume(&volume).await?;
    let scope = issuer.bootstrap_scope(
        "authored-owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(*agent),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let publisher = crate::filesystem::FilesystemContentPublisher::new(
        journal.host.clone(),
        &volume,
        &verifier,
        &scope,
        64 * 1024,
        None,
    )?;
    let (receipt, _hold) = publisher
        .stage_changes(
            &volume,
            OperationId::from_bytes([125; 16]),
            &[ContentChange::CreateAbsent {
                path: "work.txt",
                bytes,
                media_type: "text/plain",
                display_name: "work.txt",
            }],
            ContentAccess::default(),
        )?
        .await?;
    let source = receipt
        .files()
        .next()
        .cloned()
        .ok_or_else(|| Error::Invalid("source missing".into()))?;
    Ok((
        crate::runtime::ContentBindings {
            volume,
            reader: crate::filesystem::FilesystemContentVerifier::new(
                journal.host.clone(),
                &verifier,
                &scope,
                64 * 1024,
                None,
            )?,
            writer: Some(publisher),
        },
        crate::runtime::RuntimeScope::new(scope.capabilities().clone(), Default::default())?,
        source,
    ))
}
#[tokio::test]
async fn original_content_receipts_count_changes_once_and_exclude_same_bytes() -> Result<()> {
    use crate::conversation::{ContentAccess, ContentChange, ContentPublisher};
    use crate::supervision::{RunAdmission, RunAdmissionSource};
    let (journal, issuer) = super::super::test_support::journal().await?;
    let run = OperationId::from_bytes([113; 16]);
    let agent = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([81; 16]));
    let policy = Policy {
        no_progress_turns: Some(1),
        ..Default::default()
    };
    let goal_bytes = b"Finish the task";
    let policy_bytes =
        serde_json::to_vec(&policy).map_err(|error| Error::Invalid(error.to_string()))?;
    let declaration = RunAdmission::new(run, goal_bytes, &policy_bytes);
    let goal = journal
        .stage(run, "goal", goal_bytes, "text/plain", None)?
        .await?;
    let policy_file = crate::executor::stage_json(&journal, run, "policy", &policy, None).await?;
    let admission =
        crate::executor::stage_json(&journal, run, "admission", &declaration, None).await?;
    let mut journal = journal
        .with_run_admission(Some(RunAdmissionSource {
            admission,
            goal,
            policy: policy_file,
            value: declaration.verify(run, goal_bytes, &policy_bytes)?,
        }))
        .with_agent_events(agent.clone(), run, 0, true);
    let publisher = crate::filesystem::FilesystemContentPublisher::new(
        journal.host.clone(),
        &journal.volume,
        &journal.verifier,
        &journal.scope,
        64 * 1024,
        None,
    )?;
    let stream = journal.stream.clone();
    let queries = Queries::new(&*stream, &agent, run);
    let mut source = None;
    for turn in 0..2 {
        journal = journal.with_agent_events(agent.clone(), run, turn, turn == 0);
        let execution = OperationId::from_bytes([114 + turn as u8; 16]);
        journal
            .append(
                execution,
                "started",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let effect = OperationId::from_bytes([116 + turn as u8; 16]);
        let change = match source.as_ref() {
            None => ContentChange::CreateAbsent {
                path: "work.txt",
                bytes: b"original",
                media_type: "text/plain",
                display_name: "work.txt",
            },
            Some(source) => ContentChange::UpdateExpected {
                source,
                bytes: b"original",
            },
        };
        let (receipt, _hold) = publisher
            .stage_changes(&journal.volume, effect, &[change], ContentAccess::default())?
            .await?;
        for _ in 0..2 {
            journal
                .published_progress(
                    execution,
                    effect,
                    crate::executor::ProgressPublication::Changes {
                        volume: &journal.volume,
                        changes: &receipt,
                    },
                    None,
                )?
                .await?;
        }
        assert_eq!(queries.progress_in_turn(turn).await?.0, turn == 0);
        let records = journal.records(execution, 0, 4, None).await?;
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record.event, ExecutionEvent::Progress { .. }))
                .count(),
            usize::from(turn == 0)
        );
        source = receipt.files().next().cloned();
    }
    // The ordinary edit/patch context uses the same accepted receipt and
    // recovery path; it does not infer progress from a serialized tool result.
    let operation = OperationId::from_bytes([118; 16]);
    let write_operation = OperationId::from_bytes([119; 16]);
    journal
        .append(
            write_operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let (content, scope, source) = authored_workspace(&journal, &issuer, b"original").await?;
    let mut bindings = crate::runtime::Bindings::local();
    bindings.scope = scope;
    bindings.content = Some(content.clone());
    let filesystem = journal.host.clone();
    let task = Arc::new(
        bindings
            .build()?
            .agent_context(&crate::Capabilities::new(Vec::<String>::new()), content)?
            .with_execution_journal(Arc::new(journal), operation)?,
    );
    let write = crate::runtime::ToolContext::new(task.clone(), write_operation, "write")?;
    let (source, _) = write
        .stage_tool_file_once(
            write_operation,
            source.path(),
            b"original",
            "text/plain",
            source.display_name(),
            None,
        )
        .await?;
    assert!(!queries.progress_in_turn(1).await?.0);
    let context = crate::runtime::ToolContext::new(task, operation, "edit")?;
    let (file, _) = context
        .stage_file_at(operation, &source, b"changed", None)
        .await?;
    assert!(queries.progress_in_turn(1).await?.0);
    let (recovered, _) = context
        .observe_tool_source_file_once(operation, Some(source.volume()), Some(source.path()), None)
        .await?;
    assert_eq!(recovered, Some(file));
    let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    assert!(
        filesystem
            .original_file_progress(&source, &io)
            .await?
            .is_none(),
        "a later edit changed the original same-byte verdict"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn failed_progress_releases_the_original_conditional_receipt_on_write_and_recovery()
-> Result<()> {
    use crate::supervision::{RunAdmission, RunAdmissionSource};
    for native in [false, true] {
        let (journal, _issuer) = super::super::test_support::journal().await?;
        let operation = OperationId::from_bytes([122; 16]);
        let policy = Policy {
            no_progress_turns: Some(1),
            ..Default::default()
        };
        let bytes =
            serde_json::to_vec(&policy).map_err(|error| Error::Invalid(error.to_string()))?;
        let declaration = RunAdmission::new(operation, b"Finish", &bytes);
        let goal = journal
            .stage(operation, "goal", b"Finish", "text/plain", None)?
            .await?;
        let policy_file =
            crate::executor::stage_json(&journal, operation, "policy", &policy, None).await?;
        let admission =
            crate::executor::stage_json(&journal, operation, "admission", &declaration, None)
                .await?;
        // Missing agent authority makes the real progress publication refuse,
        // after the independently authorized conditional content write.
        let journal = journal.with_run_admission(Some(RunAdmissionSource {
            admission,
            goal,
            policy: policy_file,
            value: declaration.verify(operation, b"Finish", &bytes)?,
        }));
        let (content, scope, source) = authored_workspace(&journal, &_issuer, b"before").await?;
        let mut bindings = crate::runtime::Bindings::local();
        bindings.scope = scope;
        bindings.content = Some(content.clone());
        let task = Arc::new(
            bindings
                .build()?
                .agent_context(&crate::Capabilities::new(Vec::<String>::new()), content)?
                .with_execution_journal(Arc::new(journal), operation)?,
        );
        let context = crate::runtime::ToolContext::new(task, operation, "edit")?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000, 16_000_000);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let baseline = io.owned_bytes()?;
        let error = context
            .stage_file_at(operation, &source, b"after", Some(&io))
            .await
            .expect_err("missing agent authority accepted progress");
        assert!(
            error
                .to_string()
                .contains("original progress has no agent binding"),
            "{error:?}"
        );
        drop(error);
        assert_eq!(
            io.owned_bytes()?,
            baseline,
            "failed progress leaked the accepted receipt"
        );
        let (receipt, retained) = context
            .observe_tool_changes(operation, source.volume(), Some(&io))
            .await?;
        let receipt = receipt.ok_or_else(|| Error::Invalid("accepted write disappeared".into()))?;
        let hold = io.guard_owned(retained)?;
        assert_eq!(
            receipt.files().next().unwrap().descriptor().byte_length(),
            5
        );
        drop(receipt);
        drop(hold);
        assert_eq!(io.owned_bytes()?, baseline);
        let error = context
            .observe_tool_source_file_once(
                operation,
                Some(source.volume()),
                Some(source.path()),
                Some(&io),
            )
            .await
            .expect_err("recovery accepted progress without agent authority");
        assert!(
            error
                .to_string()
                .contains("original progress has no agent binding"),
            "{error:?}"
        );
        drop(error);
        assert_eq!(
            io.owned_bytes()?,
            baseline,
            "failed recovery leaked the original receipt"
        );
    }
    Ok(())
}
#[tokio::test]
async fn selected_check_repetitions_ignore_capture_options_but_stay_within_the_original_turn()
-> Result<()> {
    use crate::supervision::{RunAdmission, RunAdmissionSource};
    for (same_turn, exact_command) in [(false, true), (true, true), (true, false)] {
        let (journal, _issuer) = super::super::test_support::journal().await?;
        let run = OperationId::from_bytes([112; 16]);
        let agent = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([81; 16]));
        let policy = Policy {
            validation_commands: ["check unit".to_owned()].into_iter().collect(),
            quiet_ms: Some(10),
            ..Default::default()
        };
        let goal = "Check the project";
        let goal_bytes =
            serde_json::to_vec(goal).map_err(|error| Error::Invalid(error.to_string()))?;
        let policy_bytes =
            serde_json::to_vec(&policy).map_err(|error| Error::Invalid(error.to_string()))?;
        let declaration = RunAdmission::new(run, &goal_bytes, &policy_bytes);
        let goal = crate::executor::stage_json(&journal, run, "goal", &goal, None).await?;
        let policy_file =
            crate::executor::stage_json(&journal, run, "policy", &policy, None).await?;
        let admission =
            crate::executor::stage_json(&journal, run, "admission", &declaration, None).await?;
        let (clock, _time) = acyclic_actors::local::ControlledClock::new(100);
        let mut journal = journal
            .with_clock(Arc::new(clock))
            .with_run_admission(Some(RunAdmissionSource {
                admission,
                goal,
                policy: policy_file,
                value: declaration.verify(run, &goal_bytes, &policy_bytes)?,
            }))
            .with_agent_events(agent.clone(), run, 0, true);
        journal
            .append(
                run,
                "started",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let stream = Arc::clone(&journal.stream);
        let queries = Queries::new(stream.as_ref(), &agent, run);
        let commands = if exact_command {
            ["check unit"; 3]
        } else {
            ["check  unit", "check\tunit", " check unit"]
        };
        for (index, command) in commands.into_iter().enumerate() {
            let operation = if same_turn || index == 0 {
                run
            } else {
                OperationId::from_bytes([index as u8; 16])
            };
            journal = journal.with_agent_events(
                agent.clone(),
                run,
                if same_turn { 0 } else { index as u64 },
                false,
            );
            if operation != run {
                journal
                    .append(
                        operation,
                        "started",
                        &ExecutionEvent::Started {
                            request_digest: [1; 32],
                        },
                        None,
                    )?
                    .await?;
            }
            let call_id = format!("call-{index}");
            let invocation = crate::tool::ToolInvocation::for_model_call(
                operation,
                0,
                call_id.clone(),
                "shell".into(),
                serde_json::json!({"command":command,"yield_time_ms":index+1,"max_output_tokens":100+index}),
                None,
            )?;
            let invocation = crate::executor::stage_json(
                &journal,
                operation,
                &format!("invocation-{index}"),
                &crate::tool::RecordedInvocation {
                    invocation,
                    metadata: serde_json::json!({"opaque":index}),
                    target: None,
                    filesystem: None,
                },
                None,
            )
            .await?;
            let started = ExecutionEvent::ToolStarted {
                step: 0,
                call_id,
                invocation,
                publication: None,
            };
            journal
                .append(operation, &format!("check-{index}"), &started, None)?
                .await?;
            // The original ToolStarted producer, not a hand-authored jobs
            // record, must retain the scheduler's configured clock domain.
            let mut jobs = stream
                .read(acyclic_stream::ReadRequest {
                    path: crate::supervision::tool_jobs::path(&agent)?,
                    from: index as u64,
                    limit: 1,
                    key: None,
                    at_least: Default::default(),
                })
                .await?;
            let job = futures::TryStreamExt::try_next(&mut jobs)
                .await?
                .ok_or_else(|| Error::Invalid("original quiet start missing".into()))?;
            let job: crate::supervision::tool_jobs::Record = serde_json::from_slice(&job.value)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert!(matches!(
                job.event,
                crate::supervision::tool_jobs::Event::Started {
                    at_ms: 100,
                    quiet_ms: 10,
                    ..
                }
            ));
            // Exact original replay is one admission, even after its decision.
            journal
                .append(operation, &format!("check-{index}"), &started, None)?
                .await?;
            let signal = queries.read_signal(Kind::ValidationLoop).await?;
            if same_turn && exact_command && index == 2 {
                assert_eq!(
                    signal
                        .ok_or_else(|| Error::Invalid("repeated check did not signal".into()))?
                        .count,
                    3
                );
                assert_eq!(
                    queries
                        .admission(&policy)
                        .await?
                        .control
                        .ok_or_else(|| Error::Invalid("check pause missing".into()))?
                        .signal,
                    Some(Kind::ValidationLoop)
                );
            } else {
                assert!(signal.is_none(), "check counted a replay or another turn");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn tool_failures_use_original_invocations_and_do_not_count_distinct_errors() -> Result<()> {
    for (same_error, exact_command) in [(false, true), (true, true), (true, false)] {
        let repeated = same_error && exact_command;
        let (journal, _issuer) = super::super::test_support::journal().await?;
        let run = OperationId::new();
        let agent = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([58; 16]));
        let journal = journal.with_agent_events(agent.clone(), run, 0, true);
        journal
            .append(
                run,
                "started",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let queries = Queries::new(&*journal.stream, &agent, run);
        let commands = if exact_command {
            ["check unit"; 3]
        } else {
            [
                "echo \"$(echo \"a  b\")\"",
                "echo \"$(echo \"a b\")\"",
                "check\tunit",
            ]
        };
        for (index, command) in commands.into_iter().enumerate() {
            let call_id = format!("call-{index}");
            let invocation = crate::tool::ToolInvocation::for_model_call(
                run,
                0,
                call_id.clone(),
                "shell".into(),
                serde_json::json!({"command":command,"yield_time_ms":index + 1,"max_output_tokens":100 + index}),
                None,
            )?;
            let invocation = crate::executor::stage_json(
                &journal,
                run,
                &format!("invoke-{index}"),
                &crate::tool::RecordedInvocation {
                    invocation,
                    metadata: serde_json::json!({"opaque":index}),
                    target: None,
                    filesystem: None,
                },
                None,
            )
            .await?;
            journal
                .append(
                    run,
                    &format!("start-{index}"),
                    &ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: call_id.clone(),
                        invocation,
                        publication: None,
                    },
                    None,
                )?
                .await?;
            let result = crate::tool::ToolResult {
                value: serde_json::json!({"exit_code":1,"stdout":format!("unrelated output {index}"),"stderr":if same_error {"same failure".into()} else {format!("failure {index}")}}),
            };
            let result = crate::executor::stage_json(
                &journal,
                run,
                &format!("result-{index}"),
                &result,
                None,
            )
            .await?;
            let projection = crate::executor::stage_json(
                &journal,
                run,
                &format!("projection-{index}"),
                &serde_json::json!({"text":"failed"}),
                None,
            )
            .await?;
            let terminal = ExecutionEvent::ToolCompleted {
                step: 0,
                call_id,
                result,
                projection,
                record_kind: None,
            };
            let key = format!("terminal-{index}");
            journal.append(run, &key, &terminal, None)?.await?;
            // A retained original terminal may replay after later query cuts.
            journal.append(run, &key, &terminal, None)?.await?;
            let signal = queries.read_signal(Kind::RepeatFailure).await?;
            if repeated && index >= 1 {
                assert_eq!(
                    signal.as_ref().map(|signal| signal.count),
                    Some(index as u64 + 1)
                );
            } else {
                assert!(signal.is_none());
            }
        }
        let cut = queries
            .checkpoint()
            .await?
            .ok_or_else(|| Error::Invalid("tool query cut absent".into()))?;
        assert_eq!(
            cut.action,
            if repeated {
                Action::Pause
            } else {
                Action::Alert
            }
        );
        assert_eq!(
            cut.state.through, 3,
            "ordinary admissions or terminal retries changed query accounting"
        );
        if repeated {
            for purpose in [
                crate::executor::ModelPurpose::Response,
                crate::executor::ModelPurpose::Summary,
            ] {
                let prepared = crate::model::PreparedModelRequest::prepare(
                    crate::model::ModelRequest {
                        summary: purpose == crate::executor::ModelPurpose::Summary,
                        compose_tools: false,
                        model: crate::model::Model::new(
                            "test".into(),
                            "dispatch".into(),
                            "1".into(),
                        )?,
                        messages: vec![crate::model::ModelMessage {
                            role: crate::model::ModelRole::User,
                            content: crate::model::ModelContent::Text("next".into()),
                        }],
                        tools: Vec::new(),
                        sampling: Default::default(),
                    },
                    Limits::default(),
                    None,
                )?;
                let request = crate::executor::stage_json(
                    &journal,
                    run,
                    &format!("next-model-request-{purpose:?}"),
                    prepared.request(),
                    None,
                )
                .await?;
                let denied = journal
                    .append(
                        run,
                        &format!("denied-{purpose:?}"),
                        &ExecutionEvent::ModelStarted {
                            step: 1,
                            purpose,
                            request_digest: prepared.request_digest(),
                            request,
                            context: prepared.context_window(),
                        },
                        None,
                    )?
                    .await;
                assert!(
                    matches!(denied, Err(Error::ExecutionControlled(control)) if control.signal == Some(Kind::RepeatFailure) && control.source == cut.source)
                );
            }
            assert_eq!(
                journal.records(run, 0, 16, None).await?.len(),
                7,
                "a blocked dispatch published another intent"
            );
        }
    }
    Ok(())
}
