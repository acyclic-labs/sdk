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
        let task = Bindings {
            scope,
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
        let executor = uncompacted_executor(
            Model::new("fixture".into(), "unused".into(), "1".into())?,
            model.clone(),
            ContextPipeline::default(),
            tools,
        )
        .with_tool_authority(task.scope().clone(), policy.clone())?
        .with_task_context(&task, parent)?;
        let binding = executor
            .tool_binding("write")?
            .ok_or(Error::NotFound("write".into()))?;
        let call = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "write-receipt".into(),
            name: "write".into(),
            arguments: json!({"path":"receipt.txt","content":"accepted"}),
        };
        let reader = ToolContext::new(Arc::new(task), parent, "verify")?;
        let mut hooks = Vec::new();
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
        let mut outcome = executor
            .settle_invocation(
                journal.as_ref(),
                (parent, 0),
                (call.clone(), binding.clone()),
                &Value::Null,
                &mut hooks,
                None,
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
            let ((approval, _), _hold) = tool_approval_request(
                TaskId::from_bytes(parent.into_bytes()),
                &policy.identity(),
                &binding.definition,
                &call,
                ("approve this write".into(), None),
                None,
                (crate::interaction::ApprovalRoute::Policy, None),
            )?;
            let approval = crate::interaction::scoped_id(parent.into_bytes(), approval, &mut None)?;
            storage
                .decide_approval(
                    approval,
                    crate::approvals::ApprovalResponder::Human("human"),
                    crate::interaction::ApprovalVerdict::Allow,
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
        let replay = executor
            .settle_invocation(
                journal.as_ref(),
                (parent, 0),
                (call.clone(), binding),
                &Value::Null,
                &mut hooks,
                None,
            )?
            .await?;
        let ExecutionProgress::Ready(Some(replayed)) = replay else {
            return Err(Error::Invalid("host call lost its receipt".into()));
        };
        assert_eq!(replayed.content(), settled.content());
        assert_eq!(
            journal.replay(parent, 0, 64, None)?.await?.len(),
            records.len()
        );
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
