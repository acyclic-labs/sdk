use crate::{
    OperationId, TaskId,
    hooks::{HookEvent, HookFailure, HookRule, HookRun, HookSpec, Hooks},
    registry::ComponentIdentity,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry},
};
use serde_json::json;
const CANONICAL: &str = "mcp.docs.search";
const WIRE: &str = "mcp__docs__search";
/// Checks canonical hook selection and approval binding for a parsed provider call.
///
/// # Panics
/// Panics if hooks or approvals replace the canonical tool identity.
pub async fn assert_canonical_tool_consumers(call: &crate::model::ToolCall) -> crate::Result<()> {
    assert_eq!(call.name, CANONICAL);
    let definition = ToolDefinition {
        name: CANONICAL.into(),
        revision: "1".into(),
        description: "Search documents".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
        projection_schema: json!({}),
    };
    let invocation = ToolInvocation::for_model_call(
        OperationId::from_bytes([1; 16]),
        0,
        call.call_id.clone(),
        call.name.clone(),
        call.arguments.clone(),
        None,
    )?;
    let mut hooks = Hooks::default();
    for event in [HookEvent::PreToolUse, HookEvent::PostToolUse] {
        hooks.add_rules([HookRule {
            on: event,
            matcher: r"^mcp\.docs\.search$".into(),
            hooks: vec![HookSpec {
                run: HookRun::Executable {
                    executable: "guard".into(),
                    argv: vec![],
                },
                timeout_ms: 1000,
                failure: HookFailure::Block,
            }],
        }])?;
        let output = hooks.run_commands(event, &json!({"tool_name":invocation.name}), &ToolRegistry::default(), 4096, |_,_| async {
            Ok((Some(0), br#"{"hookSpecificOutput":{"permissionDecision":"ask","permissionDecisionReason":"canonical guard","additionalContext":"canonical guard"}}"#.to_vec(), Vec::new()))
        }).await?;
        match event {
            HookEvent::PreToolUse => assert_eq!(
                output.approval.as_deref(),
                Some("canonical guard"),
                "canonical hook identity"
            ),
            HookEvent::PostToolUse => assert_eq!(
                output.additional_context,
                ["canonical guard"],
                "canonical hook identity"
            ),
            _ => unreachable!(),
        }
    }
    let task = TaskId::from_bytes([2; 16]);
    let policy = ComponentIdentity {
        name: "test.policy".into(),
        version: "1".into(),
        digest: [3; 32],
    };
    let expected =
        crate::executor::preparation_digest(&(task, &policy, &definition, &invocation), None)?;
    let ((_, approval), _) = crate::runtime::tool_approval_request(
        task,
        &policy,
        &definition,
        &invocation,
        ("canonical guard".into(), None),
        None,
    )?;
    let crate::interaction::Interaction::Approval {
        operation_id,
        action_digest,
        ..
    } = approval
    else {
        return Err(crate::Error::Conflict("approval kind".into()));
    };
    assert_eq!(operation_id, invocation.operation_id);
    assert_eq!(action_digest, expected, "canonical approval identity");
    let mut substituted = invocation.clone();
    substituted.name = WIRE.into();
    let substituted =
        crate::executor::preparation_digest(&(task, &policy, &definition, &substituted), None)?;
    assert_ne!(action_digest, substituted);
    Ok(())
}
