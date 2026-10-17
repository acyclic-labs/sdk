use super::*;
use crate::external_executor::{ExternalAction, ExternalHost, request};
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // An explicit task is an exact constraint, never permission to fall back
    // to another native session's root task. Workspace-wide readers omit it.
    #[test]
    fn task_constraints_never_fall_back_to_a_new_root(identity in any::<[u8;16]>()) {
        let expected = crate::TaskId::from_bytes(identity);
        prop_assert_eq!(require_task(expected,None)?,expected);
        prop_assert_eq!(require_task(expected,Some(expected))?,expected);
        let mut changed = identity;
        changed[0] ^= 1;
        prop_assert!(require_task(crate::TaskId::from_bytes(changed),Some(expected)).is_err());
        let input = json!({"provider":"local","workspace":std::env::temp_dir(),"task":expected});
        let binding = HookBinding::from_bytes(&serde_json::to_vec(&input)?)?;
        prop_assert_eq!(HookBinding::from_bytes(&serde_json::to_vec(&binding)?)?,binding);
    }

    // Provider choice survives encoding without accepting another provider's
    // authority fields, relative local workspaces, or embedded NUL paths.
    #[test]
    fn local_bindings_round_trip_and_reject_ambiguous_authority(part in "[a-zA-Z0-9_-]{1,24}") {
        let workspace = std::env::temp_dir().join(&part);
        let mut input = json!({"provider":"local","workspace":workspace});
        let binding = HookBinding::from_bytes(&serde_json::to_vec(&input)?)?;
        prop_assert_eq!(HookBinding::from_bytes(&serde_json::to_vec(&binding)?)?, binding);
        input["endpoint"] = json!("https://other.test");
        prop_assert!(HookBinding::from_bytes(&serde_json::to_vec(&input)?).is_err());
        input.as_object_mut().unwrap().remove("endpoint");
        for invalid in [part.clone(), format!("{}\0{part}",workspace.display())] {
            input["workspace"] = json!(invalid);
            prop_assert!(HookBinding::from_bytes(&serde_json::to_vec(&input)?).is_err());
        }
    }

    // Rebinding changes only Attach's workspace, never the session/caller,
    // retry identity, or the original tool arguments and completions.
    #[test]
    fn workspace_selection_preserves_native_identity_and_nonattachment_bytes(
        session in "[a-z0-9-]{1,24}", selected in "[a-z0-9-]{1,24}", original in "[^\\x00]{1,24}",
    ) {
        let workspace = std::env::temp_dir().join(selected);
        for host in [ExternalHost::Codex, ExternalHost::ClaudeCode, ExternalHost::Pi] {
            let pi = host == ExternalHost::Pi;
            let mut attach = request(host,if pi {"session_start"} else {"SessionStart"},
                json!({"sessionId":session,"agentId":"child","cwd":original}))?;
            let task = attach.task_id();
            let operation = attach.operation_id;
            select_workspace(&mut attach,&workspace)?;
            prop_assert_eq!(attach.task_id(),task);
            prop_assert_eq!(attach.operation_id,operation);
            prop_assert!(attach.valid());
            let expected = ExternalAction::Attach {workspace:workspace.to_str().unwrap().into()};
            prop_assert_eq!(&attach.action,&expected);
            for (event,input) in [
                (if pi {"tool_call"} else {"PreToolUse"},json!({"sessionId":session,"agentId":"child",
                    "toolCallId":"call","toolName":"write","input":{"path":"out.txt","content":original}})),
                (if pi {"session_shutdown"} else {"SessionEnd"},json!({"sessionId":session,"agentId":"child"})),
            ] {
                let mut call = request(host,event,input)?;
                let before = serde_json::to_vec(&call)?;
                select_workspace(&mut call,&workspace)?;
                prop_assert_eq!(serde_json::to_vec(&call)?,before);
            }
        }
    }
}
