use super::{
    session_runtime::{Fixture, VolumeBinding},
    *,
};
use acyclic_harness::{
    TaskId, conversation::ContentResidencyVerifier, runtime::TaskStateProvider,
    task_volume::Component,
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Property: the fenced worker executes a tool exactly when its native tuple
    // matches the immutable attachment. Rejection leaves content unchanged and
    // the next authenticated command remains executable in that same session.
    #[test]
    fn attachment_authority_is_the_exact_native_session(
        session in "[a-z]{1,12}", caller in prop::option::of("[a-z]{1,12}"),
        host_changed in any::<bool>(), session_changed in any::<bool>(), caller_changed in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(Box::pin(async {
            let fixture = Fixture::new(VolumeBinding::Separate).await?;
            let task = Component::new(TaskId::from_bytes([78;16]).to_string()).map_err(storage)?;
            let original = request(ExternalHost::Pi,"session_start", json!({
                "sessionId":session,"agentId":caller,"cwd":"/project"
            }))?;
            super::intake::deliver(&fixture,&task,&original).await?;
            let admitted = fixture.runtime.task_host()
                .observe_admission(TaskId::from_bytes([78;16])).await?;
            let command: acyclic_harness::workflow::WorkflowCommand =
                serde_json::from_value(admitted.input["command"].clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            // Generated command inputs stay within the existing directory grant
            // of the workflow's journal volume, including after retry/reopen.
            assert!(command.payload.path().starts_with("inputs/"));
            let host = if host_changed {ExternalHost::ClaudeCode} else {ExternalHost::Pi};
            let session = if session_changed {format!("other-{session}")} else {session};
            let caller = if caller_changed {Some(format!("other-{}",caller.as_deref().unwrap_or("root")))} else {caller};
            let input = if host == ExternalHost::Pi { json!({
                "sessionId":session,"agentId":caller,"toolCallId":"call",
                "toolName":"write","input":{"path":"file","content":"text"}
            }) } else {json!({
                "session_id":session,"agent_id":caller,"tool_use_id":"call",
                "tool_name":"Write","tool_input":{"file_path":"file","content":"text"}
            })};
            let native = request(host, if host == ExternalHost::Pi {"tool_call"} else {"PreToolUse"}, input)?;
            let answer = super::intake::deliver(&fixture,&task,&native).await?;
            let rejected = host_changed || session_changed || caller_changed;
            if rejected {
                assert_eq!(answer,json!({"Failed":{"message":"request differs from the attached native session"}}));
            } else {
                assert!(answer.get("Succeeded").is_some(),"{answer}");
            }
            let reader = acyclic_harness::filesystem::FilesystemContentVerifier::new(
                fixture.files.clone(),&fixture.verifier,&fixture.signed,65_536,None)?;
            let written = reader.read_private_path(&fixture.volume,"","file",None,65_536,None)?.await;
            if rejected {assert!(matches!(written,Err(Error::NotFound(_))),"rejected native identity wrote content");}
            else {assert_eq!(written?.1.as_ref(),b"text");}
            let next = request(ExternalHost::Pi,"tool_call",json!({
                "sessionId":original.session_id,"agentId":original.caller,"toolCallId":"next",
                "toolName":"write","input":{"path":"after","content":"still authorized"}
            }))?;
            let next = super::intake::deliver(&fixture,&task,&next).await?;
            assert!(next.get("Succeeded").is_some(),"{next}");
            assert_eq!(reader.read_private_path(&fixture.volume,"","after",None,65_536,None)?.await?.1.as_ref(),b"still authorized");
            assert!(fixture.runtime.task_host().observe_admission(TaskId::from_bytes([78;16])).await? == admitted,
                "a tool request changed the retained native attachment");
            assert_eq!(fixture.runtime.task_host().outcome(TaskId::from_bytes([78;16])).await?,None);
            Ok::<_,Error>(())
        }))?;
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
