use super::*;
use crate::{Capabilities, OperationId, conversation::Limits};

#[test]
fn context_free_gate_rejects_even_valid_source_grants() -> Result<()> {
    let file = crate::tool::tests::file(b"exact")?;
    let missing = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let grants = Capabilities::new([file.read_capability(&mut None)?]);
    let granted = RuntimeScope::new(grants.clone(), Limits::default())?;
    let mut invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "preflight".into(),
        name: String::new(),
        arguments: Value::Null,
    };
    for (tool, arguments) in [
        (read()?, json!({"path":file.path()})),
        (
            edit()?,
            json!({"path":file.path(),"old_string":"exact","new_string":"changed"}),
        ),
        (
            patch_file(4096, 16)?,
            json!({"file":file,"diff":"@@\n-exact\n+changed\n"}),
        ),
    ] {
        invocation.name = tool.definition.name.clone();
        invocation.arguments = arguments;
        for scope in [None, Some(&missing), Some(&granted)] {
            assert!(matches!(
                tool.executor.authorize(scope, None, &invocation, None),
                Err(Error::Unauthorized(_))
            ));
        }
    }
    // Malformed path-addressed calls fail in preflight, before the context
    // gate; the valid control above reaches the gate.
    for (tool, arguments) in [
        (read()?, json!({"path":file.path(),"offset":0})),
        (read()?, json!({"path":file.path(),"limit":0})),
        (read()?, json!({"path":"../escape.txt"})),
        (
            read()?,
            json!({"path":file.path(),"range":{"start":0,"end":1}}),
        ),
        (
            edit()?,
            json!({"path":file.path(),"old_string":"","new_string":"changed"}),
        ),
        (
            edit()?,
            json!({"file":file,"old_text":"exact","new_text":"changed"}),
        ),
    ] {
        invocation.name = tool.definition.name.clone();
        invocation.arguments = arguments;
        assert!(matches!(
            tool.executor
                .authorize(Some(&granted), None, &invocation, None),
            Err(Error::Invalid(_))
        ));
    }
    Ok(())
}

#[test]
fn publication_template_rejects_missing_scope_and_invalid_path_before_dispatch() -> Result<()> {
    let tool = write()?;
    let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "write".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"path":"../escape.txt","text":"body","media_type":"text/plain","display_name":"body"}),
    };
    assert!(matches!(
        tool.executor.authorize(None, None, &invocation, None),
        Err(Error::Unauthorized(_))
    ));
    assert!(
        tool.executor
            .authorize(Some(&scope), None, &invocation, None)
            .is_err()
    );
    Ok(())
}
