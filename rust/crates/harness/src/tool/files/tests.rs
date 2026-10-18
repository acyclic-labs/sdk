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

fn read_call(arguments: Value) -> ToolInvocation {
    ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "read".into(),
        name: "read".into(),
        arguments,
    }
}

fn read_result(file: &FileRef, offset: u64, text: &str) -> ToolResult {
    ToolResult {
        value: serde_json::to_value(ReadFileResult {
            file: file.clone(),
            offset,
            text: text.into(),
        })
        .unwrap(),
    }
}

#[test]
fn read_projection_rejects_results_that_do_not_answer_the_request() -> Result<()> {
    let file = crate::tool::tests::file(b"a\nb\nc\n")?;
    let project = |arguments: Value, offset: u64, text: &str| {
        FileProjection::Read(schema::ProjectionMode::Full).project(
            &read_call(arguments),
            &read_result(&file, offset, text),
            None,
        )
    };
    let path = file.path();
    // Controls: the exact window and the exact whole file project.
    assert!(project(json!({"path":path,"offset":2,"limit":1}), 2, "b\n").is_ok());
    assert!(project(json!({"path":path}), 1, "a\nb\nc\n").is_ok());
    assert!(project(json!({"path":path,"offset":1}), 1, "a\nb\nc\n").is_ok());
    // A window that starts elsewhere or holds more lines than requested.
    assert!(project(json!({"path":path,"offset":2,"limit":1}), 3, "c\n").is_err());
    assert!(project(json!({"path":path,"offset":2,"limit":1}), 2, "b\nc\n").is_err());
    // A whole-file request answered by different, truncated or empty text.
    for text in ["x\ny\nz\n", "a\n", ""] {
        assert!(project(json!({"path":path}), 1, text).is_err(), "{text:?}");
        assert!(
            project(json!({"path":path,"offset":1}), 1, text).is_err(),
            "{text:?}"
        );
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn whole_read_verification_hashes_on_the_callers_ledger() -> Result<()> {
    let content = b"a\nb\nc\n";
    let file = crate::tool::tests::file(content)?;
    let call = read_call(json!({"path":file.path()}));
    let result = read_result(&file, 1, "a\nb\nc\n");
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = |budget| {
        acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
    };
    let projection = FileProjection::Read(schema::ProjectionMode::Full);
    let funded = work(acyclic_fs::WorkBudget::UNBOUNDED)?;
    projection.project(&call, &result, Some(&funded))?;
    assert!(funded.receipt().bytes_hashed >= content.len() as u64);
    // Negative control: with no hashing allowance the verification is denied.
    let denied = work(acyclic_fs::WorkBudget {
        bytes_hashed: 0,
        ..acyclic_fs::WorkBudget::UNBOUNDED
    })?;
    assert!(projection.project(&call, &result, Some(&denied)).is_err());
    assert_eq!(denied.receipt().bytes_hashed, 0);
    Ok(())
}

/// Every file tool's projection of a valid result satisfies the projection
/// schema its definition declares, in both projection modes.
#[test]
fn file_tool_projections_satisfy_their_declared_schemas() -> Result<()> {
    let file = crate::tool::tests::file(b"a\nb\n")?;
    let publication = ToolResult {
        value: serde_json::to_value(FileResult { file: file.clone() }).unwrap(),
    };
    for mode in [
        schema::ProjectionMode::Full,
        schema::ProjectionMode::Reference,
    ] {
        let tools = [
            (
                FileTool::Read.assemble(mode)?,
                json!({"path":file.path()}),
                read_result(&file, 1, "a\nb\n"),
            ),
            (
                FileTool::Write.assemble(mode)?,
                json!({"path":file.path(),"text":"a\nb\n"}),
                publication.clone(),
            ),
            (
                FileTool::Edit.assemble(mode)?,
                json!({"path":file.path(),"old_string":"a","new_string":"b"}),
                publication.clone(),
            ),
            (
                FileTool::Patch {
                    maximum_work: 4096,
                    maximum_hunks: 16,
                }
                .assemble(mode)?,
                json!({"file":file,"diff":"@@\n-a\n+b\n"}),
                publication.clone(),
            ),
        ];
        for (tool, arguments, result) in tools {
            let mut call = read_call(arguments);
            call.name = tool.definition.name.clone();
            let (value, _) = tool.projection.project(&call, &result, None)?;
            tool.definition
                .validate_projection(&value, Limits::default(), None)
                .map_err(|error| Error::Invalid(format!("{}: {error}", tool.definition.name)))?;
        }
    }
    Ok(())
}
