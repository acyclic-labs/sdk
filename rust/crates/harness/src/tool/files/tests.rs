use super::*;

use super::*;
use crate::{
    AgentId, Capabilities, OperationId,
    conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};

fn file() -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new(
                "file-preflight".into(),
                "filesystem".into(),
                "1".into(),
                &mut None,
            )?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([61; 16])),
            &mut None,
        )?,
        "source.txt".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(b"exact", "text/plain", &mut None)?,
        "source.txt".into(),
        &mut None,
    )
}

#[test]
fn context_free_gate_rejects_even_valid_source_grants() -> Result<()> {
    let file = file()?;
    let missing = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let granted = RuntimeScope::new(
        Capabilities::new([file.read_capability()?]),
        Limits::default(),
    )?;
    for (tool, arguments) in [
        (
            read_file()?,
            serde_json::to_value(ReadFileInput { file: file.clone() })
                .map_err(|error| Error::Invalid(error.to_string()))?,
        ),
        (
            edit_file()?,
            serde_json::to_value(EditFileInput {
                file: file.clone(),
                old_text: "exact".into(),
                new_text: "changed".into(),
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
        ),
        (
            patch_file(4096, 16)?,
            serde_json::to_value(PatchFileInput {
                file: file.clone(),
                diff: "@@\n-exact\n+changed\n".into(),
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
        ),
    ] {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "preflight".into(),
            name: tool.definition.name.clone(),
            arguments,
        };
        assert!(matches!(
            tool.executor.authorize(None, &invocation),
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            tool.executor.authorize(Some(&missing), &invocation),
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            tool.executor.authorize(Some(&granted), &invocation),
            Err(Error::Unauthorized(_))
        ));
    }
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "read".into(),
        name: "acyclic.read_file".into(),
        arguments: json!({"file":file}),
    };
    let narrow = RuntimeScope::new(
        Capabilities::new([file.read_capability()?]),
        Limits {
            render_bytes: 4,
            ..Limits::default()
        },
    )?;
    assert!(matches!(
        read_file()?.executor.authorize(Some(&narrow), &invocation),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[test]
fn publication_template_rejects_missing_scope_and_invalid_path_before_dispatch() -> Result<()> {
    let tool = write_file()?;
    let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?;
    let invocation = ToolInvocation {
        operation_id: OperationId::new(),
        call_id: "write".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"path":"../escape.txt","text":"body","media_type":"text/plain","display_name":"body"}),
    };
    assert!(matches!(
        tool.executor.authorize(None, &invocation),
        Err(Error::Unauthorized(_))
    ));
    assert!(tool.executor.authorize(Some(&scope), &invocation).is_err());
    assert_eq!(tool.definition.revision, "portable-5");
    assert_eq!(
        patch_file(4096, 16)?.definition.revision,
        "portable-patch-4"
    );
    Ok(())
}
