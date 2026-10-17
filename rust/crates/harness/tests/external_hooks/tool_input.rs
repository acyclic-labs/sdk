use super::*;
use acyclic_harness::filesystem::ToolTaskCommand;
use proptest::prelude::*;

proptest! {
    // Property: native shell and write arguments map losslessly into the exact
    // selected harness revision. Metadata is not silently treated as authority.
    #[test]
    fn native_tool_inputs_pin_the_registry_and_preserve_content(
        text in ".{0,128}", name in "[a-z]{1,12}", command in "[a-z ]{1,48}",
    ) {
        let tools = acyclic_harness::tools::defaults()?;
        for (host,tool,args) in [
            (ExternalHost::Codex,"exec_command",json!({"cmd":command})),
            (ExternalHost::ClaudeCode,"Bash",json!({"command":command})),
            (ExternalHost::Pi,"bash",json!({"command":command})),
            (ExternalHost::ClaudeCode,"Write",json!({"file_path":name,"content":text})),
            (ExternalHost::Pi,"write",json!({"path":name,"content":text})),
        ] {
            let native = native_request(host,tool,args)?;
            let original = native.clone();
            let actual = native.tool_call(&tools)?;
            let selected = tools.get(&actual.name,None,None)?.ok_or_else(||Error::Invalid("selected tool is absent".into()))?;
            prop_assert_eq!(&actual.revision,&selected.definition.revision);
            if actual.name=="write" {
                prop_assert_eq!(&actual.arguments["text"],&json!(text));
                prop_assert_eq!(&actual.arguments["path"],&json!(name));
            } else {prop_assert_eq!(&actual.arguments,&json!({"command":command}));}
            let bytes=serde_json::to_vec(&actual).map_err(|error|invalid(&error))?;
            prop_assert_eq!(serde_json::from_slice::<ToolTaskCommand>(&bytes).map_err(|error|invalid(&error))?,actual);
            prop_assert_eq!(native,original);
        }
    }

    // Property: ambiguous aliases, unsupported shell options, parent escapes and
    // absolute project paths are rejected before any tool is admitted.
    #[test]
    fn native_tool_input_cannot_silently_drop_options_or_escape_paths(text in ".{0,128}") {
        let tools=acyclic_harness::tools::defaults()?;
        for args in [json!({"cmd":text,"command":text}),json!({"cmd":"pwd","workdir":text})] {
            prop_assert!(native_request(ExternalHost::Codex,"exec_command",args)?.tool_call(&tools).is_err());
        }
        for path in ["../escape","/outside",".system/secret","a/../outside","a\0b"] {
            prop_assert!(native_request(ExternalHost::Pi,"write",json!({"path":path,"content":text}))?.tool_call(&tools).is_err(), "unsafe native path was admitted");
        }
        let canonical = json!({"path":"file.txt", "text":text,
            "media_type":"text/plain", "display_name":"file.txt"});
        for missing in ["path", "text", "media_type", "display_name"] {
            let mut arguments = canonical.clone();
            arguments.as_object_mut().unwrap().remove(missing);
            prop_assert!(native_request(ExternalHost::Codex,"write",arguments)?.tool_call(&tools).is_err());
        }
    }
}

fn native_request(host: ExternalHost, name: &str, args: Value) -> Result<ExternalRequest> {
    let input = if host == ExternalHost::Pi {
        json!({"sessionId":"session","toolCallId":"call","toolName":name,"input":args})
    } else {
        json!({"session_id":"session","tool_use_id":"call","tool_name":name,"tool_input":args})
    };
    request(
        host,
        if host == ExternalHost::Pi {
            "tool_call"
        } else {
            "PreToolUse"
        },
        input,
    )
}
