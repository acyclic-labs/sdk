//! Each family's layer data, applied to the calls its models make, yields
//! arguments the canonical capability decodes.
use super::{arguments::Arguments, *};
use crate::tool::files::{EditFileInput, ReadFileInput, WriteFileInput};
use crate::tools::ShellInput;
use serde_json::json;

fn adapt(profile: NativeToolProfile, variant: &str, call: Value) -> Result<Value> {
    let revision = profile
        .tools()?
        .into_iter()
        .find(|revision| revision.variant == variant)
        .ok_or_else(|| Error::NotFound(variant.into()))?;
    let template = revision
        .arguments
        .ok_or_else(|| Error::Invalid(format!("{variant} is not mapped")))?;
    Arguments::new(template, revision.refuse)?.adapt(&crate::tool::ToolInvocation {
        operation_id: crate::OperationId::from_bytes([7; 16]),
        call_id: "toolu_2".into(),
        name: variant.into(),
        arguments: call,
    })
}

fn decodes<T: serde::de::DeserializeOwned>(value: &Value) -> T {
    serde_json::from_value(value.clone()).expect("canonical input decodes")
}

#[test]
fn anthropic_calls_map_to_canonical_inputs() -> Result<()> {
    let anthropic = NativeToolProfile::Anthropic;
    let operation = crate::OperationId::from_bytes([7; 16]).to_string();
    for (variant, call, canonical) in [
        (
            "Read",
            json!({"file_path":"src/lib.rs","offset":3,"limit":2}),
            json!({"path":"src/lib.rs","selection":{"kind":"lines","offset":2,"limit":2}}),
        ),
        (
            "Read",
            json!({"file_path":"src/lib.rs"}),
            json!({"path":"src/lib.rs"}),
        ),
        (
            "Read",
            json!({"file_path":"src/lib.rs","limit":5}),
            json!({"path":"src/lib.rs","selection":{"kind":"lines","limit":5}}),
        ),
        (
            "Edit",
            json!({"file_path":"a.txt","old_string":"a","new_string":"b","replace_all":true}),
            json!({"path":"a.txt","old_text":"a","new_text":"b","replace_all":true}),
        ),
        (
            "NotebookEdit",
            json!({"notebook_path":"n.ipynb","cell_id":"c1","new_source":"x = 1"}),
            json!({"path":"n.ipynb","notebook":{"kind":"replace","cell":{"kind":"id","id":"c1"},"source":"x = 1"}}),
        ),
        (
            "NotebookEdit",
            json!({"notebook_path":"n.ipynb","cell_id":"c1","new_source":"# t","cell_type":"markdown","edit_mode":"insert"}),
            json!({"path":"n.ipynb","notebook":{"kind":"insert","cell":{"kind":"id","id":"c1"},"after":true,"id":operation,"cell_type":"markdown","source":"# t"}}),
        ),
        (
            "NotebookEdit",
            json!({"notebook_path":"n.ipynb","new_source":"x","cell_type":"code","edit_mode":"insert"}),
            json!({"path":"n.ipynb","notebook":{"kind":"insert","cell":{"kind":"index","index":0},"id":operation,"cell_type":"code","source":"x"}}),
        ),
        (
            "NotebookEdit",
            json!({"notebook_path":"n.ipynb","cell_id":"c1","new_source":"","edit_mode":"delete"}),
            json!({"path":"n.ipynb","notebook":{"kind":"delete","cell":{"kind":"id","id":"c1"}}}),
        ),
    ] {
        let adapted = adapt(anthropic, variant, call)?;
        assert_eq!(adapted, canonical, "{variant}");
        if variant == "Read" {
            let _: ReadFileInput = decodes(&adapted);
        } else {
            let _: EditFileInput = decodes(&adapted);
        }
    }
    let write = adapt(
        anthropic,
        "Write",
        json!({"file_path":"a.txt","content":"hi"}),
    )?;
    assert_eq!(decodes::<WriteFileInput>(&write).path, "a.txt");
    let shell = adapt(
        anthropic,
        "Bash",
        json!({"command":"ls","timeout":5000,"description":"list"}),
    )?;
    assert_eq!(shell, json!({"command":"ls","timeout_ms":5000}));
    assert_eq!(decodes::<ShellInput>(&shell).timeout_ms, Some(5000));
    Ok(())
}

#[test]
fn google_calls_map_to_canonical_inputs() -> Result<()> {
    let google = NativeToolProfile::Google;
    for (variant, call, canonical) in [
        (
            "read_file",
            json!({"file_path":"src/lib.rs","start_line":3,"end_line":4}),
            json!({"path":"src/lib.rs","selection":{"kind":"lines","offset":2,"limit":2}}),
        ),
        (
            "read_file",
            json!({"file_path":"src/lib.rs","end_line":4}),
            json!({"path":"src/lib.rs","selection":{"kind":"lines","limit":4}}),
        ),
        (
            "read_file",
            json!({"file_path":"src/lib.rs","start_line":1,"end_line":u64::MAX}),
            json!({"path":"src/lib.rs","selection":{"kind":"lines","offset":0,"limit":u64::MAX}}),
        ),
        (
            "read_file",
            json!({"file_path":"src/lib.rs","start_line":u64::MAX,"end_line":u64::MAX}),
            json!({"path":"src/lib.rs","selection":{"kind":"lines","offset":u64::MAX - 1,"limit":1}}),
        ),
        (
            "replace",
            json!({"file_path":"a.txt","instruction":"fix","old_string":"a","new_string":"b","allow_multiple":true}),
            json!({"path":"a.txt","old_text":"a","new_text":"b","replace_all":true}),
        ),
        (
            "write_file",
            json!({"file_path":"a.txt","content":"hi"}),
            json!({"path":"a.txt","content":"hi"}),
        ),
        (
            "run_shell_command",
            json!({"command":"ls","description":"list","dir_path":"src","is_background":false}),
            json!({"command":"ls","cwd":"src"}),
        ),
    ] {
        assert_eq!(adapt(google, variant, call)?, canonical, "{variant}");
    }
    for (start, end) in [(5, 4), (0, u64::MAX)] {
        assert!(matches!(
            adapt(
                google,
                "read_file",
                json!({"file_path":"a","start_line":start,"end_line":end})
            ),
            Err(Error::Invalid(_))
        ));
    }
    Ok(())
}

#[test]
fn refused_and_unmapped_arguments_never_reach_the_capability() -> Result<()> {
    for (profile, variant, call) in [
        (
            NativeToolProfile::Anthropic,
            "Bash",
            json!({"command":"sleep 9","run_in_background":true}),
        ),
        (
            NativeToolProfile::Anthropic,
            "Bash",
            json!({"command":"ls","dangerouslyDisableSandbox":true}),
        ),
        (
            NativeToolProfile::Anthropic,
            "Read",
            json!({"file_path":"a.pdf","pages":"1-2"}),
        ),
        (
            NativeToolProfile::Google,
            "run_shell_command",
            json!({"command":"ls","is_background":true}),
        ),
        (
            NativeToolProfile::Anthropic,
            "NotebookEdit",
            json!({"notebook_path":"n.ipynb","new_source":"x","edit_mode":"move"}),
        ),
    ] {
        assert!(adapt(profile, variant, call).is_err(), "{variant}");
    }
    // Declarations the layer does not map, and the whole Codex family until
    // its engine lands, stay unexposed.
    for (profile, variant) in [
        (NativeToolProfile::Anthropic, "Glob"),
        (NativeToolProfile::Anthropic, "Grep"),
        (NativeToolProfile::Google, "grep_search"),
    ] {
        assert!(adapt(profile, variant, json!({})).is_err(), "{variant}");
    }
    assert!(
        NativeToolProfile::Openai
            .tools()?
            .iter()
            .all(|revision| revision.arguments.is_none())
    );
    Ok(())
}

#[test]
fn malformed_templates_fail_when_the_profile_binds() {
    for template in [
        json!({"$one_based": 1}),
        json!({"$through": ["start"]}),
        json!({"$if": "flag"}),
        json!({"$unknown": "x"}),
        json!({"path": {"$case": "mode", "$other": 1}}),
    ] {
        assert!(
            Arguments::new(template.clone(), Vec::new()).is_err(),
            "{template}"
        );
    }
}
