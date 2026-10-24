use super::*;
use crate::{
    model::{ModelContent, ModelRole},
    native_settings::SettingsConfig,
};

fn hook(text: &str) -> Value {
    let output = json!({"hookSpecificOutput":{"additionalContext":text}}).to_string();
    #[cfg(windows)]
    let (command, args): (&str, Vec<String>) = {
        use base64::Engine;
        let script: Vec<u8> = format!("Write-Output '{output}'")
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        (
            "powershell.exe",
            vec![
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-EncodedCommand".into(),
                base64::engine::general_purpose::STANDARD.encode(script),
            ],
        )
    };
    #[cfg(not(windows))]
    let (command, args): (&str, Vec<String>) = (
        "sh",
        vec![
            "-c".into(),
            format!("cat >/dev/null; printf '%s' '{output}'"),
        ],
    );
    json!({"type":"command", "command":command, "args":args, "onFailure":"block"})
}

#[tokio::test]
async fn prompt_hooks_follow_static_context_and_input_in_event_order() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = root.path().join("workspace");
    let home = root.path().join("home");
    std::fs::create_dir_all(&workspace).map_err(super::super::storage)?;
    std::fs::create_dir_all(home.join(".claude")).map_err(super::super::storage)?;
    std::fs::write(workspace.join("AGENTS.md"), "PINNED_STATIC_INSTRUCTIONS")
        .map_err(super::super::storage)?;
    std::fs::write(
        home.join(".claude/settings.json"),
        json!({"hooks":{
            "SessionStart":[{"matcher":"startup", "hooks":[hook("SESSION_CONTEXT")]}],
            "UserPromptSubmit":[{"hooks":[hook("PROMPT_CONTEXT")]}]
        }})
        .to_string(),
    )
    .map_err(super::super::storage)?;
    let provider = text();
    let config = checkout_config(&workspace, (model()?, provider.clone()))?
        .state_directory(root.path().join("state"))
        .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex")));
    let harness = LocalHarness::open(config).await?;
    let task =
        super::context::admitted(harness.submit(OperationId::new(), "CURRENT_INPUT").await?)?;
    assert_eq!(harness.resume(task).await?.text, "done");
    let requests = provider.requests();
    let request = requests
        .first()
        .ok_or_else(|| Error::Invalid("missing provider request".into()))?;
    let mut positions = Vec::new();
    for needle in [
        "AGENTS.md",
        "CURRENT_INPUT",
        "SESSION_CONTEXT",
        "PROMPT_CONTEXT",
    ] {
        let (position, message) = request
            .messages
            .iter()
            .enumerate()
            .find(|(_, message)| {
                matches!(&message.content, ModelContent::Text(text) if text.contains(needle))
                    || message
                        .content
                        .file_refs()
                        .any(|file| file.path().ends_with(needle))
            })
            .ok_or_else(|| Error::Invalid(format!("missing context: {needle}")))?;
        println!(
            "PROMPT_PREFIX needle={needle} position={position} role={:?}",
            message.role
        );
        if needle.ends_with("_CONTEXT") {
            assert_eq!(message.role, ModelRole::User, "late hook context: {needle}");
        }
        positions.push(position);
    }
    println!("PROMPT_PREFIX_POSITIONS {positions:?}");
    assert!(positions.windows(2).all(|pair| pair.first() < pair.get(1)));
    harness.close().await
}
