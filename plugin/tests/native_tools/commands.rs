//! Probe the installed CLI, not a manually constructed task context.
use super::*;

pub(super) async fn native_shell_and_git_commands_use_the_fenced_owner(cwd: &Path) -> Result {
    std::fs::create_dir(cwd.join(".git"))?;
    std::fs::create_dir(cwd.join("nested"))?;
    let session = uuid::Uuid::new_v4().to_string();
    hook(
        cwd,
        "pi",
        "session_start",
        json!({"sessionId":session,"cwd":cwd}),
    )
    .await?;
    for (name, command) in [
        ("bash", "printf fenced-shell > shell.txt"),
        ("fs", "status"),
    ] {
        let answer = hook(
            cwd,
            "pi",
            "tool_call",
            json!({
                "sessionId":session,"cwd":cwd,"toolCallId":name,
                "toolName":name,"input":{"command":command},
            }),
        )
        .await?;
        assert_eq!(answer["isError"], false, "{name}: {answer}");
        assert!(answer["details"].is_object(), "{name}: {answer}");
    }
    assert_eq!(
        std::fs::read_to_string(cwd.join("shell.txt"))?,
        "fenced-shell"
    );
    let options = hook(
        cwd,
        "pi",
        "tool_call",
        json!({
            "sessionId":session,"cwd":cwd,"toolCallId":"bash-options","toolName":"bash",
            "input":{"command":"printf selected-cwd > relative.txt","cwd":"nested","timeout":30},
        }),
    )
    .await?;
    assert_eq!(options["isError"], false, "{options}");
    assert_eq!(
        std::fs::read_to_string(cwd.join("nested/relative.txt"))?,
        "selected-cwd"
    );
    assert!(!cwd.join("relative.txt").exists());
    hook(
        cwd,
        "pi",
        "session_shutdown",
        json!({"sessionId":session,"cwd":cwd}),
    )
    .await?;
    Ok(())
}
