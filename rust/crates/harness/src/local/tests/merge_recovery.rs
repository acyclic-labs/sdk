//! A conflicted `fs merge` recovers through `--continue` and `--abort` on
//! every host, with the host's own shell editing the projected checkout.
use super::*;

/// The platform shell call that replaces `path` with one line of `text`.
fn write(index: usize, path: &str, text: &str) -> ScriptedTurn {
    let (tool, command) = if cfg!(windows) {
        (
            "shell",
            format!("Set-Content -LiteralPath {path} -Value '{text}'"),
        )
    } else {
        ("shell", format!("printf '{text}\\n' > {path}"))
    };
    super::checked_commands::call(index, tool, &command)
}

async fn recover_both_paths() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let fs = |index, command| super::checked_commands::call(index, "fs", command);
    let turns = vec![
        write(0, "calc.py", "base"),
        fs(1, "commit -m base"),
        fs(2, "branch topic"),
        fs(3, "switch topic"),
        write(4, "calc.py", "theirs"),
        fs(5, "commit -m theirs"),
        fs(6, "switch main"),
        write(7, "calc.py", "ours"),
        fs(8, "commit -m ours"),
        // Continue in a child of the unchanged pre-merge main branch. The
        // abort path then selects that original root; no saved branch has a
        // parent advanced by the first merge's resolved commit.
        fs(9, "branch continuing"),
        fs(10, "switch continuing"),
        fs(11, "merge topic"),
        write(12, "calc.py", "resolved"),
        fs(13, "merge --continue"),
        fs(14, "show HEAD:calc.py"),
        fs(15, "switch main"),
        fs(16, "merge topic"),
        fs(17, "merge --abort"),
        ScriptedTurn::Text("done".into()),
    ];
    let provider = scripted(turns);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) = harness
        .submit(OperationId::new(), "merge topic and recover")
        .await?
    else {
        panic!("durable admission")
    };
    let commands = harness.commands()?;
    crate::package::harness::worker::advance(
        &harness,
        task_id,
        &super::checked_commands::Checked(&commands),
    )
    .await?;
    assert_eq!(harness.resume(task_id).await?.text, "done");
    let requests = provider.requests();
    let results: std::collections::BTreeMap<_, _> = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } => Some((call_id.as_str(), format!("{content:?}"))),
            _ => None,
        })
        .collect();
    for merge in ["checkout-11", "checkout-16"] {
        let result = &results[merge];
        assert!(
            result.contains("conflict") && !result.contains("uncertain"),
            "{merge}: {result}"
        );
    }
    for (call, result) in &results {
        let expected_error = matches!(*call, "checkout-11" | "checkout-16");
        assert_eq!(
            result.contains("\"error\""),
            expected_error,
            "{call}: {result}"
        );
    }
    for recovery in ["checkout-13", "checkout-17"] {
        assert!(
            !results[recovery].contains("error"),
            "{}",
            results[recovery]
        );
    }
    assert!(
        results["checkout-14"].contains("resolved"),
        "{}",
        results["checkout-14"]
    );
    let text =
        std::fs::read_to_string(directory.path().join("calc.py")).map_err(super::super::storage)?;
    assert_eq!(text.trim_end(), "ours");
    Ok(())
}

#[tokio::test]
async fn conflicted_merges_continue_and_abort_through_the_host_shell() -> Result<()> {
    recover_both_paths().await
}
