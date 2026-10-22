//! A conflicted `fs merge` recovers through `--continue` and `--abort` on
//! every host, with the host's own shell editing the projected checkout.
use super::*;

/// The platform shell call that replaces `path` with one line of `text`.
fn write(index: usize, path: &str, text: &str) -> ScriptedTurn {
    let (tool, command) = if cfg!(windows) {
        (
            "powershell",
            format!("Set-Content -LiteralPath {path} -Value '{text}'"),
        )
    } else {
        ("bash", format!("printf '{text}\\n' > {path}"))
    };
    super::checked_commands::call(index, tool, &command)
}

async fn recover(continues: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let fs = |index, command| super::checked_commands::call(index, "fs", command);
    let mut turns = vec![
        write(0, "calc.py", "base"),
        fs(1, "commit -m base"),
        fs(2, "branch topic"),
        fs(3, "switch topic"),
        write(4, "calc.py", "theirs"),
        fs(5, "commit -m theirs"),
        fs(6, "switch main"),
        write(7, "calc.py", "ours"),
        fs(8, "commit -m ours"),
        fs(9, "merge topic"),
    ];
    if continues {
        turns.extend([write(10, "calc.py", "resolved"), fs(11, "merge --continue")]);
    } else {
        turns.push(fs(10, "merge --abort"));
    }
    turns.push(ScriptedTurn::Text("done".into()));
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
    harness
        .advance(task_id, &super::checked_commands::Checked(&commands))
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
    let merge = &results["checkout-9"];
    assert!(
        merge.contains("conflict") && !merge.contains("uncertain"),
        "{merge}"
    );
    let recovery = if continues {
        "checkout-11"
    } else {
        "checkout-10"
    };
    for (call, result) in &results {
        let expected_error = *call == "checkout-9";
        assert_eq!(
            result.contains("\"error\""),
            expected_error,
            "{call}: {result}"
        );
    }
    assert!(
        !results[recovery].contains("error"),
        "{}",
        results[recovery]
    );
    let text =
        std::fs::read_to_string(directory.path().join("calc.py")).map_err(super::super::storage)?;
    assert_eq!(text.trim_end(), if continues { "resolved" } else { "ours" });
    Ok(())
}

#[tokio::test]
async fn a_conflicted_merge_continues_with_the_host_shell_resolution() -> Result<()> {
    recover(true).await
}

#[tokio::test]
async fn a_conflicted_merge_aborts_to_the_branch_it_merged_into() -> Result<()> {
    recover(false).await
}
