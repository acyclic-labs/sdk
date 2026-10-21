//! Write-tool bookkeeping stays outside every Git view of the workspace.
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget};

#[tokio::test]
async fn write_tool_metadata_never_reaches_a_git_command() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let write = |index: usize, text: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                format!("checkout-{index}"),
                "write".into(),
                json!({"path":"seed.txt","text":text,"media_type":"text/plain","display_name":"seed.txt"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ])
    };
    let fs = |index: usize, command: &str| super::checked_commands::call(index, "fs", command);
    let provider = scripted([
        write(0, "first\n"),
        fs(1, "add seed.txt"),
        fs(2, "commit -m first"),
        fs(3, "ls-files"),
        fs(4, "show"),
        fs(5, "archive HEAD"),
        // The media type is recorded only in the write tool's metadata.
        fs(6, "grep text/plain"),
        write(7, "second\n"),
        fs(8, "diff"),
        fs(9, "clean -n"),
        fs(10, "archive"),
        fs(11, "commit -m second"),
        fs(12, "show"),
        fs(13, "status"),
        write(14, "third\n"),
        // Stash rewinds workspace content, never the host's own state.
        fs(15, "stash"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path())
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) = harness
        .submit(OperationId::new(), "write and inspect history")
        .await?
    else {
        panic!("durable admission")
    };
    let commands = harness.commands()?;
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        harness.advance(task_id, &super::checked_commands::Checked(&commands)),
    )
    .await
    .map_err(|_| Error::Conflict("the scenario stalled".into()))??;
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
            } => Some((call_id.clone(), serde_json::to_string(content).unwrap())),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 16, "{results:#?}");
    for (call, result) in &results {
        assert!(
            !result.contains("\"error\"") && !result.contains(".system"),
            "{call}: {result}"
        );
    }
    for listing in ["checkout-3", "checkout-4", "checkout-5", "checkout-10"] {
        assert!(
            results[listing].contains("seed.txt"),
            "{}",
            results[listing]
        );
    }
    // Only the edited file differs; its rewritten metadata does not count.
    assert!(
        results["checkout-8"].contains(r#""fileChanges":1"#),
        "{}",
        results["checkout-8"]
    );
    assert!(
        results["checkout-12"].contains("second"),
        "{}",
        results["checkout-12"]
    );
    let checkout = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view();
    let stop = CancellationToken::new();
    let head = checkout
        .workspace()
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .map_err(super::super::storage)?
        .value;
    let seed = head
        .read("/seed.txt", 64, WorkBudget::UNBOUNDED, &stop)
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(seed.as_ref(), b"second\n");
    let receipts = head
        .list_directory(
            "/.system/harness-uploads",
            None,
            64,
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .map_err(super::super::storage)?
        .value
        .0
        .entries
        .len();
    assert_eq!(receipts, 3, "every write keeps its upload receipt");
    Ok(())
}
