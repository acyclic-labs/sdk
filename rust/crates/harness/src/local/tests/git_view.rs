//! Write-tool bookkeeping stays outside every Git view of the workspace.
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget};

#[tokio::test]
async fn write_tool_metadata_never_reaches_a_git_command() -> Result<()> {
    let provider = scripted([
        write(0, "seed.txt", "first\n"),
        fs(1, "add seed.txt"),
        fs(2, "commit -m first"),
        fs(3, "ls-files"),
        fs(4, "show"),
        fs(5, "archive HEAD"),
        // The media type is recorded only in the write tool's metadata.
        fs(6, "grep text/plain"),
        write(7, "seed.txt", "second\n"),
        fs(8, "diff"),
        fs(9, "clean -n"),
        fs(10, "archive"),
        fs(11, "commit -m second"),
        fs(12, "show"),
        fs(13, "status"),
        write(14, "seed.txt", "third\n"),
        // Stash rewinds workspace content, never the host's own state.
        fs(15, "stash"),
        ScriptedTurn::Text("done".into()),
    ]);
    let (harness, results, _directories) = scenario(provider).await?;
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
    assert_eq!(head_file(&harness, "/seed.txt").await?, b"second\n");
    assert_eq!(
        upload_receipts(&harness).await?,
        3,
        "every write keeps its upload receipt"
    );
    Ok(())
}

fn write(index: usize, path: &str, text: &str) -> ScriptedTurn {
    let name = path.rsplit('/').next().unwrap_or(path);
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
            format!("checkout-{index}"),
            "write".into(),
            json!({"path":path,"text":text,"media_type":"text/plain","display_name":name}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

fn fs(index: usize, command: &str) -> ScriptedTurn {
    super::checked_commands::call(index, "fs", command)
}

type ToolResults = std::collections::BTreeMap<String, String>;

/// Runs a scripted checkout task and returns each tool call's result.
async fn scenario(
    provider: Arc<ScriptedModelProvider>,
) -> Result<(LocalHarness, ToolResults, [tempfile::TempDir; 2])> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
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
    super::checked_commands::advance_until_idle(&harness, task_id, &provider).await?;
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
    Ok((harness, results, [directory, state]))
}

/// Runs `read` against the checkout's current head.
macro_rules! at_head {
    ($harness:expr, |$head:ident, $stop:ident| $read:expr) => {{
        let checkout = $harness
            .workspace()
            .ok_or_else(|| Error::NotFound("checkout".into()))?
            .view();
        let $stop = CancellationToken::new();
        let $head = checkout
            .workspace()
            .head(WorkBudget::UNBOUNDED, &$stop)
            .await
            .map_err(super::super::storage)?
            .value;
        $read.await.map_err(super::super::storage)?.value
    }};
}

/// Reads one file from the checkout's current head.
async fn head_file(harness: &LocalHarness, path: &str) -> Result<Vec<u8>> {
    Ok(at_head!(harness, |head, stop| head.read(
        path,
        64,
        WorkBudget::UNBOUNDED,
        &stop
    ))
    .as_ref()
    .to_vec())
}

/// Counts the write tool's upload receipts in the checkout's current head.
async fn upload_receipts(harness: &LocalHarness) -> Result<usize> {
    Ok(at_head!(harness, |head, stop| head.list_directory(
        "/.system/harness-uploads",
        None,
        64,
        WorkBudget::UNBOUNDED,
        &stop,
    ))
    .0
    .entries
    .len())
}

#[tokio::test]
async fn stash_rewinds_nested_workspace_content() -> Result<()> {
    let provider = scripted([
        write(0, "src/a.txt", "first\n"),
        fs(1, "add src/a.txt"),
        fs(2, "commit -m first"),
        write(3, "src/a.txt", "second\n"),
        write(4, "src/new/b.txt", "new\n"),
        fs(5, "stash"),
        ScriptedTurn::Text("done".into()),
    ]);
    let (harness, results, _directories) = scenario(provider).await?;
    assert_eq!(results.len(), 6, "{results:#?}");
    for (call, result) in &results {
        assert!(!result.contains("\"error\""), "{call}: {result}");
    }
    // The stash left exactly the committed tree behind, nested content
    // included.
    assert_eq!(head_file(&harness, "/src/a.txt").await?, b"first\n");
    assert!(
        head_file(&harness, "/src/new/b.txt").await.is_err(),
        "the stash removed the new nested file"
    );
    // The stash never rewound the host's own state.
    assert_eq!(upload_receipts(&harness).await?, 3);
    Ok(())
}
