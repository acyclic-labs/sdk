//! Configured volumes use the same persistent view and stock coding tools.
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget};

#[tokio::test]
async fn configured_volume_binds_bash_and_fs_and_reopens_the_selected_branch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    {
        let files = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
            directory.path().join("filesystem"),
        ))
        .await
        .map_err(super::super::storage)?;
        let workspace = files
            .open_workspace(
                &project.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(super::super::storage)?
            .value;
        workspace
            .write_text("/existing.txt", "preexisting")
            .await
            .map_err(super::super::storage)?;
    }
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "write".into(),
                "write".into(),
                json!({"path":"note.txt","text":"volume","media_type":"text/plain","display_name":"note.txt"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        super::checked_commands::call(
            1,
            "bash",
            "cat existing.txt note.txt; printf shell > shell.txt",
        ),
        super::checked_commands::call(2, "fs", "commit -m volume"),
        super::checked_commands::call(3, "fs", "checkout -b agents/child"),
        super::checked_commands::call(4, "bash", "printf child > note.txt"),
        ScriptedTurn::Text("done".into()),
        super::checked_commands::call(5, "fs", "status"),
        super::checked_commands::call(6, "bash", "cat note.txt"),
        super::checked_commands::call(7, "fs", "checkout main"),
        super::checked_commands::call(8, "bash", "cat note.txt shell.txt"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project.clone(), provider.clone())?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    super::checkout_reopen::run(&harness).await?;
    let checkout = harness.workspace().unwrap();
    let head = checkout
        .view()
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(
        head.read(
            "/note.txt",
            64,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map_err(super::super::storage)?
        .value
        .as_ref(),
        b"volume"
    );
    assert_eq!(
        head.read(
            "/shell.txt",
            64,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map_err(super::super::storage)?
        .value
        .as_ref(),
        b"shell"
    );
    let view = checkout.path.clone();
    drop(head);
    drop(harness);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    assert_eq!(harness.workspace().unwrap().path, view);
    super::checkout_reopen::run(&harness).await?;
    assert_eq!(
        std::fs::read(view.join("note.txt")).map_err(super::super::storage)?,
        b"volume"
    );
    let requests = provider.requests();
    for (id, expected) in [
        ("checkout-1", "preexistingvolume"),
        ("checkout-6", "child"),
        ("checkout-8", "volumeshell"),
    ] {
        let content = requests
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                crate::model::ModelContentPart::ToolResult {
                    call_id, content, ..
                } if call_id == id => Some(content),
                _ => None,
            })
            .expect("accepted bash result");
        let encoded = serde_json::to_string(content).map_err(super::super::storage)?;
        assert!(encoded.contains(expected), "{id}: {encoded}");
        assert!(!encoded.contains("\\\"error\\\""), "{id}: {encoded}");
    }
    Ok(())
}
