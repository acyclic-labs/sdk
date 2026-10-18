//! Volume-backed tools remain usable before a native projection exists.
use super::*;
use crate::model::ToolCall;
use acyclic_fs::{CancellationToken, WorkBudget};

fn write(index: usize, text: &str) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            format!("write-{index}"),
            "write".into(),
            json!({"path":".volumes/scratch/note", "text":text,
                "media_type":"text/plain", "display_name":"note"}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

#[tokio::test]
async fn logical_volume_writes_precede_one_native_projection_and_survive_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        write(0, "staged"),
        super::checked_commands::call(1, "fs", "status --volume scratch"),
        ScriptedTurn::Text("staged".into()),
        super::checked_commands::call(2, "bash", "cat .volumes/scratch/note"),
        write(3, "replaced"),
        super::checked_commands::call(
            4,
            "bash",
            "cat .volumes/scratch/note; printf persisted > .volumes/scratch/after",
        ),
        ScriptedTurn::Text("mounted".into()),
        super::checked_commands::call(
            5,
            "bash",
            "cat .volumes/scratch/note .volumes/scratch/after",
        ),
        ScriptedTurn::Text("reopened".into()),
    ]);
    let config = || {
        checkout_config(directory.path(), (model()?, provider.clone()))
            .map(|config| config.state_directory(state.path()))
    };
    let harness = LocalHarness::open(config()?).await?;
    let workspace = harness.workspace().unwrap();
    let scratch = workspace
        .mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .unwrap()
        .clone();
    assert!(scratch.native().await.is_none());
    assert!(!scratch.destination.exists());
    assert_eq!(
        harness.run("stage without native I/O").await?.text,
        "staged"
    );
    assert!(
        scratch.native().await.is_none(),
        "file tools eagerly mounted scratch"
    );
    assert!(!scratch.destination.exists());
    let head = scratch
        .workspace
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(super::super::storage)?
        .value;
    assert_eq!(
        head.read(
            "/note",
            32,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map_err(super::super::storage)?
        .value
        .as_ref(),
        b"staged"
    );
    assert_eq!(harness.run("use the native volume").await?.text, "mounted");
    assert!(
        scratch.native().await.is_none(),
        "bash retained its projection past its execution scope"
    );
    assert!(!scratch.destination.exists());
    let requests = provider.requests();
    for (call_id, expected) in [("checkout-2", "staged"), ("checkout-4", "replaced")] {
        let result = requests
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                crate::model::ModelContentPart::ToolResult {
                    call_id: id,
                    content: crate::model::ToolResultContent::Json { value },
                    ..
                } if id == call_id => Some(value),
                _ => None,
            })
            .expect("published bash result");
        assert_eq!(result["stdout"], expected, "{result}");
    }
    drop(head);
    drop(scratch);
    harness.close().await?;
    let harness = LocalHarness::open(config()?).await?;
    assert!(
        harness
            .workspace()
            .unwrap()
            .mounts
            .named(Some("scratch"))?
            .1
            .view
            .source
            .as_ref()
            .unwrap()
            .native()
            .await
            .is_none()
    );
    assert_eq!(
        harness.run("read the persisted native writes").await?.text,
        "reopened"
    );
    let requests = provider.requests();
    let output = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id,
                content: crate::model::ToolResultContent::Json { value },
                ..
            } if call_id == "checkout-5" => Some(value),
            _ => None,
        })
        .expect("reopened bash result");
    assert_eq!(output["stdout"], "replacedpersisted", "{output}");
    harness.close().await?;
    Ok(())
}
