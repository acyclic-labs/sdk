//! Compaction, rewind and clear select the root's later model context as
//! checkpoints in its own conversation stream, durable across a reopen.

use super::{
    ObservedConversation, ScriptedModelProvider, ScriptedTurn, scripted, volume, volume_config,
};
use crate::{
    Error, LocalHarness, Result, agent::ContextAction, local::agent_conversation::checkpoint,
};
use std::{path::Path, sync::Arc};

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

async fn open(directory: &Path, provider: Arc<ScriptedModelProvider>) -> Result<Arc<LocalHarness>> {
    let project = volume(directory).await?;
    Ok(Arc::new(
        super::open_scripted(
            volume_config(directory, project, provider)?
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::default()),
        )
        .await?,
    ))
}

/// The `index`th model request and its retained text files.
async fn request(
    harness: &LocalHarness,
    provider: &ScriptedModelProvider,
    index: usize,
) -> Result<String> {
    let request = provider
        .request(index)
        .ok_or_else(|| Error::Conflict(format!("no model request {index}")))?;
    super::request_text(harness, &request).await
}

#[tokio::test]
async fn checkpoints_select_later_context_and_survive_a_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(failed)?;
    let provider = scripted(
        [
            "FIRST_REPLY",
            "CHECKPOINT_SUMMARY",
            "DISCARDED_REPLY",
            "KEPT_REPLY",
        ]
        .map(|text| ScriptedTurn::Text(text.into())),
    );
    let harness = open(directory.path(), provider.clone()).await?;
    let mut client = ObservedConversation::open(&harness).await?;
    let (_, root) = harness.agents().await?;
    let root = root.clone();
    let mut usage = client.conversation.usage_query();
    let first = client.send("first", "remember FIRST_FACT").await?;
    assert!(first.outcome.is_completed(), "{:?}", first.outcome);
    super::agents::settled(&harness, &root).await?;
    let content = super::journal_content(&harness, &root)?;
    let epoch = crate::context::ContextEpoch::operation(root.as_str(), None)?;
    let (manifest, _, _) = content
        .reader
        .read_private_path(
            &content.volume,
            "context/epochs",
            &crate::context::ContextEpoch::path(epoch),
            None,
            None,
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .ok_or_else(|| Error::NotFound("journal epoch manifest".into()))?;
    assert_eq!(manifest.volume(), &content.volume);
    assert!(matches!(
        harness.agents.get().unwrap().server().bound()
            .read_file(&root, &manifest, 0, manifest.descriptor().byte_length()).await,
        Err(crate::agent::Error::Turn(error)) if matches!(*error, Error::Unauthorized(_))
    ));
    drop(content);
    while !usage.synchronized() {
        usage.advance().await?;
    }
    assert_eq!(usage.totals().completed, 1);
    assert!(usage.context().is_some());
    let original_window = usage.context();
    let expected = client.conversation.project_heads().await?;
    let files_only = client
        .conversation
        .restore("files-only", 0, crate::agent::RestoreMode::Files, expected)
        .await?;
    client.settled(files_only).await?;
    super::agents::settled(&harness, &root).await?;
    usage.advance().await?;
    assert_eq!(
        usage.context(),
        original_window,
        "files-only restore changed model occupancy"
    );

    // Compaction is one model request over the whole current context.
    let inbox = client
        .conversation
        .compact("compact", Some("KEEP_GUIDANCE"))
        .await?;
    let (sequence, entry) = client.settled_at(inbox).await?;
    super::agents::settled(&harness, &root).await?;
    usage.advance().await?;
    assert_eq!(usage.totals().completed, 2);
    assert!(
        matches!(
            usage.context(),
            Some(crate::executor::ContextUsage::Unavailable)
        ),
        "manual compaction kept its source occupancy"
    );
    assert!(matches!(
        checkpoint(&entry),
        Some(ContextAction::Compact {
            instructions: Some(_)
        })
    ));
    let turn = client.conversation.turn(sequence, &entry).await?;
    assert_eq!(
        (turn.prompt, turn.answer.as_str()),
        (None, "CHECKPOINT_SUMMARY")
    );
    let compaction = request(&harness, &provider, 1).await?;
    for needle in [
        "Summarize this conversation",
        "KEEP_GUIDANCE",
        "FIRST_FACT",
        "FIRST_REPLY",
    ] {
        assert!(compaction.contains(needle), "{needle}: {compaction}");
    }

    // A later turn starts from the summary alone.
    client.send("discard", "DISCARD_PROMPT").await?;
    super::agents::settled(&harness, &root).await?;
    usage.advance().await?;
    assert_eq!(usage.totals().completed, 3);
    assert!(usage.context().is_some());
    let after = request(&harness, &provider, 2).await?;
    assert!(after.contains("CHECKPOINT_SUMMARY"), "{after}");
    assert!(!after.contains("FIRST_FACT") && !after.contains("FIRST_REPLY"));
    assert!(!after.contains("Summarize this conversation"), "{after}");

    // A rewind runs no model request and restores the checkpoint.
    let inbox = client.conversation.rewind("rewind").await?;
    let (sequence, entry) = client.settled_at(inbox).await?;
    assert_eq!(checkpoint(&entry), Some(&ContextAction::Rewind));
    super::agents::settled(&harness, &root).await?;
    usage.advance().await?;
    assert!(
        matches!(
            usage.context(),
            Some(crate::executor::ContextUsage::Unavailable)
        ),
        "rewind kept a discarded request's occupancy"
    );
    assert_eq!(provider.request_count(), 3);
    assert_eq!(
        client.conversation.turn(sequence, &entry).await?.answer,
        "CHECKPOINT_SUMMARY"
    );
    client.send("kept", "KEPT_PROMPT").await?;
    let rewound = request(&harness, &provider, 3).await?;
    assert!(rewound.contains("CHECKPOINT_SUMMARY"), "{rewound}");
    assert!(!rewound.contains("DISCARD_PROMPT") && !rewound.contains("DISCARDED_REPLY"));
    super::agents::settled(&harness, &root).await?;
    usage.advance().await?;
    assert_eq!(usage.totals().completed, 4);
    assert!(usage.context().is_some());
    drop(usage);
    drop(client);
    drop(harness);

    // The rewind is durable: the reopened conversation keeps its selection.
    let reopened =
        scripted(["REOPENED_REPLY", "FRESH_REPLY"].map(|text| ScriptedTurn::Text(text.into())));
    let harness = open(directory.path(), reopened.clone()).await?;
    let mut client = ObservedConversation::open(&harness).await?;
    client.send("reopened", "REOPENED_PROMPT").await?;
    let resumed = request(&harness, &reopened, 0).await?;
    for needle in ["CHECKPOINT_SUMMARY", "KEPT_PROMPT", "KEPT_REPLY"] {
        assert!(resumed.contains(needle), "{needle}: {resumed}");
    }
    assert!(!resumed.contains("DISCARD_PROMPT") && !resumed.contains("FIRST_FACT"));

    // Clearing retires the conversation: the next turn has no history.
    let inbox = client.conversation.clear_all("clear").await?;
    let entry = client.settled(inbox).await?;
    assert_eq!(checkpoint(&entry), Some(&ContextAction::Clear));
    assert_eq!(reopened.request_count(), 1);
    client.send("fresh", "FRESH_PROMPT").await?;
    let fresh = request(&harness, &reopened, 1).await?;
    assert!(fresh.contains("FRESH_PROMPT"), "{fresh}");
    for needle in ["CHECKPOINT_SUMMARY", "KEPT_REPLY", "REOPENED_REPLY"] {
        assert!(!fresh.contains(needle), "{needle}: {fresh}");
    }
    Ok(())
}

#[tokio::test]
async fn a_cancelled_turn_settles_durably_without_a_model_request() -> Result<()> {
    let directory = tempfile::tempdir().map_err(failed)?;
    let (dispatched, mut requests) = tokio::sync::mpsc::channel(1);
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![crate::test_support::ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("AFTER_REPLY".into()),
        ])
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| {
            let _ = dispatched.try_send(());
            Box::pin(async { Ok(()) })
        }),
    );
    let harness = open(directory.path(), provider.clone()).await?;
    let mut client = ObservedConversation::open(&harness).await?;
    let inbox = client.conversation.send("held", "CANCELLED_PROMPT").await?;
    // The turn is cancelled while its model request streams.
    tokio::select! {
        request = requests.recv() => request.ok_or_else(|| Error::Storage("model dispatch producer closed".into()))?,
        settled = client.settled(inbox) => {
            let entry = settled?;
            return Err(Error::Conflict(format!("held turn settled before model dispatch: {:?}", entry.outcome)));
        }
    }
    assert_eq!(client.conversation.cancel().await?, Some(inbox));
    let (sequence, entry) = client.settled_at(inbox).await?;
    assert_eq!(entry.outcome, crate::agent::TurnOutcome::Cancelled);
    assert_eq!(checkpoint(&entry), None);
    let prompt = client.conversation.turn(sequence, &entry).await?.prompt;
    assert_eq!(prompt.as_deref(), Some("CANCELLED_PROMPT"));
    assert_eq!(provider.request_count(), 1);
    // The cancelled prompt stays in the later context.
    client.send("after", "AFTER_PROMPT").await?;
    let after = request(&harness, &provider, 1).await?;
    assert!(after.contains("CANCELLED_PROMPT"), "{after}");
    Ok(())
}
