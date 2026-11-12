use super::{
    ObservedConversation, ScriptedTurn, request_text, scripted, settings, volume, volume_config,
};
use crate::{Error, LocalHarness, Result};
use std::sync::Arc;

#[tokio::test]
async fn notes_and_nested_pins_append_across_five_turns_without_changing_the_frozen_prefix()
-> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = scripted((0..5).map(|_| ScriptedTurn::Text("done".into())));
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(
                directory.path(),
                volume(directory.path()).await?,
                provider.clone(),
            )?
            .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let mut client = ObservedConversation::open(&harness).await?;
    let mut notes = String::new();
    let mut frozen: Option<crate::model::ModelRequest> = None;
    for turn in 0..5 {
        notes.push_str(&format!("NOTE_{turn}\n"));
        settings::write(&harness, "context/notes.md", &notes).await?;
        settings::write(
            &harness,
            &format!("context/pins/evidence/{turn}.md"),
            &format!("PIN_{turn}\n"),
        )
        .await?;
        let entry = client
            .send(
                &format!("turn-{turn}"),
                "Continue using the working evidence.",
            )
            .await?;
        assert!(entry.outcome.is_completed(), "{entry:?}");
        let request = provider
            .request(turn)
            .ok_or_else(|| Error::NotFound("worker request".into()))?;
        let text = request_text(&harness, &request).await?;
        for index in 0..=turn {
            assert!(text.contains(&format!("NOTE_{index}")), "{text}");
            assert!(text.contains(&format!("PIN_{index}")), "{text}");
        }
        if let Some(original) = &frozen {
            let prefix_length = original
                .messages
                .len()
                .checked_sub(1)
                .ok_or_else(|| Error::Invalid("worker request is empty".into()))?;
            assert!(
                request
                    .messages
                    .starts_with(&original.messages[..prefix_length]),
                "working document edit changed the frozen prefix"
            );
            assert_eq!(request.tools, original.tools);
        } else {
            frozen = Some(request);
        }
    }
    assert_eq!(
        provider.request_count(),
        5,
        "notes and pins must not create worker turns"
    );
    Ok(())
}
