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

/// The fixture tokenizer charges every encoded request byte and every selected
/// file byte once. The same accounting drives admission and the comparison.
fn input_cost(
    request: &crate::model::PreparedModelRequest,
) -> Result<crate::context::ModelTokenCount> {
    let fixed =
        u32::try_from(request.bytes().len()).map_err(|error| Error::Invalid(error.to_string()))?;
    let messages = request
        .request()
        .messages
        .iter()
        .map(|message| {
            message
                .content
                .file_parts()
                .try_fold(0_u32, |total, (file, _)| {
                    let bytes = u32::try_from(file.descriptor().byte_length())
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    total
                        .checked_add(bytes)
                        .ok_or_else(|| Error::Invalid("fixture cost overflow".into()))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(crate::context::ModelTokenCount::from_upper_bounds(
        request.request_digest(),
        fixed,
        messages,
    ))
}

#[tokio::test]
async fn managed_context_preserves_success_and_reduces_total_input_across_followups() -> Result<()>
{
    let mut totals = Vec::new();
    for managed in [false, true] {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let mut turns = vec![ScriptedTurn::Text("a".repeat(20_000))];
        if managed {
            turns.push(ScriptedTurn::Text(
                r#"{"version":1,"drop_through":2,"summary":"RETAINED_FACT_BLUE"}"#.into(),
            ));
        }
        turns.extend([
            ScriptedTurn::Text("SECOND_DONE".into()),
            ScriptedTurn::Text("THIRD_DONE".into()),
        ]);
        let provider = Arc::new(
            super::ScriptedModelProvider::new(turns)
                .with_context_capacity(crate::context::ModelContextCapacity {
                    context_tokens: 100_000,
                    output_tokens: Some(50_000),
                })
                .with_token_counter(input_cost)
                .reading_content(),
        );
        let policy = if managed {
            crate::context::CompactionPolicy::default()
        } else {
            crate::context::CompactionPolicy::Disabled
        };
        let harness = Arc::new(
            LocalHarness::open(
                volume_config(
                    directory.path(),
                    volume(directory.path()).await?,
                    provider.clone(),
                )?
                .without_default_tools()
                .compaction(policy),
            )
            .await?,
        );
        let mut client = ObservedConversation::open(&harness).await?;
        for (id, prompt) in [
            (
                "first",
                format!("RETAINED_FACT_BLUE {}", "o".repeat(20_000)),
            ),
            ("second", "c".repeat(15_000)),
            ("third", "Use the retained fact.".into()),
        ] {
            let entry = client.send(id, &prompt).await?;
            assert!(entry.outcome.is_completed(), "managed={managed}: {entry:?}");
        }
        let requests = provider.requests();
        assert_eq!(
            requests.iter().filter(|request| !request.summary).count(),
            3
        );
        assert_eq!(
            requests.iter().filter(|request| request.summary).count(),
            usize::from(managed)
        );
        let last = requests
            .last()
            .ok_or_else(|| Error::NotFound("final worker request".into()))?;
        let text = request_text(&harness, last).await?;
        assert!(
            text.contains("RETAINED_FACT_BLUE"),
            "managed={managed}: {text}"
        );
        assert_eq!(
            text.contains("oooooooooooooooo"),
            !managed,
            "rewrite was lost between turns"
        );
        let total = requests.into_iter().try_fold(0_u64, |total, request| {
            let prepared = crate::model::PreparedModelRequest::prepare(
                request,
                crate::Limits::default(),
                None,
            )?;
            let count = input_cost(&prepared)?;
            let tokens = count.validate(&prepared, &mut None)?;
            total
                .checked_add(tokens)
                .ok_or_else(|| Error::Invalid("fixture total overflow".into()))
        })?;
        totals.push(total);
    }
    println!(
        "input tokens: unmanaged={} managed={} (including manager)",
        totals[0], totals[1]
    );
    assert!(
        totals[1] < totals[0],
        "context management did not amortize its own input cost: {totals:?}"
    );
    Ok(())
}
