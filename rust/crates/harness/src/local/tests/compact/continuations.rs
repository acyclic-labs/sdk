use super::*;
use crate::task_volume::{FileKey, Query};
use proptest::prelude::*;

#[tokio::test]
async fn completed_continuation_requests_retain_every_intervening_prompt_and_answer() -> Result<()>
{
    // Independent native fixtures share the crash-cut worker pool. Each runner
    // retains the full default shrinking budget and one generated sequence.
    for result in
        futures::future::try_join_all((0..16).map(|_| WORKERS.spawn_blocking(sequence_case)))
            .await
            .map_err(super::super::super::storage)?
    {
        result?;
    }
    Ok(())
}

fn sequence_case() -> Result<()> {
    let cases = proptest::test_runner::Config {
        cases: 1,
        max_shrink_iters: 1024,
        ..Default::default()
    };
    proptest::test_runner::TestRunner::new(cases)
        .run(
            &("[A-Z]{1,12}", prop::collection::vec("[a-z]{1,12}", 3..6)),
            |(fact, prompts)| {
                WORKERS.block_on(async {
                    let directory = tempfile::tempdir()?;
                    let project = volume(directory.path()).await?;
                    let mut turns = vec![
                        ScriptedTurn::Text("original answer".into()),
                        ScriptedTurn::Text(format!("SUMMARY_ONLY_{fact}")),
                    ];
                    turns.extend(
                        (0..prompts.len())
                            .map(|i| ScriptedTurn::Text(format!("ANSWER_{i}_{fact}"))),
                    );
                    turns.push(ScriptedTurn::Text("broken answer".into()));
                    let provider = scripted(turns);
                    // Conversation retention needs no host tools or OS mounts.
                    // Keep the real admission/executor path, with only the model.
                    let config = || {
                        volume_config(directory.path(), project.clone(), provider.clone())
                            .map(|config| config.without_default_tools())
                    };
                    let mut local = LocalHarness::open(config()?).await?;
                    let mut parent = admitted(local.submit(OperationId::new(), "original").await?);
                    local.resume(parent).await?;
                    local
                        .compact(OperationId::new(), parent, "retain fact")
                        .await?;
                    let (_, checkpoint) = local.selected_checkpoint(parent).await?.unwrap();
                    let detector = |text: &str, through: usize| {
                        text.contains(&format!("SUMMARY_ONLY_{fact}"))
                            && prompts.iter().take(through).enumerate().all(|(i, prompt)| {
                                text.contains(prompt)
                                    && text.contains(&format!("ANSWER_{i}_{fact}"))
                            })
                    };
                    for (index, prompt) in prompts.iter().enumerate() {
                        let task = admitted(
                            local
                                .submit_to(OperationId::new(), Some(parent), prompt)
                                .await?,
                        );
                        local.resume(task).await?;
                        let request = &provider.requests()[index + 2];
                        let text = serde_json::to_string(request)?;
                        prop_assert!(
                            detector(&text, index),
                            "missing history at prompt {}: {}",
                            index,
                            text
                        );
                        prop_assert!(text.contains(prompt));
                        // Inheritance metadata contains no copied summary-sized payload.
                        let key = FileKey::parse(&format!(
                            ".tasks/{task}/context/command-inherited-summary/manifest.json"
                        ))?;
                        let inherited = local
                            .task_files
                            .inner
                            .snapshot(&Query::File(key), 1)
                            .await?;
                        prop_assert_eq!(inherited.files[0].bytes.as_ref(), b"null");
                        parent = task;
                        // Discard every in-memory binding between completed prompts.
                        local.close().await?;
                        local = LocalHarness::open(config()?).await?;
                    }
                    // Reproduce the old path through the same stock execution: only the
                    // checkpoint summary is included when admitting another root.
                    let operation = OperationId::new();
                    let task = crate::runtime::task_id_for_admission(operation);
                    local.prepare_inheritance(task, None).await?;
                    let broken = format!(
                        "Previous compacted conversation:\n{}\nCurrent request:\nbroken prompt",
                        local.summary_text(&checkpoint).await?
                    );
                    let (receive, model) = local
                        .stage_bound_turn(
                            operation,
                            &broken,
                            checkpoint.boundary.manifest,
                            local.input_publisher()?.as_ref(),
                        )
                        .await?;
                    local.admit_turn(operation, None, receive, model).await?;
                    local.resume(task).await?;
                    let text = serde_json::to_string(provider.requests().last().unwrap())?;
                    let positive = serde_json::to_string(&provider.requests()[prompts.len() + 1])?;
                    prop_assert!(detector(&positive, prompts.len() - 1));
                    prop_assert!(
                        !detector(&text, prompts.len() - 1),
                        "old summary-only continuation evaded the request detector"
                    );
                    local.close().await?;
                    Ok(())
                })
            },
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}
