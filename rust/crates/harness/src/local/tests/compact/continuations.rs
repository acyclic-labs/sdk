use super::*;
use crate::task_volume::{FileKey, Query};
use proptest::prelude::*;

#[tokio::test]
async fn completed_continuation_requests_retain_every_intervening_prompt_and_answer() -> Result<()>
{
    // Independent native fixtures share the crash-cut worker pool. Each runner
    // retains the full default shrinking budget and one generated sequence.
    for result in futures::future::try_join_all(
        (0..16).map(|case| WORKERS.spawn_blocking(move || sequence_case(case % 2 == 0))),
    )
    .await
    .map_err(super::super::super::storage)?
    {
        result?;
    }
    Ok(())
}

fn sequence_case(frozen: bool) -> Result<()> {
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
                    let summary = ScriptedTurn::Text(format!("SUMMARY_ONLY_{fact}"));
                    let answer = ScriptedTurn::Text(format!("DISCARDED_{fact}"));
                    let mut turns = if frozen {
                        vec![summary, answer]
                    } else {
                        vec![answer, summary]
                    };
                    turns.extend(
                        (0..prompts.len())
                            .map(|i| ScriptedTurn::Text(format!("ANSWER_{i}_{fact}"))),
                    );
                    turns.push(ScriptedTurn::Text("broken answer".into()));
                    if frozen {
                        turns.push(ScriptedTurn::Text("unfrozen answer".into()));
                    }
                    let provider = scripted(turns);
                    // Conversation retention needs no host tools or OS mounts.
                    // Keep the real admission/executor path, with only the model.
                    let config = || {
                        volume_config(directory.path(), project.clone(), provider.clone())
                            .map(|config| config.without_default_tools())
                    };
                    let mut local = LocalHarness::open(config()?).await?;
                    let mut parent = admitted(local.submit(OperationId::new(), "original").await?);
                    if !frozen {
                        local.resume(parent).await?;
                    }
                    local
                        .compact(OperationId::new(), parent, "retain fact")
                        .await?;
                    let (mut selection, checkpoint) =
                        local.selected_checkpoint(parent).await?.unwrap();
                    if frozen {
                        // Compact an unfinished task, let its answer arrive, then rewind
                        // through the ordinary file-backed selection before continuing.
                        local.resume(parent).await?;
                        selection.through = Some(checkpoint.boundary.through.clone());
                        selection.selected_at = local.task_files.inner.head().await?.position;
                        local
                            .task_files
                            .inner
                            .append(
                                local.task_files.inner.head().await?,
                                &crate::task_volume::FileValue::new(
                                    super::super::super::commands::compact_key(parent)?,
                                    serde_json::to_vec(&selection)?,
                                ),
                            )
                            .await?;
                    }
                    let detector = |text: &str, through: usize| {
                        text.contains(&format!("SUMMARY_ONLY_{fact}"))
                            && (!frozen || !text.contains(&format!("DISCARDED_{fact}")))
                            && !text.contains(&format!("RETRY_CHANGED_{fact}"))
                            && prompts.iter().take(through).enumerate().all(|(i, prompt)| {
                                text.contains(prompt)
                                    && text.contains(&format!("ANSWER_{i}_{fact}"))
                            })
                    };
                    for (index, prompt) in prompts.iter().enumerate() {
                        let operation = OperationId::new();
                        if index == 0 {
                            // Crash after retaining the exact question, before staging
                            // its command. A changed retry must use that saved question.
                            let (cut, selected, _) =
                                super::super::super::compact::checkpoints::capture_selection(
                                    &local.task_files.inner,
                                    parent,
                                    local.limits.file_bytes,
                                )
                                .await?;
                            let selected = selected.unwrap();
                            let through = selected.through.clone().unwrap_or(cut.cursor);
                            local
                                .retain_continuation(
                                    crate::runtime::task_id_for_admission(operation),
                                    parent,
                                    selected,
                                    through,
                                    prompt,
                                )
                                .await?;
                            local.close().await?;
                            local = LocalHarness::open(config()?).await?;
                        }
                        let retry = if index == 0 {
                            format!("RETRY_CHANGED_{fact}")
                        } else {
                            prompt.clone()
                        };
                        let task =
                            admitted(local.submit_to(operation, Some(parent), &retry).await?);
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
                            checkpoint.boundary.manifest.clone(),
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
                    if frozen {
                        // Omit the frozen-cut check in the real reconstruction path,
                        // then feed its result through the same provider-request detector.
                        let (selected, cp) = local.selected_checkpoint(parent).await?.unwrap();
                        let broken = local.retained_text::<false>(&selected, &cp).await?;
                        prop_assert!(
                            broken.contains(&format!("DISCARDED_{fact}")),
                            "negative control did not restore the discarded outcome"
                        );
                        let operation = OperationId::new();
                        let task = crate::runtime::task_id_for_admission(operation);
                        local.prepare_inheritance(task, None).await?;
                        let (receive, model) = local
                            .stage_bound_turn(
                                operation,
                                &format!("{broken}\nCurrent request: omitted rewind fence"),
                                checkpoint.boundary.manifest,
                                local.input_publisher()?.as_ref(),
                            )
                            .await?;
                        local.admit_turn(operation, None, receive, model).await?;
                        local.resume(task).await?;
                        let text = serde_json::to_string(provider.requests().last().unwrap())?;
                        prop_assert!(
                            !detector(&text, prompts.len() - 1),
                            "omitted frozen-cut check escaped detection"
                        );
                    }
                    local.close().await?;
                    Ok(())
                })
            },
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}

#[tokio::test]
async fn active_child_inherits_only_checkpoint_references_and_the_pinned_model() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let summary = format!("CHECKPOINT_ONLY_A_{}", "fact ".repeat(1024));
    let provider = scripted([
        ScriptedTurn::Text(summary.clone()),
        ScriptedTurn::Text("CHECKPOINT_ONLY_B".into()),
        ScriptedTurn::Text("child answer".into()),
    ]);
    let config = || {
        volume_config(directory.path(), project.clone(), provider.clone())
            .map(|config| config.without_default_tools())
    };
    let local = LocalHarness::open(config()?).await?;
    let parent = admitted(
        local
            .submit(OperationId::new(), "unfinished parent")
            .await?,
    );
    local
        .compact(OperationId::new(), parent, "retain A")
        .await?;
    let (selected, checkpoint) = local.selected_checkpoint(parent).await?.unwrap();
    let child = admitted(
        local
            .submit_to(OperationId::new(), Some(parent), "child request")
            .await?,
    );
    assert_eq!(
        local
            .runtime
            .task_host()
            .observe_admission(child)
            .await?
            .parent,
        Some(parent)
    );
    let key = FileKey::parse(&format!(
        ".tasks/{child}/context/command-inherited-summary/manifest.json"
    ))
    .map_err(super::super::super::storage)?;
    let inherited = local
        .task_files
        .inner
        .snapshot(&Query::File(key), 1)
        .await
        .map_err(super::super::super::storage)?;
    let detector = |bytes: &[u8]| -> bool {
        serde_json::from_slice::<crate::context::SummarySelection>(bytes)
            .is_ok_and(|value| value.checkpoint == selected.checkpoint && value.through.is_some())
            && !String::from_utf8_lossy(bytes).contains("CHECKPOINT_ONLY_A")
    };
    assert!(detector(&inherited.files[0].bytes));
    assert!(
        !detector(&serde_json::to_vec(&summary).map_err(super::super::super::storage)?),
        "the former summary-payload admission escaped the same metadata detector"
    );
    assert_eq!(
        local.resolved_value(child, "model").await?.as_deref(),
        Some(checkpoint.boundary.model.as_str())
    );
    // A later compaction changes the parent's selection, not its admitted child.
    local
        .compact(OperationId::new(), parent, "retain B")
        .await?;
    local.close().await?;
    let local = LocalHarness::open(config()?).await?;
    assert_eq!(local.resume(child).await?.text, "child answer");
    let request =
        serde_json::to_string(&provider.requests()[2]).map_err(super::super::super::storage)?;
    assert!(request.contains("CHECKPOINT_ONLY_A"));
    assert!(!request.contains("CHECKPOINT_ONLY_B"));
    assert_eq!(provider.requests()[2].model.name, checkpoint.boundary.model);
    local.close().await?;
    Ok(())
}

#[tokio::test]
async fn completed_turns_continue_one_conversation_without_compaction() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted(
        ["FIRST_ANSWER", "SECOND_ANSWER", "THIRD_ANSWER"]
            .map(|text| ScriptedTurn::Text(text.into())),
    );
    let config = || {
        volume_config(directory.path(), project.clone(), provider.clone())
            .map(|config| config.without_default_tools())
    };
    let mut local = LocalHarness::open(config()?).await?;
    let first = admitted(local.submit(OperationId::new(), "FIRST_PROMPT").await?);
    local.resume(first).await?;
    // Negative control: nothing selects the completed turn's history, the
    // state in which the scheduler refused it as a terminal parent.
    assert!(local.selected_checkpoint(first).await?.is_none());
    let second = OperationId::new();
    let continued = admitted(
        local
            .submit_to(second, Some(first), "SECOND_PROMPT")
            .await?,
    );
    // A retry after reopening reuses the retained checkpoint and admission.
    local.close().await?;
    local = LocalHarness::open(config()?).await?;
    assert_eq!(
        admitted(
            local
                .submit_to(second, Some(first), "SECOND_PROMPT")
                .await?
        ),
        continued
    );
    assert_eq!(
        local
            .runtime
            .task_host()
            .observe_admission(continued)
            .await?
            .parent,
        None,
        "a completed turn was parented"
    );
    local.resume(continued).await?;
    // The interface selects each new turn, so the third prompt continues the second.
    let third = admitted(
        local
            .submit_to(OperationId::new(), Some(continued), "THIRD_PROMPT")
            .await?,
    );
    assert_eq!(local.resume(third).await?.text, "THIRD_ANSWER");
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    let history = |index: usize| serde_json::to_string(&requests[index]).unwrap();
    for (index, retained) in [
        &["FIRST_PROMPT", "FIRST_ANSWER", "SECOND_PROMPT"][..],
        &[
            "FIRST_PROMPT",
            "FIRST_ANSWER",
            "SECOND_PROMPT",
            "SECOND_ANSWER",
            "THIRD_PROMPT",
        ],
    ]
    .into_iter()
    .enumerate()
    {
        for text in retained {
            assert!(
                history(index + 1).contains(text),
                "request {} lacks {text}",
                index + 1
            );
        }
    }
    Ok(())
}
