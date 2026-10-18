use super::*;

fn guidance(request: &crate::model::ModelRequest) -> (bool, bool) {
    let has = |prefix: &str| {
        request.messages.iter().any(|message| {
            message.role == crate::model::ModelRole::System
                && matches!(&message.content, crate::model::ModelContent::Text(text)
                    if text.starts_with(prefix))
        })
    };
    (has("Keep the current goal"), has("Keep each Markdown plan"))
}

fn prompt(goal: bool, plans: bool) -> &'static str {
    match (goal, plans) {
        (false, false) => "hello",
        (true, false) => "goal mode",
        (false, true) => "plan mode",
        (true, true) => "goal mode and plan mode",
    }
}

// Exhaust the independent requested/present booleans. The predecessor carries
// the opposite guidance, including mode markers in its retained history.
#[tokio::test]
async fn new_conversation_admissions_refresh_guidance_but_retries_keep_their_selection()
-> Result<()> {
    futures::future::try_join_all((0..16).map(case)).await?;
    Ok(())
}

async fn case(bits: u8) -> Result<()> {
    let requested = (bits & 1 != 0, bits & 2 != 0);
    let present = (bits & 4 != 0, bits & 8 != 0);
    let expected = (requested.0 || present.0, requested.1 || present.1);
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted((0..4).map(|_| ScriptedTurn::Text("answer".into())));
    let config = || {
        Ok::<_, Error>(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .without_default_tools()
                .goal_continuations(None),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    let first = OperationId::new();
    let parent = admitted(
        harness
            .submit(first, prompt(!expected.0, !expected.1))
            .await?,
    )?;
    harness.resume(parent).await?;
    let old_manifest = manifest(&harness, first).await?.0;
    // Cover both an ordinary completed conversation and an explicit compaction.
    let index = if bits & 1 == 0 {
        harness
            .compact(OperationId::new(), parent, "retain history")
            .await?;
        2
    } else {
        1
    };
    let publisher = FilesystemContentPublisher::new(
        harness.files.clone(),
        harness.scratch(),
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?;
    for (enabled, path, body) in [
        (present.0, "GOAL.md", "---\nstatus: done\n---\nShip"),
        (present.1, "plans/release.md", "- [ ] Ship"),
    ] {
        if enabled {
            publisher
                .stage(
                    OperationId::new(),
                    path,
                    body.as_bytes(),
                    "text/markdown",
                    "document",
                    None,
                )?
                .await?;
        }
    }
    drop(publisher);
    let operation = OperationId::new();
    let task = admitted(
        harness
            .submit_to(operation, Some(parent), prompt(requested.0, requested.1))
            .await?,
    )?;
    harness.resume(task).await?;
    assert_eq!(
        guidance(&provider.requests()[index]),
        expected,
        "case {bits}"
    );
    // Reopen and change the retry's mode: its already staged selection is pinned.
    drop(harness);
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(
        admitted(
            harness
                .submit_to(operation, Some(parent), prompt(!expected.0, !expected.1))
                .await?
        )?,
        task
    );
    harness.resume(task).await?;
    assert_eq!(provider.requests().len(), index + 1);
    // Negative control executes the old checkpoint-manifest staging path through
    // the same stock executor. Its model request must fail the same detector.
    let broken = OperationId::new();
    let broken_task = crate::runtime::task_id_for_admission(broken);
    harness.prepare_inheritance(broken_task, None).await?;
    let (receive, model) = harness
        .stage_bound_turn(
            broken,
            prompt(requested.0, requested.1),
            old_manifest,
            harness.input_publisher()?.as_ref(),
        )
        .await?;
    harness.admit_turn(broken, None, receive, model).await?;
    harness.resume(broken_task).await?;
    assert_ne!(
        guidance(provider.requests().last().unwrap()),
        expected,
        "old route escaped case {bits}"
    );
    Ok(())
}
