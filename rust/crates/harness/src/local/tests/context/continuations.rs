use super::*;

fn guidance(request: &crate::model::ModelRequest) -> (bool, bool) {
    let has = |prefix: &str| {
        request.messages.iter().any(|message| {
            message.role == crate::model::ModelRole::User
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
    let bootstrap = Arc::new(super::super::bootstrap::Bootstrap::new().await?);
    // Six distinct durable predecessors cover all sixteen admissions. Copy only
    // closed immutable bytes; each branch owns its host, provider and ledgers.
    let keys = (0..16)
        .map(predecessor_key)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(keys.len(), 6);
    let mut predecessors = std::collections::BTreeMap::new();
    let keys = keys.into_iter().collect::<Vec<_>>();
    for batch in keys.chunks(4) {
        for result in futures::future::try_join_all(batch.iter().map(|&key| {
            let bootstrap = Arc::clone(&bootstrap);
            WORKERS.spawn(async move {
                predecessor(key, &bootstrap)
                    .await
                    .map(|value| (key, Arc::new(value)))
            })
        }))
        .await
        .map_err(super::super::super::storage)?
        {
            let (key, predecessor) = result?;
            assert!(predecessors.insert(key, predecessor).is_none());
        }
    }
    for first in (0..16).step_by(4) {
        for result in futures::future::try_join_all((first..first + 4).map(|bits| {
            let predecessor = Arc::clone(&predecessors[&predecessor_key(bits)]);
            WORKERS.spawn(async move { case(bits, &predecessor).await })
        }))
        .await
        .map_err(super::super::super::storage)?
        {
            result?;
        }
    }
    Ok(())
}

fn predecessor_key(bits: u8) -> u8 {
    let requested = (bits & 1 != 0, bits & 2 != 0);
    let present = (bits & 4 != 0, bits & 8 != 0);
    let expected = (requested.0 || present.0, requested.1 || present.1);
    u8::from(expected.0) | (u8::from(expected.1) << 1) | (u8::from(bits & 1 == 0) << 2)
}

struct Predecessor {
    bootstrap: super::super::bootstrap::Bootstrap,
    parent: crate::TaskId,
    stale_guidance: (bool, bool),
}

async fn predecessor(
    key: u8,
    bootstrap: &super::super::bootstrap::Bootstrap,
) -> Result<Predecessor> {
    let directory = bootstrap.restore()?;
    let provider = scripted((0..3).map(|_| ScriptedTurn::Text("answer".into())));
    let harness = LocalHarness::open(
        bootstrap
            .config(directory.path(), provider.clone())?
            .goal_continuations(None),
    )
    .await?;
    let first = OperationId::new();
    let parent = admitted(
        harness
            .submit(first, prompt(key & 1 == 0, key & 2 == 0))
            .await?,
    )?;
    harness.resume(parent).await?;
    let old_manifest = manifest(&harness, first).await?.0;
    // Cover both an ordinary completed conversation and an explicit compaction.
    if key & 4 != 0 {
        harness
            .compact(OperationId::new(), parent, "retain history")
            .await?;
    }
    // The old route depends on this immutable manifest, not the later
    // requested/present booleans. Execute its negative control once per exact
    // predecessor, then require every branch to reject the witnessed guidance.
    let expected = (key & 1 != 0, key & 2 != 0);
    let broken = OperationId::new();
    let broken_task = crate::runtime::task_id_for_admission(broken);
    harness.prepare_inheritance(broken_task, None).await?;
    let (receive, model) = harness
        .stage_bound_turn(
            broken,
            prompt(expected.0, expected.1),
            old_manifest,
            harness.input_publisher()?.as_ref(),
        )
        .await?;
    harness.admit_turn(broken, None, receive, model).await?;
    harness.resume(broken_task).await?;
    let stale_guidance = guidance(&provider.last_request().unwrap());
    assert_eq!(stale_guidance, (!expected.0, !expected.1));
    Ok(Predecessor {
        bootstrap: bootstrap.checkpoint(directory, harness).await?,
        parent,
        stale_guidance,
    })
}

async fn case(bits: u8, predecessor: &Predecessor) -> Result<()> {
    let requested = (bits & 1 != 0, bits & 2 != 0);
    let present = (bits & 4 != 0, bits & 8 != 0);
    let expected = (requested.0 || present.0, requested.1 || present.1);
    let bootstrap = &predecessor.bootstrap;
    let directory = bootstrap.restore()?;
    let provider = scripted([ScriptedTurn::Text("answer".into())]);
    let config = || {
        Ok::<_, Error>(
            bootstrap
                .config(directory.path(), provider.clone())?
                .goal_continuations(None),
        )
    };
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(harness.signed.agent(), bootstrap.agent);
    assert_eq!(provider.request_count(), 0);
    let parent = predecessor.parent;
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
                    crate::conversation::ContentAccess::default(),
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
        guidance(&provider.request(0).unwrap()),
        expected,
        "case {bits}"
    );
    // Reopen and change the retry's mode: its already staged selection is pinned.
    harness.close().await?;
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
    assert_eq!(provider.request_count(), 1);
    // All sixteen branches must reject their exact predecessor's witnessed
    // old route; constructing fewer witnesses cannot silently miss a branch.
    assert_eq!(predecessor.stale_guidance, (!expected.0, !expected.1));
    assert_ne!(
        predecessor.stale_guidance, expected,
        "old route escaped case {bits}"
    );
    harness.close().await?;
    Ok(())
}

// A child of a live compacted conversation receives the summary as prompt
// text. Markers in that summary are history, not this question's request.
#[tokio::test]
async fn child_guidance_ignores_compacted_summary_markers() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let summary = "Resume goal mode in GOAL.md and plan mode in plans/release.md";
    let provider = scripted([
        ScriptedTurn::Text(summary.into()),
        ScriptedTurn::Text("answer".into()),
        ScriptedTurn::Text("answer".into()),
    ]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?
            .without_default_tools()
            .goal_continuations(None),
    )
    .await?;
    // The parent stays live, so the next prompt routes as its child.
    let parent = admitted(harness.submit(OperationId::new(), "hello").await?)?;
    harness
        .compact(OperationId::new(), parent, "retain history")
        .await?;
    let child = admitted(
        harness
            .submit_to(OperationId::new(), Some(parent), "hello")
            .await?,
    )?;
    harness.resume(child).await?;
    let request = serde_json::to_string(&provider.request(1).unwrap())
        .map_err(super::super::super::storage)?;
    assert!(
        request.contains(summary),
        "child did not receive the summary"
    );
    assert_eq!(guidance(&provider.request(1).unwrap()), (false, false));
    // Negative control: the former route selected from the summary-prefixed
    // prompt through the same stock executor.
    let broken = OperationId::new();
    let broken_task = crate::runtime::task_id_for_admission(broken);
    harness.prepare_inheritance(broken_task, None).await?;
    let prompt = harness.prompt_context(Some(child), "hello").await?;
    let (receive, model) = harness.stage_turn(broken, &prompt).await?;
    harness.admit_turn(broken, None, receive, model).await?;
    harness.resume(broken_task).await?;
    assert_eq!(guidance(&provider.request(2).unwrap()), (true, true));
    // HEAD intentionally remains the admitted, undriven parent. Require that
    // state while consuming the quiescent host and retiring its native mounts.
    harness.disconnect_unfinished().await?;
    Ok(())
}
