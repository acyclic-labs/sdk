use super::*;

struct SelectTheirsDriver;

impl crate::MergeDriver for SelectTheirsDriver {
    fn fingerprint(&self) -> &[u8] {
        b"workspace-select-theirs-v1"
    }

    fn mode(&self) -> crate::MergeDriverMode {
        crate::MergeDriverMode::Deterministic
    }

    fn resolve(
        &self,
        conflict: &crate::ConflictView,
    ) -> Result<crate::MergeResolution, crate::DriverError> {
        assert_eq!(conflict.kind, crate::ConflictKind::Text);
        assert_eq!(conflict.base, crate::ConflictValue::Text("base".into()));
        assert_eq!(conflict.ours, crate::ConflictValue::Text("main".into()));
        assert_eq!(conflict.theirs, crate::ConflictValue::Text("agent".into()));
        Ok(crate::MergeResolution::Select(crate::ConflictSide::Theirs))
    }
}

#[tokio::test]
async fn merge_drivers_resolve_and_publish_real_workspace_joins() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("driver-main"))?;
    main.write_text("/shared", "base").await?;
    let agent = fork_workspace(
        &main,
        "driver-agent",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/shared", "agent").await?;
    main.write_text("/shared", "main").await?;

    let plan = agent
        .join_into(&main)
        .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("theirs", Arc::new(SelectTheirsDriver))?;
    registry.set_default("theirs")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let join_key = IdempotencyKey::new();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: join_key,
            },
            &registry,
            &mut cache,
            false,
            crate::PublicationPermit::Unrestricted,
        )
        .await?;
    let JoinOutcome::Applied(application) = outcome else {
        return Err("resolved join did not publish".into());
    };
    assert!(
        main.verify_join_commit(
            &value!(plan.commit_witness(
                &application,
                join_key,
                &WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            ))?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?
    );
    assert_eq!(
        unbounded!(main, read("/shared", 16))?,
        OwnedBytes::from_static(b"agent")
    );
    Ok(())
}

#[tokio::test]
async fn default_text_driver_projects_markers_and_publishes_them() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("text-driver-main"))?;
    main.write_text("/shared", "base\n").await?;
    let agent = fork_workspace(
        &main,
        "text-driver-agent",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    agent.write_text("/shared", "agent\n").await?;
    main.write_text("/shared", "main\n").await?;

    let plan = agent
        .join_into(&main)
        .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        )
        .await?
    else {
        return Err("text join did not expose its conflict".into());
    };
    let described = plan.describe_conflicts(conflicts, truncated).await?;
    assert_eq!(described.conflicts.len(), 1);
    assert_eq!(described.conflicts[0].path.as_deref(), Some("/shared"));
    assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Text);
    assert!(matches!(
        described.conflicts[0].ours,
        crate::ConflictValue::Text(ref text) if text == "main\n"
    ));
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
    registry.set_default("text")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            &registry,
            &mut cache,
            false,
            crate::PublicationPermit::Unrestricted,
        )
        .await?;
    assert!(matches!(outcome, JoinOutcome::Applied(_)));
    let merged = String::from_utf8(unbounded!(main, read("/shared", 1024))?.to_vec())?;
    assert!(merged.contains("main"));
    assert!(merged.contains("agent"));
    Ok(())
}

/// A workspace join (no Git metadata) where one side replaced the file under
/// its path: the replacement merges with the identity it replaced, the text
/// driver sees both texts, and the path keeps its original identity.
#[tokio::test]
async fn joins_merge_a_replaced_file_with_the_identity_it_replaced() -> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::Merge).await
}

#[tokio::test]
async fn rebase_joins_merge_a_replaced_file_with_the_identity_it_replaced()
-> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::Rebase).await
}

#[tokio::test]
async fn squash_joins_merge_a_replaced_file_with_the_identity_it_replaced()
-> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::Squash).await
}

#[tokio::test]
async fn cherry_pick_joins_merge_a_replaced_file_with_the_identity_it_replaced()
-> Result<(), Box<dyn Error>> {
    replaced_file_join(JoinHistory::CherryPick).await
}

async fn replaced_file_join(history: JoinHistory) -> Result<(), Box<dyn Error>> {
    let base = "one\ntwo\nthree\n";
    for (agent_replaces, agent_text, main_text, expected) in [
        (true, base, "ONE\ntwo\nthree\n", "ONE\ntwo\nthree\n"),
        (
            true,
            "one\ntwo\nTHREE\n",
            "ONE\ntwo\nthree\n",
            "ONE\ntwo\nTHREE\n",
        ),
        (
            false,
            "one\ntwo\nTHREE\n",
            "ONE\ntwo\nthree\n",
            "ONE\ntwo\nTHREE\n",
        ),
    ] {
        let fs = Fs::memory();
        let main = unbounded!(fs, create_workspace("replaced-main"))?;
        main.write_text("/shared", base).await?;
        let original = unbounded!(main, stat("/shared"))?.file_id;
        let agent = fork_workspace(
            &main,
            "replaced-agent",
            ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
        )
        .await?;
        let (replacing, replacing_text, editing, editing_text) = if agent_replaces {
            (&agent, agent_text, &main, main_text)
        } else {
            (&main, main_text, &agent, agent_text)
        };
        replacing.remove("/shared").await?;
        replacing.write_text("/shared", replacing_text).await?;
        assert_ne!(unbounded!(replacing, stat("/shared"))?.file_id, original);
        editing.write_text("/shared", editing_text).await?;

        let plan = agent
            .join_into(&main)
            .history(history)
            .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        // Equal content merges without a driver; edits on both sides reach
        // the text driver as one text conflict at the path.
        let outcome = match plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                BTreeMap::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        {
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => {
                let described = plan.describe_conflicts(conflicts, truncated).await?;
                assert_eq!(described.conflicts.len(), 1);
                assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Text);
                assert_eq!(described.conflicts[0].path.as_deref(), Some("/shared"));
                let mut registry = crate::MergeDriverRegistry::new();
                registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
                registry.set_default("text")?;
                let mut cache = crate::MemoryMergeResolutionCache::default();
                plan.apply_with_drivers(
                    ApplyOptions {
                        if_target: plan.target_head(),
                        idempotency_key: IdempotencyKey::new(),
                    },
                    &registry,
                    &mut cache,
                    false,
                    crate::PublicationPermit::Unrestricted,
                )
                .await?
            }
            outcome => outcome,
        };
        let variant = match &outcome {
            JoinOutcome::Applied(_) => "applied",
            JoinOutcome::AlreadyApplied(_) => "already applied",
            JoinOutcome::NoChanges(_) => "no changes",
            JoinOutcome::StaleTarget(_) => "stale target",
            JoinOutcome::Conflicted { .. } => "conflicted",
            JoinOutcome::Fenced => "fenced",
            JoinOutcome::IdempotencyConflict => "idempotency conflict",
        };
        // Merging an equal replacement into the target's edit leaves the
        // target as it is.
        let expected_variant = if agent_text == base {
            "no changes"
        } else {
            "applied"
        };
        assert_eq!(
            variant, expected_variant,
            "{history:?} agent_replaces={agent_replaces} agent_text={agent_text:?}"
        );
        let merged = String::from_utf8(unbounded!(main, read("/shared", 1024))?.to_vec())?;
        assert_eq!(merged, expected);
        let record = unbounded!(main, stat("/shared"))?;
        assert_eq!(record.file_id, original);
        assert_eq!(record.link_count, 1);
    }
    Ok(())
}

/// A file moved onto the path is not a replacement: the kernel pairs only a
/// file the deleting side created, so the join must not offer the moved
/// file's text as the deleted identity's resolution (the kernel would leave
/// the path bound to the moved file and drop the merged text with the
/// unlinked identity).
#[tokio::test]
async fn joins_keep_a_file_moved_onto_a_deleted_path_conflicted() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("moved-main"))?;
    main.write_text("/a", "a one\na two\n").await?;
    main.write_text("/b", "b one\nb two\n").await?;
    let agent = fork_workspace(
        &main,
        "moved-agent",
        ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
    )
    .await?;
    main.remove("/a").await?;
    let mut moving = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(moving, rename("/b", "/a", true))?;
    let TransactionCommit::Committed(_) = commit(&mut moving).await? else {
        return Err("rename did not commit".into());
    };
    main.write_text("/a", "b one\nb TWO\n").await?;
    agent.write_text("/a", "A ONE\na two\n").await?;

    let plan = agent
        .join_into(&main)
        .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            BTreeMap::new(),
            crate::PublicationPermit::Unrestricted,
        )
        .await?
    else {
        return Err("a delete/modify beside a move must conflict".into());
    };
    let described = plan.describe_conflicts(conflicts, truncated).await?;
    assert!(
        described
            .conflicts
            .iter()
            .all(|conflict| conflict.kind != crate::ConflictKind::Text),
        "the moved file must not stand in for the deleted one: {:?}",
        described
            .conflicts
            .iter()
            .map(|conflict| conflict.kind)
            .collect::<Vec<_>>()
    );
    let mut registry = crate::MergeDriverRegistry::new();
    registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
    registry.set_default("text")?;
    let mut cache = crate::MemoryMergeResolutionCache::default();
    let outcome = plan
        .apply_with_drivers(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            &registry,
            &mut cache,
            false,
            crate::PublicationPermit::Unrestricted,
        )
        .await;
    assert!(
        !matches!(outcome, Ok(JoinOutcome::Applied(_))),
        "the agent's edit must not be published away"
    );
    assert_eq!(
        String::from_utf8(unbounded!(main, read("/a", 64))?.to_vec())?,
        "b one\nb TWO\n"
    );
    Ok(())
}

/// The replacement rule follows directory identity, not full paths: under a
/// directory both sides renamed, a replaced file still reaches the text
/// driver as one text conflict and keeps its original identity.
#[tokio::test]
async fn joins_merge_a_replaced_file_under_a_renamed_directory() -> Result<(), Box<dyn Error>> {
    for agent_replaces in [true, false] {
        let fs = Fs::memory();
        let main = unbounded!(fs, create_workspace("renamed-main"))?;
        let mut seeding = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(seeding, create_directory("/d"))?;
        let TransactionCommit::Committed(_) = commit(&mut seeding).await? else {
            return Err("directory did not commit".into());
        };
        main.write_text("/d/a", "one\ntwo\nthree\n").await?;
        let original = unbounded!(main, stat("/d/a"))?.file_id;
        let agent = fork_workspace(
            &main,
            "renamed-agent",
            ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
        )
        .await?;
        for side in [&main, &agent] {
            let mut renaming = unbounded!(side, begin_transaction(IdempotencyKey::new()))?;
            unbounded!(renaming, rename("/d", "/e", true))?;
            let TransactionCommit::Committed(_) = commit(&mut renaming).await? else {
                return Err("rename did not commit".into());
            };
        }
        let (replacing, editing) = if agent_replaces {
            (&agent, &main)
        } else {
            (&main, &agent)
        };
        replacing.remove("/e/a").await?;
        replacing.write_text("/e/a", "one\ntwo\nTHREE\n").await?;
        assert_ne!(unbounded!(replacing, stat("/e/a"))?.file_id, original);
        editing.write_text("/e/a", "ONE\ntwo\nthree\n").await?;

        let plan = agent
            .join_into(&main)
            .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                BTreeMap::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        else {
            return Err("overlapping whole-file edits must reach the driver".into());
        };
        let described = plan.describe_conflicts(conflicts, truncated).await?;
        assert_eq!(
            described.conflicts.len(),
            1,
            "agent_replaces={agent_replaces}"
        );
        assert_eq!(
            described.conflicts[0].kind,
            crate::ConflictKind::Text,
            "agent_replaces={agent_replaces}"
        );
        let mut registry = crate::MergeDriverRegistry::new();
        registry.register("text", Arc::new(crate::DefaultTextMergeDriver))?;
        registry.set_default("text")?;
        let mut cache = crate::MemoryMergeResolutionCache::default();
        assert!(matches!(
            plan.apply_with_drivers(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                &registry,
                &mut cache,
                false,
                crate::PublicationPermit::Unrestricted,
            )
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(
            String::from_utf8(unbounded!(main, read("/e/a", 64))?.to_vec())?,
            "ONE\ntwo\nTHREE\n"
        );
        let record = unbounded!(main, stat("/e/a"))?;
        assert_eq!(record.file_id, original);
        assert_eq!(record.link_count, 1);
    }
    Ok(())
}
