use super::*;

#[tokio::test]
async fn sibling_forks_adding_distinct_files_join_in_sequence() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("siblings-main"))?;
    main.write_text("/README.md", "base\n").await?;
    let base = unbounded!(main, head())?;
    let first = fork_workspace(
        &main,
        "siblings-first",
        ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
    )
    .await?;
    let second = fork_workspace(
        &main,
        "siblings-second",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    first.write_text("/a.rs", "a\n").await?;
    second.write_text("/b.rs", "b\n").await?;

    for (label, fork) in [("first", &first), ("second", &second)] {
        let plan = fork.join_into(&main).plan().await?;
        let outcome = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                crate::PublicationPermit::Unrestricted,
            )
            .await?;
        let applied = match outcome {
            JoinOutcome::Applied(_) => true,
            JoinOutcome::Conflicted { conflicts, .. } => {
                let described = plan.describe_conflicts(&conflicts, false).await?;
                panic!("{label} join conflicted: {:?}", described.conflicts);
            }
            _ => false,
        };
        assert!(applied, "{label} join did not apply");
    }
    assert_eq!(
        unbounded!(main, read("/a.rs", 16))?,
        OwnedBytes::from_static(b"a\n")
    );
    assert_eq!(
        unbounded!(main, read("/b.rs", 16))?,
        OwnedBytes::from_static(b"b\n")
    );
    assert_eq!(
        unbounded!(main, read("/README.md", 16))?,
        OwnedBytes::from_static(b"base\n")
    );
    Ok(())
}

#[tokio::test]
async fn sibling_files_created_independently_merge_by_path() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("file-fold-main"))?;
    main.write_text("/README.md", "base\n").await?;
    let base = unbounded!(main, head())?;
    let mut forks = Vec::new();
    for (name, other) in [
        ("file-fold-first", "first\n"),
        ("file-fold-second", "second\n"),
    ] {
        let fork = fork_workspace(
            &main,
            name,
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
        fork.write_text("/same.rs", "same\n").await?;
        fork.write_text("/other.rs", other).await?;
        forks.push(fork);
    }
    let mut outcomes = Vec::new();
    for fork in &forks {
        let plan = fork.join_into(&main).plan().await?;
        outcomes.push(
            match plan
                .apply(
                    ApplyOptions {
                        if_target: plan.target_head(),
                        idempotency_key: IdempotencyKey::new(),
                    },
                    crate::PublicationPermit::Unrestricted,
                )
                .await?
            {
                JoinOutcome::Applied(_) => Vec::new(),
                JoinOutcome::Conflicted { conflicts, .. } => plan
                    .describe_conflicts(&conflicts, false)
                    .await?
                    .conflicts
                    .into_iter()
                    .map(|conflict| conflict.path)
                    .collect(),
                _ => panic!("join did not apply"),
            },
        );
    }
    assert_eq!(outcomes, [Vec::new(), vec![Some("/other.rs".to_owned())]]);

    let fork = fork_workspace(
        &main,
        "file-fold-third",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    fork.write_text("/same.rs", "same\n").await?;
    let plan = fork.join_into(&main).plan().await?;
    match plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            crate::PublicationPermit::Unrestricted,
        )
        .await?
    {
        JoinOutcome::NoChanges(_) => {}
        JoinOutcome::Conflicted { conflicts, .. } => {
            let described = plan.describe_conflicts(&conflicts, false).await?;
            panic!(
                "an identical independent creation must fold: {:?}",
                described.conflicts
            );
        }
        _ => panic!("an identical independent creation changes nothing"),
    }
    assert_eq!(
        unbounded!(main, read("/same.rs", 16))?,
        OwnedBytes::from_static(b"same\n")
    );
    assert_eq!(
        unbounded!(main, read("/other.rs", 16))?,
        OwnedBytes::from_static(b"first\n")
    );
    Ok(())
}
