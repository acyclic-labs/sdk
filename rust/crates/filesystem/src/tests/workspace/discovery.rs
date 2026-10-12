use super::*;

#[tokio::test]
async fn pinned_directory_discovery_does_not_scan_ten_thousand_retained_workspaces()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "discovered-agent",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/visible.txt", "visible").await?;
    let pinned = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let cancellation = CancellationToken::new();
    let before = pinned
        .list_directory("/", None, 1, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map(|receipt| crate::OperationReceipt {
            value: receipt.value.0,
            work: receipt.work,
        })?;
    let mut retained = Vec::with_capacity(10_000);
    for index in 0..10_000 {
        let other = fs
            .create_workspace(
                &format!("retained-agent-{index}"),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        retained.push(
            other
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await?
                .value,
        );
    }
    workspace
        .write_text("/late.txt", "not in pinned view")
        .await?;
    let after = pinned
        .list_directory("/", None, 1, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map(|receipt| crate::OperationReceipt {
            value: receipt.value.0,
            work: receipt.work,
        })?;
    assert_eq!(retained.len(), 10_000);
    assert_eq!(before.value, after.value);
    assert_eq!(after.value.entries.len(), 1);
    assert!(after.work.backend_read_operations <= before.work.backend_read_operations);
    assert!(after.work.page_reads <= before.work.page_reads);
    assert_eq!(after.work.source_entries_visited, 0);
    assert_eq!(after.work.materializations, 0);
    Ok(())
}

#[test]
fn names_are_canonical_bounded_and_path_independent() -> Result<(), Box<dyn Error>> {
    let composed = WorkspaceName::new(
        "caf\u{e9}",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?
    .value;
    let decomposed = WorkspaceName::new(
        "cafe\u{301}",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?
    .value;
    assert_eq!(composed, decomposed);
    assert_eq!(composed.as_str(), "caf\u{e9}");
    assert!(matches!(
        WorkspaceName::new(
            "",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error),
        Err(WorkspaceError::Name(WorkspaceNameError::Empty))
    ));
    assert!(matches!(
        WorkspaceName::new(
            "..",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error),
        Err(WorkspaceError::Name(WorkspaceNameError::Reserved))
    ));
    assert!(matches!(
        WorkspaceName::new(
            "parent/child",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error),
        Err(WorkspaceError::Name(WorkspaceNameError::InvalidCharacter))
    ));
    Ok(())
}

#[test]
fn workspace_names_preserve_nfc_and_cancellation() -> Result<(), Box<dyn Error>> {
    use unicode_normalization::UnicodeNormalization;
    let cancellation = CancellationToken::new();
    for spelling in [
        "cafe\u{301}",
        "\u{344}",
        "\u{1100}\u{1161}\u{11a8}",
        "\u{9c7}\u{9be}",
        "\u{315}\u{300}A",
        "A\u{30a}\u{301}",
        "a\u{315}\u{300}",
        "a\u{301}\u{300}",
    ] {
        let expected: String = spelling.nfc().collect();
        let normalized = WorkspaceName::new(spelling, WorkBudget::UNBOUNDED, &cancellation)?;
        assert_eq!(normalized.value.as_str(), expected);
        assert!(normalized.work.peak_allocation_bytes >= normalized.value.retained_bytes());
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = WorkspaceName::new("name", WorkBudget::UNBOUNDED, &cancelled)
        .err()
        .ok_or("cancelled canonical name unexpectedly admitted")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
    Ok(())
}

#[test]
fn identity_is_deterministic_and_namespace_scoped() -> Result<(), Box<dyn Error>> {
    let name = WorkspaceName::new(
        "repo",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?
    .value;
    let first = WorkspaceId::derive(
        [1; 16],
        &name,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?
    .value;
    assert_eq!(
        first,
        WorkspaceId::derive(
            [1; 16],
            &name,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )?
        .value
    );
    assert_ne!(
        first,
        WorkspaceId::derive(
            [2; 16],
            &name,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )?
        .value
    );
    assert_ne!(
        first,
        WorkspaceId::derive(
            [1; 16],
            &WorkspaceName::new(
                "other",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )?
            .value,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )?
        .value
    );
    Ok(())
}

#[tokio::test]
async fn named_workspace_opens_and_forks_one_exact_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = fs
        .create_workspace(
            "repo",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let base = main
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let fork_key = IdempotencyKey::from_bytes([0x42; 16]);
    let fork = main
        .fork::<_, _, crate::WorkspaceError>(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let retried = main
        .fork::<_, _, crate::WorkspaceError>(
            "agent-42",
            ForkOptions::from_generation(base.clone(), fork_key),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    assert_eq!(
        fs.open_workspace(
            "repo",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await?
        .value
        .id(),
        main.id()
    );
    assert_ne!(fork.id(), main.id());
    assert_ne!(
        fork.head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await?
        .value
        .id(),
        base.id()
    );
    assert_eq!(retried.id(), fork.id());
    assert_eq!(
        retried
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        fork.head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await?
        .value
        .id()
    );
    assert!(
        main.fork::<_, _, crate::WorkspaceError>(
            "agent-42",
            ForkOptions::from_generation(base, IdempotencyKey::from_bytes([0x43; 16])),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| std::future::ready(Ok(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED
            }))
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn string_workspace_paths_match_native_namespace_names() -> Result<(), Box<dyn Error>> {
    for profile in [
        crate::model::FilesystemProfile::Posix,
        crate::model::FilesystemProfile::Windows,
    ] {
        let fs = Fs::memory();
        let config = VolumeConfig {
            profile,
            ..VolumeConfig::portable(Lifecycle::Ephemeral)
        };
        let workspace = fs
            .create_workspace_with_config(
                "native-path",
                config,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        workspace.write_text("/ÃƒÂ©.txt", "native").await?;
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        let path = NamespacePath::from_portable_in_profile(
            &PortablePath::parse("/ÃƒÂ©.txt", config.limits)?,
            profile,
            config.limits,
        )?;
        assert!(
            checkout
                .lookup_no_follow(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await?
                .value
                .record
                .is_some()
        );
    }
    Ok(())
}

#[tokio::test]
async fn generation_lookup_paths_preserves_order_absence_and_duplicates()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "lookup-paths",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/present", "body").await?;
    let generation = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;
    let records = generation
        .lookup_paths(
            &[present.clone(), absent, present.clone()],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(records.value.len(), 3);
    assert!(records.value[0].is_some());
    assert!(records.value[1].is_none());
    assert_eq!(records.value[0], records.value[2]);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_failure = generation
        .lookup_paths(
            std::slice::from_ref(&present),
            WorkBudget::UNBOUNDED,
            &cancelled,
        )
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("pre-cancelled lookup must fail"))?;
    assert_eq!(*cancelled_failure.work, crate::WorkCounters::default());
    let budget_failure = generation
        .lookup_paths(&[present], WorkBudget::default(), &CancellationToken::new())
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("zero-budget checkout must fail"))?;
    assert_ne!(*budget_failure.work, crate::WorkCounters::default());
    assert!(budget_failure.work.verify(WorkBudget::default()).is_err());
    Ok(())
}

#[tokio::test]
async fn bounded_path_index_hit_never_substitutes_a_false_empty_result()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "bounded-path-index",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    transaction.write_text("/a", "body").await?;
    transaction
        .hard_link(
            "/a",
            "/b",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert!(matches!(
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::default(),
                        },
                    )
                }
            )
            .await?
            .value,
        TransactionCommit::Committed(_)
    ));
    let generation = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let file_id = generation
        .stat(
            "/a",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .file_id;

    let complete = generation
        .namespace_records_for_file_ids([file_id], 16)
        .await?;
    assert!(complete.complete);
    assert_eq!(
        complete
            .records
            .iter()
            .find(|(id, _)| *id == file_id)
            .map(|(_, records)| records.len()),
        Some(2)
    );

    let bounded = generation
        .namespace_records_for_file_ids([file_id], 1)
        .await?;
    assert!(!bounded.complete);
    assert_eq!(
        bounded
            .records
            .iter()
            .find(|(id, _)| *id == file_id)
            .map(|(_, records)| records.len()),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
async fn generation_reader_resolves_many_paths_from_one_pinned_root() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "generation-reader",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/present", "body").await?;
    let generation = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;

    let reader = generation
        .workspace()
        .checkout(
            GenerationSelector::Exact(generation.id()),
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value
        .pinned_reader()
        .map_err(WorkspaceError::engine)?;
    let descriptions = reader
        .describe_files(
            &[present, absent],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;

    assert_eq!(descriptions.len(), 2);
    assert_eq!(
        descriptions[0].as_ref().map(|file| file.logical_bytes),
        Some(4)
    );
    assert!(descriptions[1].is_none());
    Ok(())
}

#[tokio::test]
async fn readonly_creation_lookup_keeps_authentic_initial_generation_and_failed_work()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let operation = IdempotencyKey::from_bytes([0xd4; 16]).operation_id();
    let cancellation = CancellationToken::new();
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    for allowance in allowances
        .as_object_mut()
        .expect("budget fields")
        .values_mut()
    {
        *allowance = serde_json::json!(100_000_000_u64);
    }
    let budget: WorkBudget = serde_json::from_value(allowances)?;
    let created = fs
        .create_workspace_with_operation(
            "readonly-original-creation",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?
        .value;
    let workspace = fs
        .open_workspace(
            "readonly-original-creation",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/later", "advanced head").await?;
    let advanced = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .id();
    assert_ne!(advanced, created.id());
    let original = fs
        .workspace_for_creation_operation(
            "readonly-original-creation",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?;
    original.work.verify(budget)?;
    assert_eq!(
        original.value.as_ref().map(|generation| generation.id()),
        Some(created.id())
    );
    assert_eq!(
        original
            .value
            .as_ref()
            .map(|generation| generation.workspace_id()),
        Some(workspace.id())
    );
    assert_eq!(original.work.authority_records_appended, 0);
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        advanced
    );

    let wrong_operation = IdempotencyKey::from_bytes([0xd5; 16]).operation_id();
    let mut wrong_config = config;
    wrong_config.lifecycle = Lifecycle::Durable;
    for (requested_config, requested_operation) in
        [(config, wrong_operation), (wrong_config, operation)]
    {
        let failure = fs
            .workspace_for_creation_operation(
                "readonly-original-creation",
                requested_config,
                requested_operation,
                budget,
                &cancellation,
            )
            .await
            .err()
            .expect("mismatched original creation is rejected");
        assert_ne!(*failure.work, crate::WorkCounters::UNCHARGED);
        failure.work.verify(budget)?;
        assert_eq!(failure.work.authority_records_appended, 0);
    }
    let missing = fs
        .workspace_for_creation_operation(
            "readonly-missing-creation",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?;
    assert!(missing.value.is_none());
    assert_eq!(missing.work.authority_records_appended, 0);
    missing.work.verify(budget)?;
    assert!(
        fs.open_workspace(
            "readonly-missing-creation",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .is_err()
    );

    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = fs
        .workspace_for_creation_operation(
            "readonly-original-creation",
            config,
            operation,
            denied,
            &cancellation,
        )
        .await
        .err()
        .expect("finite denial retains name admission prefix");
    assert_ne!(*failure.work, crate::WorkCounters::UNCHARGED);
    failure.work.verify(denied)?;
    assert_eq!(failure.work.authority_records_appended, 0);
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        advanced
    );
    Ok(())
}
