use super::*;

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
    }
    assert!(matches!(
        value!(WorkspaceName::new(
            "",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )),
        Err(WorkspaceError::Name(WorkspaceNameError::Empty))
    ));
    assert!(matches!(
        value!(WorkspaceName::new(
            "..",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )),
        Err(WorkspaceError::Name(WorkspaceNameError::Reserved))
    ));
    assert!(matches!(
        value!(WorkspaceName::new(
            "parent/child",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )),
        Err(WorkspaceError::Name(WorkspaceNameError::InvalidCharacter))
    ));
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = WorkspaceName::new("name", WorkBudget::UNBOUNDED, &cancelled)
        .err()
        .ok_or("cancelled canonical name unexpectedly admitted")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    Ok(())
}

#[test]
fn identity_is_deterministic_and_namespace_scoped() -> Result<(), Box<dyn Error>> {
    let name = value!(WorkspaceName::new(
        "repo",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ))?;
    let first = value!(WorkspaceId::derive(
        [1; 16],
        &name,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ))?;
    assert_eq!(
        first,
        value!(WorkspaceId::derive(
            [1; 16],
            &name,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        ))?
    );
    assert_ne!(
        first,
        value!(WorkspaceId::derive(
            [2; 16],
            &name,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        ))?
    );
    assert_ne!(
        first,
        value!(WorkspaceId::derive(
            [1; 16],
            &value!(WorkspaceName::new(
                "other",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            ))?,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        ))?
    );
    Ok(())
}

#[tokio::test]
async fn named_workspace_opens_and_forks_one_exact_generation() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("repo"))?;
    let base = unbounded!(main, head())?;
    let fork_key = IdempotencyKey::from_bytes([0x42; 16]);
    let fork = fork_workspace(
        &main,
        "agent-42",
        ForkOptions::from_generation(base.clone(), fork_key),
    )
    .await?;
    let retried = fork_workspace(
        &main,
        "agent-42",
        ForkOptions::from_generation(base.clone(), fork_key),
    )
    .await?;
    assert_eq!(unbounded!(fs, open_workspace("repo"))?.id(), main.id());
    assert_ne!(fork.id(), main.id());
    assert_ne!(unbounded!(fork, head())?.id(), base.id());
    assert_eq!(retried.id(), fork.id());
    assert_eq!(
        unbounded!(retried, head())?.id(),
        unbounded!(fork, head())?.id()
    );
    assert!(
        fork_workspace(
            &main,
            "agent-42",
            ForkOptions::from_generation(base, IdempotencyKey::from_bytes([0x43; 16]))
        )
        .await
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
        let workspace = unbounded!(fs, create_workspace_with_config("native-path", config))?;
        workspace.write_text("/ÃƒÂ©.txt", "native").await?;
        let mut checkout = unbounded!(
            workspace,
            checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
        )?;
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
