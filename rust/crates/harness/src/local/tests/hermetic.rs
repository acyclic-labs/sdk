//! Test compositions own their roots; Clippy resolves and rejects ambient discovery outside the native seam.
mod feature_policy;
use super::*;
use std::path::PathBuf;

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn default_state_overlap_is_refused_before_checkout_publication() -> Result<()> {
    struct Restore(Option<PathBuf>);
    impl Drop for Restore {
        fn drop(&mut self) {
            super::super::owner::STATE_ROOT.set(self.0.take());
        }
    }
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let info = checkout.path().join(".git/info");
    std::fs::create_dir_all(&info).map_err(super::super::storage)?;
    let exclude = info.join("exclude");
    std::fs::write(&exclude, b"# retained user rule\n").map_err(super::super::storage)?;
    let _restore =
        Restore(super::super::owner::STATE_ROOT.replace(Some(checkout.path().join("nested/data"))));
    let config = LocalHarnessConfig::new(checkout.path(), (model()?, text()))
        .compaction(crate::context::CompactionPolicy::Disabled)
        .without_default_tools();
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let config = config.settings(crate::native_settings::SettingsConfig::for_test(
        checkout.path(),
    )?);
    assert!(matches!(
        config.resolved_state_directory(),
        Err(Error::Invalid(_))
    ));
    let result = LocalHarness::open(config).await;
    assert!(
        matches!(&result, Err(Error::Invalid(_))),
        "unexpected open result: {}",
        match &result {
            Ok(_) => "opened".into(),
            Err(error) => error.to_string(),
        }
    );
    assert!(!checkout.path().join(".volumes").exists());
    assert!(!checkout.path().join("nested").exists());
    assert_eq!(
        std::fs::read(exclude).map_err(super::super::storage)?,
        b"# retained user rule\n"
    );
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn explicit_state_overlap_is_refused_before_checkout_publication() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let checkout = root.path().join("checkout");
    let info = checkout.join(".git/info");
    std::fs::create_dir_all(&info).map_err(super::super::storage)?;
    let exclude = info.join("exclude");
    std::fs::write(&exclude, b"# retained user rule\n").map_err(super::super::storage)?;
    for state in [
        root.path().to_path_buf(),
        checkout.clone(),
        checkout.join("state"),
    ] {
        let config = LocalHarnessConfig::new(&checkout, (model()?, text()))
            .state_directory(state)
            .compaction(crate::context::CompactionPolicy::Disabled)
            .without_default_tools();
        #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
        let config = config.settings(crate::native_settings::SettingsConfig::for_test(&checkout)?);
        assert!(matches!(
            LocalHarness::open(config).await,
            Err(Error::Invalid(_))
        ));
        assert!(!checkout.join(".volumes").exists());
        assert!(!checkout.join("state").exists());
        assert_eq!(
            std::fs::read(&exclude).map_err(super::super::storage)?,
            b"# retained user rule\n"
        );
    }
    Ok(())
}

#[test]
fn unit_tests_cannot_resolve_the_per_user_store() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let unrooted = LocalHarnessConfig::new(checkout.path(), (model()?, text()));
    assert!(
        matches!(super::super::owner::user_state_root(), Err(Error::Unsupported(message))
            if message == super::super::owner::UNIT_TESTS_INJECT_STATE)
    );
    assert!(
        matches!(unrooted.resolved_state_directory(), Err(Error::Unsupported(message))
            if message == super::super::owner::UNIT_TESTS_INJECT_STATE)
    );
    assert!(
        !checkout.path().join(".volumes").exists(),
        "refused ambient selection published checkout metadata"
    );
    // Positive control: the injected directory is the one resolution, and the test helper always injects one.
    let injected = tempfile::tempdir().map_err(super::super::storage)?;
    let rooted = LocalHarnessConfig::new(checkout.path(), (model()?, text()))
        .state_directory(injected.path())
        .prepare_state()?;
    assert_eq!(rooted.resolved_state_directory()?, injected.path());
    let helper = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .prepare_state()?;
    let owned = helper.resolved_state_directory()?;
    assert!(!owned.starts_with(checkout.path()));
    assert_ne!(
        owned,
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .prepare_state()?
            .resolved_state_directory()?
    );
    let default = unrooted.prepare_state()?;
    assert!(default.test_state.is_none());
    assert!(!default.hermetic);
    assert!(helper.hermetic);
    assert!(
        LocalHarnessConfig::new(checkout.path(), (model()?, text()))
            .hermetic()
            .resolved_state_directory()
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn opening_without_an_injected_root_owns_temporary_state() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let config = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .without_default_tools();
    // Negative control: bypassing preparation cannot enter the per-user root.
    assert!(config.resolved_state_directory().is_err());
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let config = config.settings(crate::native_settings::SettingsConfig::for_test(
        checkout.path(),
    )?);
    let harness = LocalHarness::open(config).await?;
    harness.close().await?;
    Ok(())
}

#[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
#[test]
fn unit_tests_cannot_resolve_the_user_home_or_codex_home() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let project = super::super::LocalProject::Workspace(checkout.path().to_path_buf());
    assert!(
        matches!(super::super::settings::configuration(&project, None, false), Err(Error::Unsupported(message))
            if message == super::super::settings::UNIT_TESTS_INJECT_SETTINGS)
    );
    Ok(())
}

#[test]
#[cfg(feature = "native-execution")]
fn default_state_is_one_directory_per_project_under_the_root() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let projects = tempfile::tempdir().map_err(super::super::storage)?;
    let [first, second] = ["first", "second"].map(|name| projects.path().join(name));
    for path in [&first, &second] {
        std::fs::create_dir(path).map_err(super::super::storage)?;
    }
    let at = |path: &PathBuf| {
        let checkout = super::super::owner::Checkout::capture(path)?;
        super::super::owner::state_directory_in(
            root.path(),
            &super::super::LocalProject::Workspace(path.clone()),
            Some(&checkout),
        )
    };
    let state = at(&first)?;
    assert_eq!(state.parent(), Some(root.path()));
    assert_eq!(state, at(&first.join("."))?);
    assert_ne!(state, at(&second)?);
    // Negative control: a root inside the checkout would put owner state in the agent's view.
    assert!(matches!(
        super::super::owner::state_directory_in(
            &first.join("nested"),
            &super::super::LocalProject::Workspace(first.clone()),
            Some(&super::super::owner::Checkout::capture(&first)?),
        ),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[cfg(feature = "native-execution")]
async fn retained_store_lifetime(order: Vec<usize>, retain: bool) -> Result<bool> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let mut config =
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .without_default_tools()
            .prepare_state()?;
    let root = config.resolved_state_directory()?;
    let released = config.test_state.as_ref().unwrap().release_barrier();
    // Negative control reproduces the old harness-only owner on the real stores.
    let premature = if retain {
        None
    } else {
        config.test_state.take()
    };
    let harness = LocalHarness::open(config).await?;
    let mut stores: Vec<Option<Box<dyn std::any::Any>>> = vec![
        Some(Box::new(harness.filesystem().clone())),
        Some(Box::new(harness.task_runtime().clone())),
        Some(Box::new(harness.task_files().clone())),
        Some(Box::new(
            harness
                .workspace
                .as_ref()
                .unwrap()
                .store
                .clone()
                .with_ownership_anchor(None),
        )),
    ];
    harness.close().await?;
    drop(premature);
    let mut valid = root.is_dir();
    for index in order {
        valid &= root.is_dir();
        drop(stores[index].take());
    }
    // Existing ownership barriers observe completion of provider work without polling.
    acyclic_native_runtime::run_blocking_io(move || released.wait(), None)
        .map_err(super::super::storage)?
        .await
        .map_err(super::super::storage)?;
    Ok(valid && !root.exists())
}

#[cfg(feature = "native-execution")]
mod state_lifetime_properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(12))]
        #[test]
        fn every_retained_store_keeps_the_state_root(order in Just(vec![0usize, 1, 2, 3]).prop_shuffle()) {
            // Property: any drop order keeps the backing root until the final store releases it.
            prop_assert!(WORKERS.block_on(retained_store_lifetime(order, true)).unwrap());
        }
    }

    #[tokio::test]
    async fn all_store_drop_orders_detect_a_missing_owner() -> Result<()> {
        use futures::StreamExt as _;
        let mut orders = Vec::new();
        for a in 0..4 {
            for b in 0..4 {
                for c in 0..4 {
                    for d in 0..4 {
                        let order = vec![a, b, c, d];
                        if (0..4).all(|index| order.contains(&index)) {
                            orders.push(order);
                        }
                    }
                }
            }
        }
        assert_eq!(orders.len(), 24);
        // Independent fixture roots permit overlapping I/O without sharing state.
        let outcomes = futures::stream::iter(orders)
            .map(|order| async move {
                assert!(retained_store_lifetime(order.clone(), true).await?);
                assert!(!retained_store_lifetime(order, false).await?);
                Ok::<_, Error>(())
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
        for outcome in outcomes {
            outcome?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn harness_only_owner_fails_the_same_lifetime_oracle() -> Result<()> {
        assert!(!retained_store_lifetime(vec![0, 1, 2, 3], false).await?);
        assert!(retained_store_lifetime(vec![0, 1, 2, 3], true).await?);
        Ok(())
    }
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn binding_supplied_stores_without_a_checkout_never_resolves_user_state() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let config = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .without_default_tools()
        .prepare_state()?;
    let root = config.resolved_state_directory()?;
    let harness = LocalHarness::open(config).await?;
    let storage = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(root.join("filesystem")))
        .await
        .map_err(super::super::storage)?;
    let stream = harness.task_stream();
    let volume = harness.task_files().volume().clone();
    let verifier = harness.verifier.clone();
    let signed = harness.signed.clone();
    let LocalHarness { runtime, files, .. } = harness;
    let runtime = Arc::try_unwrap(runtime)
        .map_err(|_| Error::Conflict("fixture runtime is still shared".into()))?;
    let mut config =
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .volume(volume)
            .without_default_tools()
            .prepare_state()?;
    config.state_directory = None;
    config.test_state = None;
    assert!(config.resolved_state_directory().is_err());
    let bound = LocalHarness::bind(
        config,
        runtime,
        files,
        (storage, stream),
        (&verifier, &signed),
        None,
    )
    .await?;
    bound.close().await
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn an_existing_volume_keeps_its_callers_store_ownership() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let volume = super::volume(directory.path()).await?;
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(super::super::storage)?;
    let config = crate::test_support::local_harness_config(
        directory.path().join("filesystem"),
        (model()?, text()),
    )?
    .volume(volume)
    .without_default_tools()
    .prepare_state()?;
    let state = config.resolved_state_directory()?;
    let released = config.test_state.as_ref().unwrap().release_barrier();
    let harness = LocalHarness::open(config).await?;
    harness.close().await?;
    acyclic_native_runtime::run_blocking_io(move || released.wait(), None)
        .map_err(super::super::storage)?
        .await
        .map_err(super::super::storage)?;
    // The temporary companion state closes independently of the caller's live provider.
    assert!(!state.exists());
    assert!(
        !filesystem
            .local_root_release_barrier()
            .unwrap()
            .is_released()
    );
    Ok(())
}

#[cfg(feature = "native-execution")]
async fn checkout_binding_lifetime(retain: bool) -> Result<bool> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .without_default_tools(),
    )
    .await?;
    let original = harness.task_stream();
    let storage = harness.workspace.as_ref().unwrap().filesystem.clone();
    let volume = harness.task_files().volume().clone();
    let verifier = harness.verifier.clone();
    let signed = harness.signed.clone();
    let LocalHarness { runtime, files, .. } = harness;
    let runtime = Arc::try_unwrap(runtime)
        .map_err(|_| Error::Conflict("fixture runtime is still shared".into()))?;
    let mut config =
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .volume(volume)
            .without_default_tools()
            .prepare_state()?;
    let root = config.resolved_state_directory()?;
    let released = config.test_state.as_ref().unwrap().release_barrier();
    // Omit the actual store owner, reproducing the former harness-only lifetime.
    let premature = if retain {
        None
    } else {
        config.test_state.take()
    };
    let bound = LocalHarness::bind(
        config,
        runtime,
        files,
        (storage, original),
        (&verifier, &signed),
        Some(checkout.path()),
    )
    .await?;
    let store = bound.workspace.as_ref().unwrap().store.clone();
    bound.close().await?;
    drop(premature);
    let retained = root.is_dir();
    drop(store);
    acyclic_native_runtime::run_blocking_io(move || released.wait(), None)
        .map_err(super::super::storage)?
        .await
        .map_err(super::super::storage)?;
    Ok(retained && !root.exists())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn binding_a_checkout_retains_its_state_until_the_shared_store_is_dropped() -> Result<()> {
    assert!(checkout_binding_lifetime(true).await?);
    assert!(!checkout_binding_lifetime(false).await?);
    Ok(())
}

#[test]
fn explicit_roots_allocate_no_temporary_state() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let allocations = || super::super::config::TEMPORARY_STATES.with(std::cell::Cell::get);
    let before = allocations();
    let config = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .state_directory(state.path())
        .prepare_state()?;
    assert_eq!(allocations(), before);
    assert!(config.test_state.is_none());
    assert_eq!(config.resolved_state_directory()?, state.path());
    // Negative control: the default path must actually invoke the same allocator.
    let default = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .prepare_state()?;
    assert_eq!(allocations(), before + 1);
    assert!(default.test_state.is_some());
    Ok(())
}

#[tokio::test]
async fn open_rejects_control_state_inside_the_checkout() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let state = checkout.path().join("state");
    let config = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .state_directory(&state)
        .without_default_tools();
    assert!(
        matches!(LocalHarness::open(config).await, Err(Error::Invalid(message))
        if message.contains("separate from the checkout"))
    );
    assert!(!state.exists(), "refused root created a directory");
    assert!(
        !checkout.path().join(".volumes").exists(),
        "refused control root published checkout metadata"
    );
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn bind_rejects_control_state_inside_the_checkout() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
            .without_default_tools(),
    )
    .await?;
    let storage = harness.workspace.as_ref().unwrap().filesystem.clone();
    let stream = harness.task_stream();
    let volume = harness.task_files().volume().clone();
    let verifier = harness.verifier.clone();
    let signed = harness.signed.clone();
    let LocalHarness { runtime, files, .. } = harness;
    let runtime = Arc::try_unwrap(runtime)
        .map_err(|_| Error::Conflict("fixture runtime is still shared".into()))?;
    let state = checkout.path().join("state");
    let config = crate::test_support::local_harness_config(checkout.path(), (model()?, text()))?
        .volume(volume)
        .state_directory(&state)
        .without_default_tools();
    assert!(
        matches!(LocalHarness::bind(config, runtime, files, (storage, stream),
        (&verifier, &signed), Some(checkout.path())).await,
        Err(Error::Invalid(message)) if message.contains("separate from the checkout"))
    );
    assert!(!state.exists(), "refused root created a directory");
    Ok(())
}

#[test]
fn canonical_separation_rejects_both_overlap_directions() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let checkout = root.path().join("checkout");
    std::fs::create_dir(&checkout).map_err(super::super::storage)?;
    for state in [
        root.path().to_path_buf(),
        checkout.clone(),
        checkout.join("state"),
    ] {
        assert!(super::super::owner::separated_directory(&state, Some(&checkout)).is_err());
    }
    assert!(!checkout.join("state").exists());
    let separate = root.path().join("separate");
    assert!(super::super::owner::separated_directory(&separate, Some(&checkout)).is_ok());
    #[cfg(unix)]
    {
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&checkout, &alias).map_err(super::super::storage)?;
        assert!(
            super::super::owner::separated_directory(&alias.join("nested"), Some(&checkout))
                .is_err()
        );
        assert!(!checkout.join("nested").exists());
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn unresolved_suffixes_cannot_create_directories_before_rejection(depth in 1usize..5) {
        let checkout = tempfile::tempdir().unwrap();
        let mut state = checkout.path().to_path_buf();
        for _ in 0..depth {
            state.push("missing");
            state.push("..");
        }
        state.push("state");
        proptest::prop_assert!(super::super::owner::separated_directory(&state, Some(checkout.path())).is_err());
        proptest::prop_assert_eq!(std::fs::read_dir(checkout.path()).unwrap().count(), 0);
        // Accepted canonical paths round-trip through the same production boundary.
        let storage = tempfile::tempdir().unwrap();
        let accepted = super::super::owner::separated_directory(&storage.path().join("state"), Some(checkout.path())).unwrap();
        let reopened = super::super::owner::separated_directory(&accepted, Some(checkout.path())).unwrap();
        proptest::prop_assert_eq!(accepted, reopened);
    }
}
