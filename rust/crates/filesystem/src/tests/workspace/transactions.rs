use super::*;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // mkdir observes prefix bindings, not directory contents, whether it is
    // staged alone or in a batch. Explicit directory observations and replaced
    // bindings must still prevent a safe rebase.
    #[test]
    fn create_dirs_rebase_preserves_exact_bindings(
        name in "entry[a-z]{1,4}",
        payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..17),
    ) {
        #[derive(Clone, Copy, Debug)]
        enum WriteMode {
            Serial,
            #[cfg(not(target_arch = "wasm32"))]
            Batched,
            #[cfg(not(target_arch = "wasm32"))]
            PendingBatch,
        }
        let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
            for mode in [
                WriteMode::Serial,
                #[cfg(not(target_arch = "wasm32"))]
                WriteMode::Batched,
                #[cfg(not(target_arch = "wasm32"))]
                WriteMode::PendingBatch,
            ] {
            for winner_mode in [
                WriteMode::Serial,
                #[cfg(not(target_arch = "wasm32"))]
                WriteMode::Batched,
            ] {
            for inside in [false, true] {
            for observe_directory in [false, true] {
                for replace_prefix in [false, true] {
                    if inside && replace_prefix {
                        continue;
                    }
                    // The other writer adds a sibling under this directory.
                    let (directory, components): (&str, &[&[u8]]) = if inside {
                        ("/shared/nested", &[b"shared", b"nested"])
                    } else {
                        ("/shared", &[b"shared"])
                    };
                    let sibling = format!("{directory}/{name}");
                    let fs = Fs::memory();
                    let workspace = unbounded!(fs, create_workspace_with_config("mkdir-dependencies", VolumeConfig {
                        concurrency: crate::model::ConcurrencyMode::SerializedAuthority,
                        ..VolumeConfig::portable(Lifecycle::Ephemeral)
                    }))?;
                    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                    unbounded!(seed, create_dir_all("/shared/nested"))?;
                    commit(&mut seed).await?;
                    let mut pending = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                    #[cfg(not(target_arch = "wasm32"))]
                    if matches!(mode, WriteMode::PendingBatch) {
                        pending.write_text("/staged", "pending").await?;
                    }
                    if observe_directory {
                        let limits = pending.checkout.volume_config().limits;
                        let path = crate::kernel::NamespacePath::new(components.iter().map(|component| crate::kernel::LogicalName::new(
                            crate::kernel::NameEncoding::Utf8, component.to_vec(),
                        )).collect::<Result<_, _>>()?, limits)?;
                        unbounded!(pending.checkout, lookup_no_follow(&path))?;
                    }
                    match mode {
                        WriteMode::Serial => {
                            unbounded!(pending, create_dir_all("/shared/nested"))?;
                            pending.write_text("/shared/nested/local", "local").await?;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        WriteMode::Batched | WriteMode::PendingBatch => {
                            unbounded!(pending, write_batch(&["/shared/nested"], &[("/shared/nested/local", OwnedBytes::copy_from_slice(b"local"))]))?;
                        }
                    }
                    let mut other = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                    if replace_prefix {
                        unbounded!(other, remove("/shared/nested"))?;
                        other.write_text("/shared/nested", "replacement").await?;
                    }
                    match winner_mode {
                        WriteMode::Serial => {
                            unbounded!(other, write(&sibling, OwnedBytes::copy_from_slice(&payload)))?;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        WriteMode::Batched | WriteMode::PendingBatch => {
                            unbounded!(other, write_batch(&[directory], &[(sibling.as_str(), OwnedBytes::copy_from_slice(&payload))]))?;
                        }
                    }
                    assert!(matches!(commit(&mut other).await?, TransactionCommit::Committed(_)));
                    let outcome = commit(&mut pending).await?;
                    if observe_directory || replace_prefix {
                        assert!(matches!(outcome, TransactionCommit::Conflict { .. }));
                        assert!(matches!(unbounded!(pending, rebase(1))?, TransactionRebase::Conflicted { .. }));
                    } else {
                        assert!(matches!(outcome, TransactionCommit::Committed(_)), "mode={mode:?} winner={winner_mode:?} inside={inside}");
                        assert_eq!(unbounded!(workspace, read("/shared/nested/local", 16))?.as_ref(), b"local");
                        #[cfg(not(target_arch = "wasm32"))]
                        if matches!(mode, WriteMode::PendingBatch) {
                            assert_eq!(unbounded!(workspace, read("/staged", 16))?.as_ref(), b"pending");
                        }
                    }
                    assert_eq!(unbounded!(workspace, read(&sibling, 32))?.as_ref(), payload.as_slice());
                }
            }
            }
            }
            }
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}
