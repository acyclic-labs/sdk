use super::*;

proptest! {
    // Property: borrowed keys preserve the owned tuple's total ordering and
    // exact match semantics, including empty, Unicode and duplicate keys.
    #[test]
    fn borrowed_keys_match_owned_keys(
        keys in prop::collection::vec((".{0,48}", ".{0,48}"), 0..128),
        query in (".{0,48}", ".{0,48}"),
    ) {
        let map: BTreeMap<_, _> = keys.iter().cloned().enumerate()
            .map(|(index, key)| (key, index)).collect();
        for key in keys.iter().chain(std::iter::once(&query)) {
            prop_assert_eq!(
                super::super::lookup::get(&map, &key.0, &key.1), map.get(key),
            );
        }
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn funded_resolution_preserves_limits_and_charges(
        name in "[a-z]{1,32}", revision in "[a-z]{1,32}",
        form in 0u8..3, missing in any::<bool>(), allowance in 0u64..128,
    ) {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let mut registry = ToolRegistry::default();
        let mut tool = checked(prototype())?;
        tool.definition.name.clone_from(&name);
        tool.definition.revision.clone_from(&revision);
        let data = Arc::make_mut(&mut registry.data);
        data.versions.insert((name.clone(), revision.clone()), Arc::new(tool));
        data.selected.insert(name.clone(), ModelSelection::Revision(revision.clone()));
        let query = if missing { format!("missing.{name}") } else { name };
        let pinned = format!("{query}@{revision}");
        let (query, explicit) = if form == 1 { (pinned.as_str(), None) }
            else { (query.as_str(), (form == 0).then_some(revision.as_str())) };
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            bytes_copied: 0, allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            items_examined: allowance,
            ..WorkBudget::UNBOUNDED
        };
        let actual_work = checked(NativeMaterializationWork::new(budget, &cancellation))?;
        let actual_io = checked(crate::filesystem::PreparationIo::from_native_work(&actual_work))?;
        let before = checked(actual_work.remaining())?;
        let expected = registry.get(query, explicit, None)?.is_some();
        let actual = registry.get(query, explicit, Some(&actual_io));
        // Only an unpinned query needs its name scanned. Resolving the map's
        // borrowed key requires neither copied bytes nor another allocation.
        let scanned = if explicit.is_none() { query.len() as u64 } else { 0 };
        if scanned > allowance {
            prop_assert!(matches!(actual, Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded {
                        counter: "items_examined", ..
                    }), ..
            })), "unfunded query scan must refuse");
        } else {
            prop_assert_eq!(actual?.is_some(), expected);
            let after = checked(actual_work.remaining())?;
            prop_assert_eq!(after.bytes_copied, before.bytes_copied);
            prop_assert_eq!(after.allocation_operations, before.allocation_operations);
            prop_assert_eq!(after.peak_allocation_bytes, before.peak_allocation_bytes);
            prop_assert_eq!(after.items_examined, before.items_examined - scanned);
            cancellation.cancel();
            let cancelled = registry.get(query, explicit, Some(&actual_io));
            if explicit.is_some() {
                prop_assert!(matches!(cancelled, Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
                })), "cancelled pinned lookup must refuse");
            } else {
                prop_assert!(matches!(cancelled, Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
                })),
                    "cancelled query scan must refuse");
            }
        }
    }
}

#[test]
fn resolution_without_subscriber() -> Result<()> {
    resolution_needs_no_temporary_key(0)
}

#[test]
fn resolution_with_tracing() -> Result<()> {
    resolution_needs_no_temporary_key(1)
}

#[cfg(feature = "filesystem")]
#[test]
fn resolution_with_accounting() -> Result<()> {
    resolution_needs_no_temporary_key(2)
}

fn resolution_needs_no_temporary_key(mode: u8) -> Result<()> {
    let prototype = prototype()?;
    let dispatch = if mode == 1 {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    for count in [1, 64, 4096] {
        let mut registry = ToolRegistry::default();
        let data = Arc::make_mut(&mut registry.data);
        for index in 0..count {
            let name = format!("tool.{index:06}");
            let mut tool = prototype.clone();
            tool.definition.name.clone_from(&name);
            data.versions
                .insert((name.clone(), "1".into()), Arc::new(tool));
            data.selected
                .insert(name, ModelSelection::Revision("1".into()));
        }
        let name = format!("tool.{:06}", count - 1);
        let pinned = format!("{name}@1");
        for form in 0..3 {
            let (name, revision) = match form {
                0 => (name.as_str(), Some("1")),
                1 => (pinned.as_str(), None),
                _ => (name.as_str(), None),
            };
            let mut counts = None;
            // Observe both the first lookup and its warmed repeat. Allocation
            // counts are exact; repeating a timing benchmark adds no coverage.
            for _ in 0..2 {
                #[cfg(feature = "filesystem")]
                let cancellation = acyclic_fs::CancellationToken::new();
                #[cfg(feature = "filesystem")]
                let work = acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .map_err(|(cause, _)| cause)?;
                #[cfg(feature = "filesystem")]
                let io = (mode == 2)
                    .then(|| crate::filesystem::PreparationIo::from_native_work(&work))
                    .transpose()?;
                #[cfg(not(feature = "filesystem"))]
                let io = None;
                let mut error = None;
                let allocation = acyclic_test_support::measure(|| {
                    match registry.get(std::hint::black_box(name), revision, io.as_ref()) {
                        Ok(Some(tool)) => {
                            std::hint::black_box(tool);
                        }
                        result => {
                            error = Some(result.map(|_| ()));
                        }
                    }
                });
                assert!(error.is_none(), "lookup failed: {error:?}");
                let actual = (
                    allocation.count_total,
                    allocation.bytes_total,
                    allocation.bytes_max,
                );
                assert_eq!(*counts.get_or_insert(actual), actual);
                if mode != 2 {
                    assert_eq!(
                        actual,
                        (0, 0, 0),
                        "temporary lookup key: mode={mode} tools={count} form={form}"
                    );
                }
            }
        }
    }
    Ok(())
}
