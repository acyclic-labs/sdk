use super::*;
use acyclic_test_support::differential;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

// Frozen pre-index classifier. It owns no candidate logic or state.
fn original(
    paths: &[NamespacePath],
    directory_count: usize,
    kinds: &mut [Option<FileKind>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    fn contains(
        paths: &[NamespacePath],
        target: &NamespacePath,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceError> {
        for path in paths {
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if path == target {
                return Ok(true);
            }
        }
        Ok(false)
    }
    for (index, path) in paths.iter().enumerate() {
        let kind = kinds.get_mut(index).ok_or(FsError::InvalidDiff)?;
        if index < directory_count {
            match *kind {
                Some(FileKind::Directory) => continue,
                Some(_) => return Err(WorkspaceError::NotDirectory),
                None => {}
            }
            if contains(&paths[..index], path, accounting)? {
                *kind = Some(FileKind::Directory);
            }
        } else {
            if kind.is_some_and(|kind| kind != FileKind::Regular)
                || contains(&paths[..directory_count], path, accounting)?
            {
                return Err(WorkspaceError::NotRegularFile);
            }
            if kind.is_some() || contains(&paths[directory_count..index], path, accounting)? {
                *kind = Some(FileKind::Regular);
            }
        }
    }
    Ok(())
}

fn paths(ids: &[u16]) -> Vec<NamespacePath> {
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    ids.iter()
        .map(|id| super::super::paths::customer_path(&format!("/p{id:04}"), config).unwrap())
        .collect()
}

fn compare(ids: &[u16], directories: usize, input: &[Option<FileKind>]) -> TestCaseResult {
    let [before, after] = differential::pair::<_, _, (&'static str, u64)>(
        || (paths(ids), input.to_vec()),
        |(paths, mut kinds), old| {
            let cancellation = CancellationToken::new();
            let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            let result = if old {
                original(&paths, directories, &mut kinds, &mut accounting)
            } else {
                classify_batch_paths(&paths, directories, &mut kinds, &mut accounting)
            };
            result.map(|()| kinds).map_err(|error| format!("{error:?}"))
        },
    );
    prop_assert_eq!(before, after);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn indexed_classification_preserves_original_kinds_and_exact_errors(
        input in prop::collection::vec((0u16..16, 0u8..3), 0..32), split in 0usize..33,
    ) {
        let ids: Vec<_> = input.iter().map(|(id, _)| *id).collect();
        let kinds: Vec<_> = input.iter().map(|(_, kind)| match kind {
            0 => None, 1 => Some(FileKind::Regular), _ => Some(FileKind::Directory),
        }).collect();
        compare(&ids, split.min(ids.len()), &kinds)?;
        // Every case includes accepted duplicate directories/files and both
        // rejection classes, even when its generated input is empty or denied.
        compare(&[7, 7, 1, 1], 2, &[None; 4])?;
        compare(&[1, 1], 1, &[None; 2])?;
        compare(&[1], 1, &[Some(FileKind::Regular)])?;
        compare(&ids, 0, &vec![None; ids.len()])?;
    }
}

fn measurement(
    count: usize,
    repeat_scans: bool,
) -> (
    Result<Vec<Option<FileKind>>, String>,
    crate::WorkCounters,
    acyclic_test_support::AllocationInfo,
) {
    let paths = paths(&(0..u16::try_from(count).unwrap()).collect::<Vec<_>>());
    let mut kinds = vec![None; paths.len()];
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut result = Ok(());
    let allocations = acyclic_test_support::measure(|| {
        result = classify_batch_paths(&paths, 0, &mut kinds, &mut accounting);
        if result.is_ok() && repeat_scans {
            // Real add-back: all n*(n-1)/2 old comparisons, no extra allocation
            // or semantic change that could mask the work observation.
            result = original(&paths, 0, &mut kinds, &mut accounting);
        }
    });
    (
        result.map(|()| kinds).map_err(|error| format!("{error:?}")),
        accounting.spent(),
        allocations,
    )
}

fn exact(count: usize, repeat_scans: bool) -> TestCaseResult {
    let (output, work, allocations) = measurement(count, repeat_scans);
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("transaction_batch_expected.json")).unwrap();
    let actual = serde_json::json!({
        "work": work, "allocations": differential::allocation_counts(allocations),
    });
    prop_assert!(
        output == Ok(vec![None; count]) && actual == expected[format!("classification.{count}")],
        "exact indexed classification work/allocation differs: actual={actual}"
    );
    Ok(())
}

#[test]
fn indexed_classification_has_exact_work_and_allocations() {
    for count in [1024, 2048] {
        exact(count, false).unwrap();
    }
}

#[test]
fn restored_quadratic_scans_fail_the_same_exact_guard() {
    for count in [1024, 2048] {
        let error = exact(count, true).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exact indexed classification work/allocation differs")
        );
    }
}

#[test]
fn classification_accounting_measurements() {
    for count in [1024, 2048] {
        let (output, work, allocations) = measurement(count, false);
        assert_eq!(output, Ok(vec![None; count]));
        eprintln!(
            "PERF_ACCOUNTING {}",
            serde_json::json!({
                "case": format!("classification.{count}"), "work": work,
                "allocations": differential::allocation_counts(allocations),
            })
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn distinct_sorted_paths_scale_with_exact_counts_at_n_and_twice_n(n in 1usize..1025) {
        for count in [n, 2 * n] {
            let (output, work, _) = measurement(count, false);
            prop_assert_eq!(output, Ok(vec![None; count]));
            // One kind inspection per input, one sortedness and one duplicate
            // comparison per adjacent pair. The old scans instead take n*(n-1)/2.
            prop_assert_eq!(work.items_examined, (3 * count - 2) as u64);
        }
    }
}
