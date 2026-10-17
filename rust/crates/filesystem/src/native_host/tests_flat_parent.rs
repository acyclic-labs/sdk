use super::*;
use proptest::prelude::*;

fn same_stat(actual: &HostStat, expected: &HostStat) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(actual.identity(), expected.identity());
    prop_assert_eq!(actual.file_type(), expected.file_type());
    prop_assert_eq!(actual.len(), expected.len());
    prop_assert_eq!(actual.nlink(), expected.nlink());
    prop_assert_eq!(actual.created().ok(), expected.created().ok());
    prop_assert_eq!(actual.modified().ok(), expected.modified().ok());
    prop_assert_eq!(actual.accessed().ok(), expected.accessed().ok());
    #[cfg(unix)]
    {
        prop_assert_eq!(actual.raw.st_mode, expected.raw.st_mode);
        prop_assert_eq!(actual.raw.st_uid, expected.raw.st_uid);
        prop_assert_eq!(actual.raw.st_gid, expected.raw.st_gid);
        prop_assert_eq!(actual.raw.st_rdev, expected.raw.st_rdev);
        prop_assert_eq!(actual.raw.st_ctime, expected.raw.st_ctime);
        prop_assert_eq!(actual.raw.st_ctime_nsec, expected.raw.st_ctime_nsec);
        prop_assert_eq!(actual.handle(), expected.handle());
    }
    #[cfg(windows)]
    {
        prop_assert_eq!(actual.raw.FileAttributes, expected.raw.FileAttributes);
        prop_assert_eq!(actual.raw.ReparseTag, expected.raw.ReparseTag);
        prop_assert_eq!(actual.raw.AllocationSize, expected.raw.AllocationSize);
        prop_assert_eq!(actual.raw.ChangeTime, expected.raw.ChangeTime);
    }
    Ok(())
}

fn admission(
    required: u64,
    limit: u64,
    succeeded: bool,
    charged: u64,
) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(succeeded, limit >= required);
    prop_assert_eq!(charged, required.min(limit));
    Ok(())
}

fn direct_stat(root: &HostRoot, name: &str) -> Result<(HostStat, u64), Box<dyn std::error::Error>> {
    let work =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(error, _)| error)?;
    #[cfg(unix)]
    let stat = unix_name(OsStr::new(name), &mut &work, |name, work| {
        stat_at(root.directory.as_raw_fd(), name, work)
    })?;
    #[cfg(windows)]
    let stat = {
        let file = open_windows_at(
            &root.directory,
            &test_path(name),
            fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
            nt::FILE_OPEN,
            nt::FILE_SYNCHRONOUS_IO_NONALERT,
            &mut &work,
        )?;
        root.stat_file(&file, &mut &work)?
    };
    Ok((stat, work.receipt().backend_read_operations))
}

#[test]
fn flat_stat_matches_direct_leaf_and_exact_admission() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    for name in ["leaf", "éclair"] {
        std::fs::write(temporary.path().join(name), b"")?;
        std::fs::hard_link(
            temporary.path().join(name),
            temporary.path().join(format!("{name}-alias")),
        )?;
    }
    let root = test_root(temporary.path())?;
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // The full parent/leaf entry point must return the independently queried
    // record and admit exactly the retained leaf work, at every admission prefix.
    runner.run(
        &(any::<bool>(), any::<bool>(), 0_usize..257, 0_u64..8),
        |(unicode, alias, size, extra)| {
            let mut name = if unicode { "éclair" } else { "leaf" }.to_owned();
            if alias {
                name.push_str("-alias");
            }
            std::fs::write(temporary.path().join(&name), vec![0x5a; size])
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            let (expected, required) =
                direct_stat(&root, &name).map_err(|e| TestCaseError::fail(e.to_string()))?;
            let path = test_path(&name);
            for limit in (0..=required).chain([extra, u64::MAX]) {
                let budget = WorkBudget {
                    backend_read_operations: limit,
                    ..WorkBudget::UNBOUNDED
                };
                let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
                    .map_err(|(e, _)| TestCaseError::fail(e.to_string()))?;
                let result = root.stat(&path, &mut &work);
                admission(
                    required,
                    limit,
                    result.is_ok(),
                    work.receipt().backend_read_operations,
                )?;
                work.receipt()
                    .verify(budget)
                    .map_err(|e| TestCaseError::fail(e.to_string()))?;
                if let Ok(actual) = result {
                    same_stat(&actual, &expected)?;
                }
            }
            let legacy = NativeMaterializationWork::new(
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(e, _)| TestCaseError::fail(e.to_string()))?;
            // Real old-path control: a duplicate of this same authority precedes
            // the retained leaf operation. It must fail the exact-read oracle.
            let duplicate = HostRoot {
                directory: clone_directory(&root.directory, &mut &legacy)
                    .map_err(|e| TestCaseError::fail(e.to_string()))?,
                identity: root.identity(),
            };
            let actual = duplicate
                .stat(&path, &mut &legacy)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            same_stat(&actual, &expected)?;
            prop_assert!(
                admission(
                    required,
                    u64::MAX,
                    true,
                    legacy.receipt().backend_read_operations
                )
                .is_err()
            );
            let cancellation = crate::CancellationToken::new();
            let cancelled = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(e, _)| TestCaseError::fail(e.to_string()))?;
            cancellation.cancel();
            prop_assert!(root.stat(&path, &mut &cancelled).is_err());
            prop_assert_eq!(cancelled.receipt().backend_read_operations, 0);
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn flat_stat_uses_held_root_after_its_name_is_replaced() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("root");
    std::fs::create_dir(&original)?;
    std::fs::write(original.join("leaf"), b"original")?;
    let root = test_root(&original)?;
    let (expected, _) = direct_stat(&root, "leaf")?;
    std::fs::rename(&original, temporary.path().join("moved"))?;
    std::fs::create_dir(&original)?;
    std::fs::write(original.join("leaf"), b"replacement")?;
    let actual = root.stat(&test_path("leaf"), &mut ordinary_work())?;
    same_stat(&actual, &expected).map_err(|error| io::Error::other(error.to_string()))?;
    let replacement = test_root(&original)?;
    let wrong = replacement.stat(&test_path("leaf"), &mut ordinary_work())?;
    assert!(
        same_stat(&wrong, &expected).is_err(),
        "ambient re-resolution escaped detection"
    );
    Ok(())
}
