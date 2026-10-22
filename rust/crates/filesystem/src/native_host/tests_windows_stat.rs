use super::*;
use proptest::prelude::*;

fn fields(stat: &HostStat) -> (u32, [i64; 7], [u32; 4]) {
    let raw = stat.raw;
    (
        stat.volume_serial_number,
        [
            raw.FileId,
            raw.CreationTime,
            raw.LastAccessTime,
            raw.LastWriteTime,
            raw.ChangeTime,
            raw.AllocationSize,
            raw.EndOfFile,
        ],
        [
            raw.FileAttributes,
            raw.ReparseTag,
            raw.NumberOfLinks,
            raw.EffectiveAccess,
        ],
    )
}

fn admitted(expected: u64, actual: u64) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn source_stat_matches_every_held_field_and_admission_prefix()
-> Result<(), Box<dyn std::error::Error>> {
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
    // The source path must preserve every native metadata field, and each
    // attempted read must be recorded, including the two-read fallback.
    runner.run(
        &(any::<bool>(), any::<bool>(), any::<bool>(), 0_usize..81),
        |(unicode, alias, native, size)| {
            let base = if unicode { "éclair" } else { "leaf" };
            let name = if alias {
                format!("{base}-alias")
            } else {
                base.to_owned()
            };
            std::fs::write(temporary.path().join(&name), vec![0x5a; size])
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let path: Vec<_> = name.encode_utf16().collect();
            let mut legacy = ordinary_work();
            let expected = root
                .stat(&path, &mut legacy)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let parent_reads = legacy.receipt().backend_read_operations - 2;
            for limit in (0..=parent_reads + 3).chain([u64::MAX]) {
                let budget = WorkBudget {
                    backend_read_operations: limit,
                    ..WorkBudget::UNBOUNDED
                };
                let mut work = DemandWork::new(budget, &crate::CancellationToken::new());
                let original =
                    NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
                        .map_err(|(error, _)| TestCaseError::fail(error.to_string()))?;
                let (result, receipt) = if native {
                    (
                        root.stat_source(&path, &mut original.clone()),
                        original.receipt(),
                    )
                } else {
                    (root.stat_source(&path, &mut work), work.receipt())
                };
                prop_assert_eq!(result.is_ok(), limit > parent_reads);
                let retained = if limit.saturating_sub(parent_reads) == 2 {
                    2
                } else {
                    1
                };
                admitted(
                    limit.min(parent_reads + retained),
                    receipt.backend_read_operations,
                )?;
                receipt
                    .verify(budget)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                if let Ok(actual) = result {
                    prop_assert_eq!(fields(&actual), fields(&expected));
                }
            }
            // The retained held-leaf implementation must fail the one-query oracle.
            prop_assert!(
                admitted(parent_reads + 1, legacy.receipt().backend_read_operations).is_err()
            );
            let mut wrong = expected;
            wrong.raw.NumberOfLinks ^= 1;
            prop_assert_ne!(
                fields(&wrong),
                fields(&expected),
                "lost hard-link metadata escaped detection"
            );
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn unsupported_stat_queries_charge_attempts_and_reject_the_refund_control()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("leaf"), b"payload")?;
    let root = test_root(temporary.path())?;
    let volume = u32::try_from(root.identity().device)?;
    let path: Vec<_> = "leaf".encode_utf16().collect();
    for code in [None, Some(1), Some(50), Some(87), Some(124)] {
        for reads in 0..=4 {
            for broken in [false, true] {
                let budget = WorkBudget {
                    backend_read_operations: reads,
                    ..WorkBudget::UNBOUNDED
                };
                let mut work = ordinary_work();
                let name = windows_open_name(&path, &mut work)?;
                let mut limited = DemandWork::new(budget, &crate::CancellationToken::new());
                let attempted = std::cell::Cell::new(0_u64);
                let actual = query_windows_stat(
                    &mut limited,
                    if broken {
                        WorkCounters::UNCHARGED
                    } else {
                        READ
                    },
                    || {
                        attempted.set(attempted.get() + 1);
                        match code {
                            None => Ok(None),
                            Some(code) => Err(io::Error::from_raw_os_error(code)),
                        }
                    },
                    |work| {
                        let held = open_windows_named(
                            &root.directory,
                            &name,
                            fs::FILE_READ_ATTRIBUTES | fs::SYNCHRONIZE,
                            nt::FILE_OPEN,
                            nt::FILE_SYNCHRONOUS_IO_NONALERT,
                            work,
                        )?;
                        attempted.set(attempted.get() + 1);
                        let stat = stat_windows_file(&held, volume, work);
                        if stat.is_ok() {
                            attempted.set(attempted.get() + 1);
                        }
                        stat
                    },
                );
                if broken {
                    assert!(
                        admitted(attempted.get(), limited.receipt().backend_read_operations)
                            .is_err(),
                        "uncharged query escaped detection"
                    );
                } else {
                    admitted(reads.min(3), limited.receipt().backend_read_operations)
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    assert_eq!(actual.is_ok(), reads >= 3);
                    assert_eq!(attempted.get(), reads.min(3));
                }
                limited.receipt().verify(budget)?;
            }
        }
    }
    Ok(())
}

#[test]
fn source_stat_keeps_root_and_nested_path_authority() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("root");
    std::fs::create_dir(&original)?;
    std::fs::create_dir(original.join("nested"))?;
    std::fs::write(original.join("nested/leaf"), b"original")?;
    let root = test_root(&original)?;
    let path: Vec<_> = "nested\\leaf".encode_utf16().collect();
    let expected = root.stat(&path, &mut ordinary_work())?;
    std::fs::rename(&original, temporary.path().join("moved"))?;
    std::fs::create_dir(&original)?;
    std::fs::create_dir(original.join("nested"))?;
    std::fs::write(original.join("nested/leaf"), b"replacement")?;
    assert_eq!(
        fields(&root.stat_source(&path, &mut ordinary_work())?),
        fields(&expected)
    );
    let replacement = test_root(&original)?;
    assert_ne!(
        fields(&replacement.stat_source(&path, &mut ordinary_work())?),
        fields(&expected)
    );
    for name in ["", ".", "nested\\."] {
        let path: Vec<_> = name.encode_utf16().collect();
        assert_eq!(
            fields(&root.stat_source(&path, &mut ordinary_work())?),
            fields(&root.stat(&path, &mut ordinary_work())?)
        );
    }
    for name in [
        "nested\\..\\leaf",
        "nested\\leaf:stream",
        "nested\\bad/leaf",
        "nested\\bad\0leaf",
    ] {
        let path: Vec<_> = name.encode_utf16().collect();
        assert!(root.stat_source(&path, &mut ordinary_work()).is_err());
    }
    Ok(())
}

#[test]
fn source_stat_preserves_a_denied_synchronize_open() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("denied");
    std::fs::write(&path, b"secret")?;
    let root = test_root(temporary.path())?;
    let acl = |args: &[&str]| -> Result<(), Box<dyn std::error::Error>> {
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new("icacls.exe")
                .arg(&path)
                .args(args),
            std::time::Duration::from_secs(5),
            64 * 1024,
        )?;
        assert!(output.status.success(), "icacls failed: {output:?}");
        Ok(())
    };
    acl(&["/deny", "*S-1-1-0:(S)"])?;
    let name: Vec<_> = "denied".encode_utf16().collect();
    let legacy = root.stat(&name, &mut ordinary_work());
    let actual = root.stat_source(&name, &mut ordinary_work());
    let volume = u32::try_from(root.identity().device)?;
    let unchecked = stat_windows_name(
        &root.directory,
        &windows_open_name(&name, &mut ordinary_work())?,
        volume,
        true,
    );
    acl(&["/reset"])?;
    // The deliberately unchecked query succeeds despite the denied old open.
    let unchecked = unchecked?.ok_or("negative control did not return metadata")?;
    assert_eq!(unchecked.raw.EffectiveAccess & fs::SYNCHRONIZE.0, 0);
    assert!(matches!(legacy, Err(DemandError::Io(ref error))
        if error.kind() == io::ErrorKind::PermissionDenied));
    assert!(matches!(actual, Err(DemandError::Io(ref error))
        if error.kind() == io::ErrorKind::PermissionDenied));
    Ok(())
}

#[test]
fn source_stat_observes_a_reparse_leaf_and_rejects_traversal()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let inside = temporary.path().join("inside");
    let outside = temporary.path().join("outside");
    std::fs::create_dir(&inside)?;
    std::fs::create_dir(&outside)?;
    std::fs::write(outside.join("leaf"), b"outside")?;
    let junction = inside.join("junction");
    crate::windows_test_filesystems::junction(&junction, &outside)?;
    let root = test_root(&inside)?;
    let leaf: Vec<_> = "junction".encode_utf16().collect();
    let nested: Vec<_> = "junction\\leaf".encode_utf16().collect();
    let expected = root.stat(&leaf, &mut ordinary_work())?;
    let actual = root.stat_source(&leaf, &mut ordinary_work());
    let escaped = root.stat_source(&nested, &mut ordinary_work());
    std::fs::remove_dir(&junction)?;
    assert_eq!(fields(&actual?), fields(&expected));
    assert_eq!(expected.raw.ReparseTag, 0xa0000003);
    assert!(escaped.is_err());
    assert_eq!(std::fs::read(outside.join("leaf"))?, b"outside");
    Ok(())
}
