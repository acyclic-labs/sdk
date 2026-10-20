use super::*;

#[cfg(target_os = "linux")]
#[test]
fn held_canonical_directory_does_not_scan_unrelated_siblings()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    for index in 0..128 {
        std::fs::create_dir(temporary.path().join(format!("sibling-{index}")))?;
    }
    let wanted = temporary.path().join("wanted");
    std::fs::create_dir(&wanted)?;
    let directory = Dir::open_ambient_dir(&wanted, cap_std::ambient_authority())?;
    let cancellation = crate::CancellationToken::new();
    let mut work = DemandWork::new(
        WorkBudget {
            backend_read_operations: 8,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let (actual, allocation) = canonical_directory_path_held(directory, &mut work)?;
    assert_eq!(actual, std::fs::canonicalize(&wanted)?);
    assert_eq!(work.work.backend_read_operations, 4);
    drop(actual);
    work.release(allocation)?;
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[cfg(unix)]
#[test]
fn held_canonical_directory_observes_renamed_object_not_replaced_spelling()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("original");
    let moved = temporary.path().join("moved");
    std::fs::create_dir(&original)?;
    let directory = Dir::open_ambient_dir(&original, cap_std::ambient_authority())?;
    std::fs::rename(&original, &moved)?;
    std::fs::create_dir(&original)?;
    let mut work = ordinary_work();
    let (actual, allocation) = canonical_directory_path_held(directory, &mut work)?;
    assert_eq!(actual, std::fs::canonicalize(&moved)?);
    assert_ne!(actual, std::fs::canonicalize(&original)?);
    assert_eq!(work.live_allocation_bytes(), actual.capacity() as u64);
    drop(actual);
    work.release(allocation)?;
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn held_canonical_directory_needs_search_permission_without_directory_read_permission()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt as _;
    let temporary = tempfile::tempdir()?;
    let wanted = temporary.path().join("execute-only");
    std::fs::create_dir(&wanted)?;
    let expected = wanted.canonicalize()?;
    let mut work = ordinary_work();
    let directory = unix_name(wanted.as_os_str(), &mut work, |name, work| {
        open_unix(libc::AT_FDCWD, name, cwd::cwd_metadata_flags(), work).map(Dir::from_std_file)
    })?;
    std::fs::set_permissions(&wanted, std::fs::Permissions::from_mode(0o100))?;
    let result = canonical_directory_path_held(directory, &mut work);
    std::fs::set_permissions(&wanted, std::fs::Permissions::from_mode(0o700))?;
    let (actual, allocation) = result?;
    assert_eq!(actual, expected);
    drop(actual);
    work.release(allocation)?;
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[cfg(windows)]
#[test]
fn canonical_wide_directory_retains_native_capacity_without_scalar_conversion()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
    let temporary = tempfile::tempdir()?;
    let input = temporary.path().join(OsString::from_wide(&[0xD800, 120]));
    std::fs::create_dir(&input)?;
    let expected = std::fs::canonicalize(&input)?;
    let expected: Vec<u16> = expected.as_os_str().encode_wide().collect();
    let directory = Dir::open_ambient_dir(&input, cap_std::ambient_authority())?;
    let mut work = ordinary_work();
    let (actual, allocation) = canonical_directory_wide(&directory, &mut work)?;
    assert_eq!(actual, expected);
    assert_ne!(actual.last(), Some(&0));
    assert_eq!(work.live_allocation_bytes(), actual.capacity() as u64 * 2);
    drop(actual);
    work.release(allocation)?;
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[test]
fn held_child_uses_held_parent_after_lexical_replacement() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("original");
    std::fs::create_dir(&original)?;
    std::fs::create_dir(original.join("child"))?;
    let parent = test_root(&original)?;
    let expected = test_root(&original.join("child"))?.identity();
    std::fs::rename(&original, temporary.path().join("moved"))?;
    std::fs::create_dir(&original)?;
    std::fs::create_dir(original.join("child"))?;
    let mut work = ordinary_work();
    let child = test_path("child");
    #[cfg(windows)]
    let child = &child;
    assert_eq!(parent.open_child(child, &mut work)?.identity(), expected);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[cfg(unix)]
#[test]
fn held_child_refuses_symlinks_and_nonleaf_names() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::create_dir(temporary.path().join("child"))?;
    std::os::unix::fs::symlink("child", temporary.path().join("link"))?;
    let parent = test_root(temporary.path())?;
    for name in ["link", "..", "child/../child"] {
        let mut work = ordinary_work();
        assert!(parent.open_child(Path::new(name), &mut work).is_err());
        assert_eq!(work.live_allocation_bytes(), 0);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn held_ancestry_survives_aliases_and_lexical_parent_replacement()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("parent");
    std::fs::create_dir(&path)?;
    std::fs::create_dir(path.join("child"))?;
    std::os::unix::fs::symlink(&path, temporary.path().join("alias"))?;
    let parent = test_root(&path)?.identity();
    let child = Dir::open_ambient_dir(
        temporary.path().join("alias/child"),
        cap_std::ambient_authority(),
    )?;
    let identity = directory_identity(&child, &mut ordinary_work())?;
    std::fs::rename(&path, temporary.path().join("moved"))?;
    std::fs::create_dir(&path)?;
    let replacement = test_root(&path)?.identity();
    let mut work = DemandWork::new(
        WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    );
    assert!(directory_has_ancestor(&child, identity, &mut work)?);
    assert!(directory_has_ancestor(&child, parent, &mut work)?);
    assert!(!directory_has_ancestor(&child, replacement, &mut work)?);
    assert_eq!(work.live_allocation_bytes(), 0);
    Ok(())
}

#[test]
fn held_scanners_keep_independent_cursors_and_scoped_native_allocations()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("kept"), b"data")?;
    let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    let budget = WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
            + 2 * HostStatReader::BUFFER_BYTES as u64
            + 64,
        ..WorkBudget::UNBOUNDED
    };
    let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
        .map_err(|(cause, _)| cause)?;
    let (mut first, first_allocation) = read_dir_held(&directory, &mut &work)?;
    let (mut second, second_allocation) = read_dir_held(&directory, &mut &work)?;
    let first_entry = first.next(&mut &work)?.unwrap();
    let second_entry = second.next(&mut &work)?.unwrap();
    assert_eq!(first_entry.0.name, second_entry.0.name);
    assert!(first_entry.0.stat.is_some());
    assert!(second_entry.0.stat.is_some());
    drop((
        first_entry,
        second_entry,
        first,
        first_allocation,
        second,
        second_allocation,
    ));
    let (live, _) = work.allocations(|ledger, _, _, _| {
        Ok::<_, NativeTreeMaterializationError>(ledger.live_bytes())
    })?;
    assert_eq!(live, NativeMaterializationWork::accounting_state_bytes());
    Ok(())
}

#[test]
fn a_record_survives_metadata_admission_failure_without_another_refill()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("kept"), b"data")?;
    let root = test_root(temporary.path())?;
    let mut first = ordinary_work();
    let path = test_path("");
    #[cfg(windows)]
    let path = &path;
    let (mut reader, _) = root.read_dir_stats(path, &mut first)?;
    first.budget.backend_read_operations = first.work.backend_read_operations + 1;
    assert!(matches!(
        reader.next(&mut first),
        Err(DemandError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_read_operations",
            ..
        }))
    ));
    let mut retry = ordinary_work();
    retry.claim_allocation(reader.logical_owned_bytes(), 0, WorkCounters::UNCHARGED)?;
    let (entry, _) = reader.next(&mut retry)?.expect("denied record survives");
    #[cfg(unix)]
    assert_eq!(entry.name, OsStr::new("kept"));
    #[cfg(windows)]
    assert_eq!(entry.name, test_path("kept"));
    assert!(entry.stat.is_some());
    Ok(())
}
