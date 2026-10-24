use super::*;

#[test]
fn component_opens_respect_creation_read_and_cancellation_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    std::fs::create_dir(temporary.path().join("existing"))?;
    for create in [false, true] {
        for limit in 0..=1 {
            let budget = WorkBudget {
                backend_read_operations: if create { 0 } else { limit },
                backend_write_operations: if create { limit } else { 0 },
                ..WorkBudget::UNBOUNDED
            };
            let cancellation = crate::CancellationToken::new();
            let work = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let result =
                directory_path(&root.directory, &test_path("existing"), create, &mut &work);
            assert_eq!(result.is_ok(), limit == 1);
            assert_eq!(
                work.receipt().backend_read_operations,
                if create { 0 } else { limit }
            );
            assert_eq!(
                work.receipt().backend_write_operations,
                if create { limit } else { 0 }
            );
            work.receipt().verify(budget)?;
            cancellation.cancel();
            let before = work.receipt();
            assert!(
                directory_path(&root.directory, &test_path("existing"), create, &mut &work,)
                    .is_err()
            );
            assert_eq!(work.receipt(), before);
        }
        // Cancellation must also stop a fresh ledger with room to do the IO.
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        cancellation.cancel();
        assert!(
            directory_path(&root.directory, &test_path("existing"), create, &mut &work).is_err()
        );
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
    }
    Ok(())
}

#[test]
fn component_opens_preserve_identity_and_exact_work_for_both_dispositions()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    for create in [false, true] {
        let name = if create { "créated&%" } else { "existing" };
        if !create {
            std::fs::create_dir(temporary.path().join(name))?;
        }
        let path = test_path(name);
        let mut work = ordinary_work();
        let (result, events) = acyclic_test_support::differential::observe::<_, _, &'static str>(
            || stat_probe::Probe::rescan(false),
            |probe| {
                let result = directory_path(&root.directory, &path, create, &mut work);
                probe.publish();
                result
            },
        );
        let held = result?;
        assert_eq!(events, vec!["NtCreateFile"]);
        let receipt = work.receipt();
        assert_eq!(receipt.backend_read_operations, u64::from(!create));
        assert_eq!(receipt.backend_write_operations, u64::from(create));
        assert_eq!(
            directory_identity(&held, &mut ordinary_work())?,
            test_root(&temporary.path().join(name))?.identity()
        );
        let mut control = ordinary_work();
        let (result, events) = acyclic_test_support::differential::observe::<_, _, &'static str>(
            || stat_probe::Probe::rescan(false),
            |probe| {
                let result = open_windows_directory(&root.directory, &path, create, &mut control);
                probe.publish();
                result
            },
        );
        let (_, identity) = result?;
        assert_eq!(events, vec!["NtCreateFile", "GetFileInformationByHandle"]);
        assert_eq!(identity, directory_identity(&held, &mut ordinary_work())?);
        // The frozen open-then-query route is the negative physical-work control.
        assert_eq!(
            control.receipt().backend_read_operations,
            receipt.backend_read_operations + 1
        );
    }
    Ok(())
}

#[test]
fn component_opens_reject_real_junctions_at_every_depth_and_disposition()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let inside = temporary.path().join("inside");
    let outside = temporary.path().join("outside");
    std::fs::create_dir(&inside)?;
    std::fs::create_dir(&outside)?;
    std::fs::create_dir(outside.join("child"))?;
    std::fs::write(outside.join("authored"), b"unchanged")?;
    crate::windows_test_filesystems::junction(&inside.join("alias"), &outside)?;
    let root = test_root(&inside)?;
    assert!(std::fs::read_dir(inside.join("alias/child")).is_ok());
    for create in [false, true] {
        for name in ["alias", r"alias\child", r"alias\new-child"] {
            let path = test_path(name);
            assert!(directory_path(&root.directory, &path, create, &mut ordinary_work()).is_err());
            assert!(
                root.create_directories(Path::new(name), &mut ordinary_work())
                    .is_err()
            );
        }
    }
    assert!(!outside.join("new-child").exists());
    assert_eq!(std::fs::read(outside.join("authored"))?, b"unchanged");
    std::fs::remove_dir(inside.join("alias"))?;
    Ok(())
}

#[test]
fn component_opens_keep_each_ancestors_original_access_check()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let parent = temporary.path().join("parent");
    std::fs::create_dir(&parent)?;
    std::fs::create_dir(parent.join("child"))?;
    let root = test_root(temporary.path())?;
    let acl = |arguments: &[&str]| -> Result<(), Box<dyn std::error::Error>> {
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new("icacls.exe")
                .arg(&parent)
                .args(arguments),
            std::time::Duration::from_secs(5),
            64 * 1024,
        )?;
        assert!(output.status.success(), "icacls: {output:?}");
        Ok(())
    };
    // Only this ancestor denies synchronize. A whole-path shortcut could skip
    // its read-access check and reach the independently accessible child.
    acl(&["/deny", "*S-1-1-0:(S)"])?;
    let unchecked = std::fs::read_dir(parent.join("child"));
    let results = [false, true].map(|create| {
        directory_path(
            &root.directory,
            &test_path(r"parent\child"),
            create,
            &mut ordinary_work(),
        )
    });
    let held_creation = root.create_directories(Path::new(r"parent\child"), &mut ordinary_work());
    acl(&["/reset"])?;
    assert!(
        unchecked.is_ok(),
        "whole-path negative control: {unchecked:?}"
    );
    for result in results {
        assert!(matches!(result, Err(DemandError::Io(error))
            if error.kind() == io::ErrorKind::PermissionDenied));
    }
    assert!(matches!(held_creation, Err(DemandError::Io(error))
        if error.kind() == io::ErrorKind::PermissionDenied));
    Ok(())
}

#[test]
fn held_creation_queries_only_the_returned_identity_without_dropping_barriers()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    for depth in 1..=4 {
        let relative: std::path::PathBuf = (0..depth).map(|_| "nested").collect();
        // Exercise both creation and reopening concurrently publishable names.
        for _ in 0..2 {
            let mut work = ordinary_work();
            let (created, events) = acyclic_test_support::differential::observe::<_, _, &'static str>(
                || stat_probe::Probe::rescan(false),
                |probe| {
                    let created = root.create_directories(&relative, &mut work);
                    probe.publish();
                    created
                },
            );
            let created = created?;
            assert_eq!(
                events
                    .iter()
                    .filter(|event| **event == "GetFileInformationByHandle")
                    .count(),
                1
            );
            let receipt = work.receipt();
            assert_eq!(receipt.durability_operations, depth);
            assert_eq!(receipt.backend_write_operations, depth);
            // One writable parent open per barrier and one final identity query.
            assert_eq!(receipt.backend_read_operations, depth + 1);
            assert_eq!(
                created.identity(),
                test_root(&temporary.path().join(&relative))?.identity()
            );

            // The former shape queries, then discards, every parent's identity.
            let mut old = ordinary_work();
            let mut held: Option<Dir> = None;
            for component in relative.components() {
                let directory = held.as_ref().unwrap_or(&root.directory);
                let (child, _) =
                    with_wide_path(Path::new(component.as_os_str()), &mut old, |name, work| {
                        open_windows_directory(directory, name, true, work)
                    })?;
                sync_directory_held(directory, &mut old)?;
                held = Some(child);
            }
            assert_eq!(old.receipt().durability_operations, depth);
            assert_eq!(old.receipt().backend_read_operations, 2 * depth);
            if depth > 1 {
                assert!(old.receipt().backend_read_operations > receipt.backend_read_operations);
            }
        }
    }
    Ok(())
}
