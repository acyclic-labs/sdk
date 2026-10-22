use super::*;

#[test]
fn file_opens_check_kind_and_reparse_during_the_open_for_every_disposition()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    let variants = [
        (NativeOpenOptions::Read, false, false),
        (NativeOpenOptions::ReadWrite, false, false),
        (NativeOpenOptions::CreateNew, true, true),
        #[cfg(feature = "local")]
        (NativeOpenOptions::ReadWriteCreateNew, true, true),
        (NativeOpenOptions::ReadWriteCreate, true, false),
    ];
    for (index, (options, creates, exclusive)) in variants.iter().enumerate() {
        for existing in [false, true] {
            let name = format!("file-é&%-{index}-{existing}");
            let path = temporary.path().join(&name);
            if existing {
                std::fs::write(&path, b"preserved")?;
            }
            let mut work = ordinary_work();
            let (result, events) = acyclic_test_support::differential::observe::<_, _, &'static str>(
                || stat_probe::Probe::rescan(false),
                |probe| {
                    let result = open_leaf(&root.directory, Path::new(&name), options, &mut work);
                    probe.publish();
                    result
                },
            );
            assert_eq!(events, vec!["NtCreateFile"]);
            assert_eq!(
                result.is_ok(),
                if *exclusive {
                    !existing
                } else {
                    existing || *creates
                }
            );
            assert_eq!(work.receipt().backend_read_operations, u64::from(!*creates));
            assert_eq!(work.receipt().backend_write_operations, u64::from(*creates));
            if let Err(DemandError::Io(error)) = &result {
                assert_eq!(
                    error.kind(),
                    if existing {
                        io::ErrorKind::AlreadyExists
                    } else {
                        io::ErrorKind::NotFound
                    }
                );
            }
            if let Ok(file) = result {
                assert!(file.metadata()?.is_file());
                drop(file);
                assert_eq!(
                    std::fs::read(&path)?,
                    if existing {
                        b"preserved".as_slice()
                    } else {
                        b""
                    }
                );
            }
            if existing {
                assert_eq!(std::fs::read(&path)?, b"preserved");
            }
        }
        let directory = format!("directory-{index}");
        std::fs::create_dir(temporary.path().join(&directory))?;
        assert!(
            open_leaf(
                &root.directory,
                Path::new(&directory),
                options,
                &mut ordinary_work()
            )
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn file_opens_preserve_budget_and_unspent_cancellation_checks()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    std::fs::write(temporary.path().join("existing"), b"bytes")?;
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
            let options = if create {
                NativeOpenOptions::ReadWriteCreate
            } else {
                NativeOpenOptions::Read
            };
            assert_eq!(
                open_leaf(&root.directory, Path::new("existing"), &options, &mut &work).is_ok(),
                limit == 1
            );
            assert_eq!(
                work.receipt().backend_read_operations,
                if create { 0 } else { limit }
            );
            assert_eq!(
                work.receipt().backend_write_operations,
                if create { limit } else { 0 }
            );
            work.receipt().verify(budget)?;
        }
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        cancellation.cancel();
        let options = if create {
            NativeOpenOptions::ReadWriteCreate
        } else {
            NativeOpenOptions::Read
        };
        assert!(open_leaf(&root.directory, Path::new("existing"), &options, &mut &work).is_err());
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
    }
    assert_eq!(std::fs::read(temporary.path().join("existing"))?, b"bytes");
    Ok(())
}

#[test]
fn file_opens_reject_final_links_without_changing_the_target()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    let target = temporary.path().join("target");
    let link = temporary.path().join("link");
    std::fs::write(&target, b"unchanged")?;
    match std::os::windows::fs::symlink_file(&target, &link) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    assert_eq!(std::fs::read(&link)?, b"unchanged");
    for options in [
        NativeOpenOptions::Read,
        NativeOpenOptions::ReadWrite,
        NativeOpenOptions::CreateNew,
        #[cfg(feature = "local")]
        NativeOpenOptions::ReadWriteCreateNew,
        NativeOpenOptions::ReadWriteCreate,
    ] {
        let result = open_leaf(
            &root.directory,
            Path::new("link"),
            &options,
            &mut ordinary_work(),
        );
        match options {
            NativeOpenOptions::CreateNew => assert!(
                matches!(result, Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists)
            ),
            #[cfg(feature = "local")]
            NativeOpenOptions::ReadWriteCreateNew => assert!(
                matches!(result, Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists)
            ),
            _ => assert!(matches!(result, Err(DemandError::InvalidRequest))),
        }
    }
    assert!(std::fs::symlink_metadata(&link)?.is_symlink());
    assert_eq!(std::fs::read(&target)?, b"unchanged");
    Ok(())
}
