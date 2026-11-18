use super::*;
use std::io::Write as _;

#[cfg(feature = "test-support")]
#[test]
fn rename_custody_transfer_precedes_a_lost_acknowledgement() -> io::Result<()> {
    use crate::testing::{DurableFault, Schedule};
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    for fault in [DurableFault::Before, DurableFault::After] {
        let root = tempfile::tempdir()?;
        let from = root.path().join("staging");
        let to = root.path().join("owner");
        create(&from)?.write_all(b"original publisher")?;
        let (schedule, _) = Schedule::seeded(71);
        schedule.cut_at(0, fault);
        let mut committed = false;
        let mut recreated = Ok(());
        let result = runtime.block_on(schedule.scope(async {
            crate::namespace::durable_rename_observed(
                &from,
                &to,
                crate::RenameMode::NoReplace,
                None,
                || {
                    committed = true;
                    recreated =
                        create(&from).and_then(|mut file| file.write_all(b"next publisher"));
                },
            )
        }));
        assert!(result.is_err());
        recreated?;
        assert_eq!(committed, fault == DurableFault::After);
        if committed {
            assert_eq!(fs::read(&from)?, b"next publisher");
            assert_eq!(fs::read(&to)?, b"original publisher");
        } else {
            assert_eq!(fs::read(&from)?, b"original publisher");
            assert!(!to.exists());
        }
    }
    Ok(())
}

#[test]
fn private_directories_and_staging_ownership_share_the_native_boundary() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    #[cfg(windows)]
    let directory = root
        .path()
        .join("a".repeat(90))
        .join("b".repeat(90))
        .join("c".repeat(90));
    #[cfg(not(windows))]
    let directory = root.path().to_path_buf();
    let nested = directory.join("new").join("private");
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        assert!(nested.as_os_str().encode_wide().count() > 260);
        assert!(
            !nested
                .as_os_str()
                .encode_wide()
                .take(4)
                .eq([92, 92, 63, 92]),
            "control requires an ordinary DOS path"
        );
    }
    create_directory_all(&nested)?;
    validate_directory(&nested)?;
    #[cfg(windows)]
    let directories = [nested.clone(), nested.canonicalize()?];
    #[cfg(not(windows))]
    let directories = [nested];
    for directory in directories {
        create_directory_all(&directory)?;
        let path;
        {
            let mut staged = StagedFile::create(&directory, ".credential-")?;
            path = staged.path.clone();
            assert!(is_private(staged.as_file())?);
            assert_eq!(
                create(&path).unwrap_err().kind(),
                io::ErrorKind::AlreadyExists
            );
            staged.as_file_mut().write_all(b"fixture")?;
            assert!(is_private(&open(&path, false)?)?);
        }
        assert!(!path.exists());
    }
    Ok(())
}

#[test]
fn native_error_kinds_preserve_shared_create_and_reopen_semantics() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("original");
    let _original = create(&path)?;
    assert_eq!(
        create(&path).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(is_private(&open_or_create(&path)?)?);
    assert_eq!(
        create(&root.path().join("missing").join("child"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    #[cfg(windows)]
    {
        let hresult = io::Error::from_raw_os_error(0x8007_0003_u32.cast_signed());
        assert_ne!(
            hresult.kind(),
            io::ErrorKind::NotFound,
            "negative control: HRESULT cannot drive the shared parent creation branch"
        );
        assert_eq!(windows::io_error(hresult).kind(), io::ErrorKind::NotFound);
    }
    Ok(())
}
