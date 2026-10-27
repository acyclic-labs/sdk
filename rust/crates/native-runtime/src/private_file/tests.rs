use super::*;
use std::io::{Read as _, Write as _};

#[cfg(unix)]
#[test]
fn credential_identity_requires_a_persistent_account_namespace() -> io::Result<()> {
    // Exercise the real account lookup without opening any credential or
    // creating a lock in the user's profile. Fixture custody remains local.
    let directory = unix_user_data_directory()?;
    assert_eq!(persistent_user_directory(&directory)?, directory);
    // The former production selector was exactly /tmp. It cannot pass the
    // same admission: PrivateTmp maps that name to a different inode per service.
    assert!(persistent_user_directory(Path::new("/tmp")).is_err());
    assert!(persistent_user_directory(Path::new("/tmp/service/home")).is_err());
    assert!(persistent_user_directory(Path::new("/var/tmp/service/home")).is_err());
    assert!(persistent_user_directory(Path::new("relative/home")).is_err());

    // The job-owned source directory provides a persistent fixture parent;
    // nothing is created in the OS account's actual profile. The old
    // home-only admission accepts this parent but misses the appended alias.
    let root = tempfile::Builder::new()
        .prefix(".custody-namespace-")
        .tempdir_in(std::env::current_dir()?)?;
    let home = fs::canonicalize(root.path())?;
    persistent_user_directory(&home)?;
    let data = home.join(".local/share");
    fs::create_dir_all(&data)?;
    let admitted = credential_directory(&data)?;
    assert_eq!(admitted, fs::canonicalize(data.join("acyclic.authority.signing-seed.v1"))?);
    fs::remove_dir(&admitted)?;
    fs::remove_dir(&data)?;
    let temporary = tempfile::tempdir()?;
    std::os::unix::fs::symlink(temporary.path(), &data)?;
    // This is the actual former admission boundary, and its unchanged parent
    // still passes. The full namespace must fail before any credential access.
    assert!(persistent_user_directory(&home).is_ok());
    let error = match credential_directory(&data) {
        Ok(_) => return Err(io::Error::other("temporary data alias was admitted")),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(fs::read_dir(temporary.path().join("acyclic.authority.signing-seed.v1"))?.count(), 0);
    Ok(())
}

#[test]
fn private_file_permissions_exist_before_any_secret_is_written() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("credential");
    let mut file = create(&path)?;
    assert!(is_private(&file)?);
    file.write_all(b"fixture")?;
    assert!(is_private(&open(&path, false)?)?);
    Ok(())
}

#[test]
fn every_publication_cut_exposes_only_complete_private_records() -> io::Result<()> {
    for failed in [
        PublicationStage::Created,
        PublicationStage::Synced,
        PublicationStage::Installed,
        PublicationStage::Published,
    ] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("owner");
        let temporary = root.path().join("staging");
        let result = publish(
            &path,
            &temporary,
            crate::RenameMode::NoReplace,
            |file| file.write_all(b"complete"),
            |stage| {
                if stage == failed {
                    Err(io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        assert!(!temporary.exists());
        if matches!(
            failed,
            PublicationStage::Installed | PublicationStage::Published
        ) {
            assert_eq!(fs::read(&path)?, b"complete");
            assert!(is_private(&open(&path, false)?)?);
        } else {
            assert!(!path.exists());
        }
    }
    Ok(())
}

#[test]
fn no_replace_and_staging_collision_preserve_the_original_publisher() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    let temporary = root.path().join("staging");
    create(&path)?.write_all(b"original")?;
    assert!(
        publish(
            &path,
            &temporary,
            crate::RenameMode::NoReplace,
            |file| file.write_all(b"replacement"),
            |_| Ok(())
        )
        .is_err()
    );
    assert_eq!(fs::read(&path)?, b"original");
    create(&temporary)?.write_all(b"another publisher")?;
    assert!(
        publish(
            &path,
            &temporary,
            crate::RenameMode::Replace,
            |_| panic!("foreign staging cannot be opened"),
            |_| Ok(())
        )
        .is_err()
    );
    assert_eq!(fs::read(&temporary)?, b"another publisher");
    Ok(())
}

#[test]
fn published_staging_name_reuse_survives_return_error_and_unwind() -> io::Result<()> {
    for outcome in 0..3 {
        let root = tempfile::tempdir()?;
        let path = root.path().join("owner");
        let temporary = root.path().join("staging");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            publish(
                &path,
                &temporary,
                crate::RenameMode::NoReplace,
                |file| file.write_all(b"original publisher"),
                |stage| {
                    if stage == PublicationStage::Published {
                        create(&temporary)?.write_all(b"next publisher")?;
                        match outcome {
                            1 => return Err(io::Error::other("lost publication acknowledgement")),
                            2 => panic!("unwind after staging name reuse"),
                            _ => {}
                        }
                    }
                    Ok(())
                },
            )
        }));
        match outcome {
            0 => assert!(matches!(result, Ok(Ok(())))),
            1 => assert!(matches!(result, Ok(Err(_)))),
            _ => assert!(result.is_err()),
        }
        assert!(
            temporary.is_file(),
            "published owner removed a reused staging name"
        );
        assert_eq!(fs::read(&temporary)?, b"next publisher");
        assert_eq!(fs::read(&path)?, b"original publisher");
        assert!(is_private(&open(&temporary, false)?)?);
    }
    Ok(())
}

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
fn bounded_reader_rejects_growth_and_admits_before_allocation() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    create(&path)?.write_all(b"original")?;
    assert!(
        read_with::<(), io::Error>(&path, 1, |_, _| panic!("oversize read was admitted")).is_err()
    );
    let result = read_with::<Vec<u8>, io::Error>(&path, 100, |file, length| {
        let mut bytes = Vec::with_capacity(usize::try_from(length).map_err(io::Error::other)? + 1);
        file.read_to_end(&mut bytes)?;
        open(&path, true)?.set_len(length + 1)?;
        Ok(bytes)
    });
    assert!(result.is_err());
    Ok(())
}

#[test]
fn links_cannot_turn_a_private_write_into_a_write_to_another_inode() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    let alias = root.path().join("alias");
    create(&path)?.write_all(b"original")?;
    fs::hard_link(&path, &alias)?;
    assert!(open(&path, true).is_err());
    assert!(open_or_create(&alias).is_err());
    assert_eq!(fs::read(&path)?, b"original");
    // Negative control: an ordinary path write follows the hard-link alias.
    fs::write(&alias, b"changed")?;
    assert_eq!(fs::read(&path)?, b"changed");
    Ok(())
}

#[cfg(unix)]
#[test]
fn public_modes_symlinks_and_nonregular_entries_fail_before_reading() -> io::Result<()> {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    create(&path)?.write_all(b"fixture")?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644))?;
    assert!(open(&path, false).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    let alias = root.path().join("alias");
    symlink(&path, &alias)?;
    assert!(open(&alias, false).is_err());
    assert!(open(root.path(), false).is_err());
    Ok(())
}

#[test]
fn negative_control_in_place_publication_exposes_a_torn_record() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    assert!(
        publish(
            &path,
            &path,
            crate::RenameMode::NoReplace,
            |_| Ok(()),
            |_| Ok(())
        )
        .is_err()
    );
    create(&path)?.write_all(b"half")?;
    assert_ne!(fs::read(&path)?, b"half-complete");
    Ok(())
}

#[test]
fn racing_original_publishers_reopen_one_complete_winner() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| -> io::Result<()> {
        let handles: Vec<_> = (0..2)
            .map(|index| {
                let path = &path;
                let barrier = &barrier;
                let temporary = root.path().join(format!("publisher-{index}"));
                scope.spawn(move || {
                    barrier.wait();
                    publish_new(
                        path,
                        &temporary,
                        |file| file.write_all(&[index]),
                        |_| Ok(()),
                    )
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<io::Result<_>>()?;
        assert_eq!(outcomes.iter().filter(|created| **created).count(), 1);
        let winner = read(&path, 1)?;
        assert_eq!(winner.0.len(), 1);
        let index = usize::from(*winner.0.first().ok_or(io::ErrorKind::InvalidData)?);
        assert_eq!(outcomes.get(index), Some(&true));
        Ok(())
    })
}

#[test]
fn private_directories_and_staging_ownership_share_the_native_boundary() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let nested = root.path().join("new").join("private");
    create_directory_all(&nested)?;
    validate_directory(&nested)?;
    let path;
    {
        let mut staged = StagedFile::create(&nested, ".credential-")?;
        path = staged.path.clone();
        assert!(is_private(staged.as_file())?);
        staged.as_file_mut().write_all(b"fixture")?;
    }
    assert!(!path.exists());
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

#[test]
fn a_staging_collision_cannot_claim_that_an_original_owner_was_published() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let destination = directory.path().join("owner");
    let staging = directory.path().join("occupied");
    let _held = super::create(&staging)?;
    assert!(!destination.exists());
    // Negative control: interpreting every AlreadyExists as a published winner
    // accepts the occupied staging pathname while no owner record exists.
    let unchecked_winner = super::publish(
        &destination,
        &staging,
        crate::RenameMode::NoReplace,
        |_| Ok(()),
        |_| Ok(()),
    )
    .is_err_and(|error| error.kind() == io::ErrorKind::AlreadyExists);
    assert!(unchecked_winner);
    assert!(super::publish_new(&destination, &staging, |_| Ok(()), |_| Ok(())).is_err());
    assert!(!destination.exists());
    Ok(())
}

#[cfg(windows)]
#[test]
fn inherited_owner_upgrade_tightens_before_read_and_rejects_live_or_aliased_files() -> io::Result<()>
{
    let directory = tempfile::tempdir()?;
    let private = directory.path().join("private");
    create_directory_all(&private)?;
    let path = private.join("legacy-owner");
    let mut writer = File::create(&path)?;
    writer.write_all(b"original identity")?;
    // The old strict path cannot open inherited legacy records. A live writer
    // also prevents the guarded upgrade from acquiring exclusive ownership.
    assert!(read(&path, 100).is_err());
    assert!(
        read_for_upgrade_with::<(), io::Error>(&path, 100, |_, _| panic!("live writer admitted"))
            .is_err()
    );
    drop(writer);
    let alias = directory.path().join("alias");
    fs::hard_link(&path, &alias)?;
    assert!(
        read_for_upgrade_with::<(), io::Error>(&path, 100, |_, _| panic!("alias admitted"))
            .is_err()
    );
    fs::remove_file(alias)?;
    let bytes = read_for_upgrade_with(&path, 100, |file, _| {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok::<_, io::Error>(bytes)
    })?;
    assert_eq!(bytes, b"original identity");
    assert!(is_private(&open(&path, false)?)?);
    assert_eq!(read(&path, 100)?.0, bytes);
    Ok(())
}
