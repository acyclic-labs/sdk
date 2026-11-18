use acyclic_native_runtime::RenameMode;
use acyclic_native_runtime::private_file::{
    PublicationStage, StagedFile, create, is_private, open, open_or_create, publish, publish_new,
    read, read_with,
};
use std::fs;
use std::io::{self, Read as _, Seek as _, Write as _};

#[test]
fn every_publication_cut_exposes_only_complete_private_records() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    for (index, failed) in [
        None,
        Some(PublicationStage::Created),
        Some(PublicationStage::Synced),
        Some(PublicationStage::Installed),
        Some(PublicationStage::Published),
    ]
    .into_iter()
    .enumerate()
    {
        let path = root.path().join(format!("owner-{index}"));
        let temporary = root.path().join(format!("staging-{index}"));
        let result = publish(
            &path,
            &temporary,
            RenameMode::NoReplace,
            |file| {
                assert!(is_private(file)?);
                file.write_all(b"com")?;
                assert!(!path.exists(), "partial private record became visible");
                if failed.is_none() {
                    return Err(io::ErrorKind::Interrupted.into());
                }
                file.write_all(b"plete")
            },
            |stage| {
                if Some(stage) == failed {
                    Err(io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::Interrupted,
            "publication cut {failed:?}"
        );
        assert!(
            !temporary.exists(),
            "unpublished staging owner did not retire"
        );
        if matches!(
            failed,
            Some(PublicationStage::Installed | PublicationStage::Published)
        ) {
            assert_eq!(fs::read(&path)?, b"complete");
            assert!(is_private(&open(&path, false)?)?);
        } else {
            assert!(!path.exists());
        }
    }
    let path = root.path().join("owner");
    assert!(publish(&path, &path, RenameMode::NoReplace, |_| Ok(()), |_| Ok(())).is_err());
    // Cover every UTF-16 tail alignment through the real publisher. In
    // particular, an aligned name leaves no implicit packet padding for NUL.
    for padding in 0..4 {
        let path = root.path().join(format!("retained{}", "x".repeat(padding)));
        let mut staged = StagedFile::create(root.path(), "original-")?;
        staged.as_file_mut().write_all(b"original publication")?;
        let retained = staged.publish_retained(&path, RenameMode::NoReplace)?;
        let mut replacement = StagedFile::create(root.path(), "replacement-")?;
        replacement
            .as_file_mut()
            .write_all(b"replacement publication")?;
        replacement.publish(&path, RenameMode::Replace)?;
        let mut held = retained.as_file();
        held.rewind()?;
        let mut bytes = Vec::new();
        held.read_to_end(&mut bytes)?;
        assert_eq!(bytes, b"original publication");
        // Negative control: reopening the selected name observes a different owner.
        assert_eq!(fs::read(&path)?, b"replacement publication");
        drop(retained);
        assert_eq!(fs::read(&path)?, b"replacement publication");
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
            RenameMode::NoReplace,
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
            RenameMode::Replace,
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
                RenameMode::NoReplace,
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
fn reads_do_not_commit_namespace_but_publication_still_requires_durability() -> io::Result<()> {
    use acyclic_native_runtime::testing::{DurableFault, Schedule};
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    create(&path)?.write_all(b"original")?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (schedule, _) = Schedule::seeded(83);
    schedule.cut_at(0, DurableFault::Failed);
    runtime.block_on(schedule.scope(async {
        assert_eq!(read(&path, 8)?.0, b"original");
        assert!(schedule.durable_writes().is_empty());
        let mut next = StagedFile::create(root.path(), ".next-")?;
        next.as_file_mut().write_all(b"replacement")?;
        assert!(next.publish(&path, RenameMode::Replace).is_err());
        assert_eq!(read(&path, 8)?.0, b"original");
        Ok::<_, io::Error>(())
    }))?;
    Ok(())
}

#[test]
fn optional_reader_rejects_growth_and_admits_before_allocation() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("owner");
    create(&path)?.write_all(b"original")?;
    assert!(
        read_with::<(), io::Error>(&path, 1, |_, _| panic!("oversize read was admitted")).is_err()
    );
    assert_eq!(read(&path, None)?.0, b"original");
    let result = read_with::<Vec<u8>, io::Error>(&path, None, |file, length| {
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
fn a_staging_collision_cannot_claim_that_an_original_owner_was_published() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let destination = directory.path().join("owner");
    let staging = directory.path().join("occupied");
    let _held = create(&staging)?;
    assert!(!destination.exists());
    // Negative control: interpreting every AlreadyExists as a published winner
    // accepts the occupied staging pathname while no owner record exists.
    let unchecked_winner = publish(
        &destination,
        &staging,
        RenameMode::NoReplace,
        |_| Ok(()),
        |_| Ok(()),
    )
    .is_err_and(|error| error.kind() == io::ErrorKind::AlreadyExists);
    assert!(unchecked_winner);
    assert!(publish_new(&destination, &staging, |_| Ok(()), |_| Ok(())).is_err());
    assert!(!destination.exists());
    Ok(())
}
