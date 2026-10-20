use super::*;
use crate::native_host::stat_probe::{Fault, Probe, events};

/// One checkout capture: hold the root, then read its incarnation.
fn capture(path: &std::path::Path) -> Result<crate::NativeRootIncarnation, DemandError> {
    hold_root(path)?.incarnation()
}

/// What one held-root capture must do when one native query fails once.
enum Expected {
    /// The unchanged incarnation.
    Same,
    /// The same identity, classified as a filesystem without stable ids.
    Unstable,
    /// A failure with the injected cause.
    Cause(i32),
}

fn expected(query: &str, errno: i32) -> Expected {
    // An interrupted query is retried, and an `EPERM` open is retried without
    // `O_NOATIME`; both name the same incarnation. A kernel without file
    // handles is a filesystem without stable ids. Every other failure keeps
    // its cause.
    if errno == libc::EINTR || (query == "openat" && errno == libc::EPERM) {
        Expected::Same
    } else if query == "handle" && errno == libc::ENOSYS {
        Expected::Unstable
    } else {
        Expected::Cause(errno)
    }
}

/// Every native query on the identity path, failed once with every
/// transient or capability errno, names the unchanged incarnation, classifies
/// a kernel without file handles, or fails with the injected cause: never a
/// different incarnation.
fn every_fault_keeps_one_incarnation(fold: bool) -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let baseline = capture(directory.path()).map_err(|error| error.to_string())?;
    for query in ["openat", "fstat", "handle", "fsid"] {
        for errno in [libc::EINTR, libc::ENOSYS, libc::EPERM] {
            let fault = Fault::inject(query, errno);
            if fold {
                fault.fold_errors();
            }
            let held = capture(directory.path());
            if !fault.fired() {
                return Err(format!("{query} was never queried"));
            }
            match (expected(query, errno), held) {
                (Expected::Same, Ok(held)) if held == baseline => {}
                (Expected::Unstable, Ok(held))
                    if held.identity == baseline.identity
                        && held.generation == crate::NativeRootGeneration::Unstable => {}
                (Expected::Cause(errno), Err(crate::demand::DemandError::Io(error)))
                    if error.raw_os_error() == Some(errno) => {}
                (_, held) => return Err(format!("{query} errno {errno}: {held:?}")),
            }
        }
    }
    Ok(())
}

#[test]
fn every_identity_query_fault_keeps_one_incarnation_or_its_cause() {
    every_fault_keeps_one_incarnation(false).unwrap();
}

#[test]
fn negative_control_a_folded_handle_error_changes_the_incarnation() {
    assert!(every_fault_keeps_one_incarnation(true).is_err());
}

/// The physical native queries of one held-root capture.
fn capture_queries(duplicate: bool) -> Vec<&'static str> {
    let directory = tempfile::tempdir().unwrap();
    let probe = Probe::rescan(false);
    if duplicate {
        probe.duplicate_queries();
    }
    capture(directory.path()).unwrap();
    events()
}

#[test]
fn a_held_root_capture_makes_exactly_one_open_identity_handle_and_volume_query() {
    assert_eq!(
        capture_queries(false),
        ["openat", "fstat", "handle", "fsid"]
    );
}

#[test]
fn negative_control_a_repeated_identity_query_is_counted() {
    assert_ne!(capture_queries(true), ["openat", "fstat", "handle", "fsid"]);
}

/// A remount may renumber `st_dev`; the held root's volume is the filesystem's
/// own id, read from the held descriptor. Negative control: at least one
/// qualification filesystem (a UUID-backed disk or tmpfs) reports an `f_fsid`
/// that differs from its device number, so a device-scoped volume fails.
#[test]
fn a_held_root_names_its_volume_by_the_filesystem_id_not_the_device() {
    let mut discriminated = false;
    for parent in [std::env::temp_dir(), std::path::PathBuf::from("/dev/shm")] {
        let Ok(directory) = tempfile::tempdir_in(&parent) else {
            continue;
        };
        let held = capture(directory.path()).unwrap();
        let fsid = rustix::fs::statvfs(directory.path()).unwrap().f_fsid;
        let expected = if fsid == 0 {
            held.identity.device
        } else {
            fsid
        };
        assert_eq!(held.volume, expected, "{}", parent.display());
        discriminated |= fsid != 0 && fsid != held.identity.device;
    }
    assert!(
        discriminated,
        "no filesystem distinguishes f_fsid from st_dev"
    );
}

/// A kill after a step of a record publication, then a later publication
/// with another candidate, in a fresh held directory.
fn killed_then_published(step: usize, torn: bool) -> Result<[u8; 16], DemandError> {
    use crate::native_host::publish_probe;
    let directory = tempfile::tempdir().unwrap();
    let held = hold_root(directory.path())?;
    let leaf = std::path::Path::new("token");
    publish_probe::tear(torn);
    publish_probe::kill_at(Some(step));
    let killed = held.read_or_publish(leaf, || Ok([1; 16]));
    publish_probe::kill_at(None);
    publish_probe::tear(false);
    assert!(killed.is_err(), "step {step} was not killed");
    held.read_or_publish(leaf, || Ok([2; 16]))
}

/// A kill after every durable step of a publication leaves no record (the
/// next publication wins) or the complete one (once renamed into place).
#[test]
fn a_kill_at_any_publication_step_leaves_no_record_or_the_complete_one() {
    use crate::native_host::publish_probe::STEPS;
    for step in 0..STEPS {
        let published = killed_then_published(step, false).unwrap();
        assert_eq!(
            published,
            if step == 3 { [1; 16] } else { [2; 16] },
            "step {step}"
        );
    }
}

/// Negative control: a record written in place is torn by a kill mid-write,
/// and every later read refuses it.
#[test]
fn negative_control_a_record_published_in_place_is_torn_by_a_kill() {
    assert!(killed_then_published(1, true).is_err());
}

/// Concurrent first publications in one directory all return one record.
#[test]
fn concurrent_publishers_return_one_record() {
    let directory = tempfile::tempdir().unwrap();
    let records: Vec<[u8; 16]> = std::thread::scope(|scope| {
        let publishers: Vec<_> = (0..8_u8)
            .map(|candidate| {
                let path = directory.path();
                scope.spawn(move || {
                    hold_root(path)
                        .and_then(|held| {
                            held.read_or_publish(std::path::Path::new("token"), || {
                                Ok([candidate; 16])
                            })
                        })
                        .unwrap()
                })
            })
            .collect();
        publishers
            .into_iter()
            .map(|publisher| publisher.join().unwrap())
            .collect()
    });
    assert!(
        records.iter().all(|record| *record == records[0]),
        "{records:?}"
    );
}

/// Publishing requires one candidate; reading any existing publication must
/// require none, even when the candidate source is unavailable.
fn candidate_work(eager: bool, readers: usize) -> Result<(), String> {
    use crate::native_host::publish_probe;
    let directory = tempfile::tempdir().unwrap();
    let root = hold_root(directory.path()).unwrap();
    let leaf = std::path::Path::new("token");
    let unavailable = || Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into());
    // Candidate failures keep their cause and publish nothing.
    assert!(matches!(
        root.read_or_publish::<16>(leaf, unavailable),
        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied
    ));
    assert!(!directory.path().join(leaf).exists());
    let calls = std::cell::Cell::new(0);
    let published = root
        .read_or_publish(leaf, || {
            calls.set(calls.get() + 1);
            Ok([7; 16])
        })
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(published, [7; 16]);
    publish_probe::generate_candidates_eagerly(eager);
    let result = (|| {
        for _ in 0..readers {
            let record = root.read_or_publish::<16>(leaf, || {
                calls.set(calls.get() + 1);
                unavailable()
            });
            if !matches!(record, Ok(record) if record == published) || calls.get() != 1 {
                return Err("an existing record requested another candidate".into());
            }
        }
        Ok(())
    })();
    publish_probe::generate_candidates_eagerly(false);
    result
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn reading_an_existing_record_never_generates_another_candidate(readers in 1usize..9) {
        proptest::prop_assert_eq!(candidate_work(false, readers), Ok(()));
    }
}

#[test]
fn negative_control_eager_candidates_add_work_and_can_prevent_reopen() {
    assert!(candidate_work(true, 1).is_err());
}

/// Enumerate all crash cuts of the token's reachability chain. The kernel
/// may forget any directory entry whose parent was not flushed. The same
/// detector must reject removing either the ancestor or racing-reader flush.
#[test]
fn a_returned_record_has_a_durable_name_through_every_held_parent() {
    use crate::native_host::{publish_probe, stat_probe::take_directory_syncs};
    for omit_parents in [false, true] {
        for omit_reader in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let root = hold_root(directory.path()).unwrap();
            take_directory_syncs();
            publish_probe::omit_parent_sync(omit_parents);
            publish_probe::omit_reader_sync(omit_reader);
            let first = root
                .create_directories(std::path::Path::new("volumes"))
                .unwrap();
            let second = first
                .create_directories(std::path::Path::new("incarnation"))
                .unwrap();
            let expected = [root.identity(), first.identity(), second.identity()];
            // Pause the winner immediately after rename, before its flush.
            // The reader's return is an admissible crash cut in its own right.
            let reader = second.try_clone().unwrap();
            publish_probe::before_directory_sync(move || {
                assert_eq!(
                    reader
                        .read_or_publish(std::path::Path::new("token"), || Ok([2; 16]))
                        .unwrap(),
                    [1; 16]
                );
                let synced = take_directory_syncs();
                let durable = expected.iter().all(|parent| synced.contains(parent));
                assert_eq!(
                    durable,
                    !omit_parents && !omit_reader,
                    "parent={omit_parents}, reader={omit_reader}"
                );
            });
            second
                .read_or_publish(std::path::Path::new("token"), || Ok([1; 16]))
                .unwrap();
            publish_probe::omit_parent_sync(false);
            publish_probe::omit_reader_sync(false);
        }
    }
}

#[test]
fn held_directory_traversal_borrows_parents_without_duplicating_handles() {
    use crate::native_host::{publish_probe, stat_probe::take_directory_clones};
    for depth in 1..=4 {
        let directory = tempfile::tempdir().unwrap();
        let root = hold_root(directory.path()).unwrap();
        let relative: PathBuf = (0..depth).map(|_| "child").collect();
        for duplicate in [false, true] {
            take_directory_clones();
            publish_probe::duplicate_parent_handles(duplicate);
            root.create_directories(&relative).unwrap();
            publish_probe::duplicate_parent_handles(false);
            assert_eq!(take_directory_clones(), if duplicate { depth } else { 0 });
        }
    }
}

#[cfg(feature = "native-mount")]
#[test]
fn held_recovery_rejects_every_normalized_directory_alias_before_open() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), directory.path().join("alias")).unwrap();
    let root = hold_root(directory.path()).unwrap();
    for leaf in ["alias/", "alias/.", "alias//"] {
        assert!(
            matches!(
                crate::recover_disconnected_native_mount_destination_in(
                    &root,
                    std::ffi::OsStr::new(leaf)
                ),
                Err(crate::NativeMountError::InvalidDestination)
            ),
            "accepted {leaf:?}"
        );
    }
}

/// A directory created below a held root is created in the held root, never
/// in a replacement at the root's path; one created below the reopened path
/// lands in the replacement.
#[test]
fn held_directories_are_created_in_the_held_root_across_a_replacement() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("root");
    let moved = parent.path().join("moved");
    let replaced = |reopen: bool| {
        std::fs::create_dir(&root).unwrap();
        let held = hold_root(&root).unwrap();
        std::fs::rename(&root, &moved).unwrap();
        std::fs::create_dir(&root).unwrap();
        let held = if reopen {
            hold_root(held.path()).unwrap()
        } else {
            held
        };
        held.create_directories(std::path::Path::new("a/b"))
            .unwrap();
        let landed = (moved.join("a/b").is_dir(), root.join("a").exists());
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&moved).unwrap();
        landed
    };
    assert_eq!(replaced(false), (true, false));
    assert_eq!(replaced(true), (false, true), "negative control");
}
