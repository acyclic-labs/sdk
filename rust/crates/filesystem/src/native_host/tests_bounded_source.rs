use super::*;
use crate::CancellationToken;

#[cfg(unix)]
#[test]
fn concurrent_held_reads_share_one_actual_started_request_allowance()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("held");
    std::fs::write(&path, b"actual held bytes")?;
    let file = File::open(path)?;
    let budget = WorkBudget {
        backend_read_operations: 1,
        ..WorkBudget::UNBOUNDED
    };
    let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
        .map_err(|(error, _)| error)?;
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let read = || {
            barrier.wait();
            file_identity_and_length(&file, &mut &work)
        };
        let first = scope.spawn(read);
        let second = scope.spawn(read);
        let results = [first.join().unwrap(), second.join().unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(
            results
                .iter()
                .any(|result| matches!(result, Err(NativeTreeMaterializationError::Work(_))))
        );
    });
    work.receipt().verify(budget)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn cleanup_reservations_restore_only_their_own_non_lifo_allowance()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("leaf"), b"actual cleanup")?;
    let parent = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        backend_write_operations: 2,
        ..WorkBudget::default()
    };
    let mut work = DemandWork::new(budget, &cancellation);
    let earlier = work.reserve_cleanup_write()?;
    let later = work.reserve_cleanup_write()?;
    cancellation.cancel();
    let (deleted, recorded) = work.attempt_cleanup_write(earlier, || {
        // SAFETY: the held parent and static leaf remain live during cleanup.
        unsafe { libc::unlinkat(parent.as_raw_fd(), c"leaf".as_ptr(), 0) }
    });
    assert_eq!(deleted, 0);
    recorded?;
    assert!(work.work.admit(&WRITE, &work.budget).is_err());
    work.cancel_cleanup_write(later);
    work.work.verify(budget)?;
    assert!(!temporary.path().join("leaf").exists());
    Ok(())
}

#[cfg(windows)]
#[test]
fn native_stat_preserves_full_file_id_and_opaque_reparse_kind() -> io::Result<()> {
    let mut information = nt::FILE_STAT_INFORMATION {
        FileId: -1,
        FileAttributes: fs::FILE_ATTRIBUTE_REPARSE_POINT.0,
        ReparseTag: 0x9000_001A,
        ..Default::default()
    };
    let opaque = HostStat::from_stat_information(&information, 17)?;
    assert_eq!(opaque.raw.FileId as u64, u64::MAX);
    assert_eq!(opaque.volume_serial_number, 17);
    assert!((opaque.file_type() == crate::demand::SourceNodeKind::RegularFile));
    assert!((opaque.file_type() != crate::demand::SourceNodeKind::SymbolicLink));
    for tag in [0xA000_0003, 0xA000_000C] {
        information.ReparseTag = tag;
        assert!(
            HostStat::from_stat_information(&information, 17)?.file_type()
                == crate::demand::SourceNodeKind::SymbolicLink
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn relative_root_preserves_raw_join_semantics_and_retained_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::ffi::OsStrExt as _;
    let cwd = std::env::current_dir()?;
    for relative in [
        Path::new(""),
        Path::new("."),
        Path::new(OsStr::from_bytes(b"./raw-\xff/../root/")),
    ] {
        let mut work = ordinary_work();
        let normalized = normalize_root_path(relative, &mut work)?;
        assert_eq!(normalized, cwd.join(relative));
        assert_eq!(work.live_allocation_bytes(), normalized.capacity() as u64);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn held_cwd_ancestry_reconstructs_paths_beyond_native_path_max()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::ffi::OsStringExt as _;
    let temporary = tempfile::tempdir()?;
    let mut expected = temporary.path().canonicalize()?;
    let mut directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    let mut raw_name = vec![b'a'; 220];
    #[cfg(not(target_os = "macos"))]
    { raw_name[0] = 0xff; }
    let name = OsString::from_vec(raw_name);
    for _ in 0..24 {
        directory.create_dir(&name)?;
        directory = directory.open_dir(&name)?;
        expected.push(&name);
    }
    assert!(expected.as_os_str().len() > libc::PATH_MAX as usize);
    let mut work = ordinary_work();
    let reconstructed = canonical_directory_path_held(directory, &mut work)?.0;
    assert_eq!(reconstructed, expected);
    assert_eq!(
        work.live_allocation_bytes(),
        reconstructed.capacity() as u64
    );
    Ok(())
}

#[test]
fn cancellation_stops_native_source_boundaries_before_io() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"payload")?;
    let root = test_root(temporary.path())?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = test_path("file");
    assert!(matches!(
        root.stat(&path, &mut work),
        Err(DemandError::Cancelled)
    ));
    assert!(matches!(
        root.open_file(
            &path,
            #[cfg(windows)]
            nt::FILE_SYNCHRONOUS_IO_NONALERT,
            &mut work
        ),
        Err(DemandError::Cancelled)
    ));
    assert!(matches!(
        root.read_dir_stats(&test_path(""), &mut work),
        Err(DemandError::Cancelled)
    ));
    assert_eq!(work.work, WorkCounters::UNCHARGED);
    Ok(())
}

#[cfg(unix)]
#[test]
fn link_reads_are_capped_and_each_growth_read_is_retained() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let target = "t".repeat(600);
    std::os::unix::fs::symlink(&target, temporary.path().join("link"))?;
    let root = test_root(temporary.path())?;
    assert!(matches!(
        root.read_link(Path::new("link"), 129, &mut ordinary_work()),
        Err(DemandError::InvalidRequest)
    ));
    assert_eq!(
        root.read_link(Path::new("link"), u64::MAX, &mut ordinary_work())?
            .0,
        target.as_bytes()
    );
    Ok(())
}

#[test]
fn held_rename_never_replaces_an_existing_destination() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("source"), b"source")?;
    std::fs::write(temporary.path().join("destination"), b"destination")?;
    let root = test_root(temporary.path())?;
    let directory = root.open_dir_held(&test_path(""), &mut ordinary_work())?;
    let rename = || {
        rename_held(
            &directory,
            &test_path("source"),
            &directory,
            &test_path("destination"),
            acyclic_native_runtime::RenameMode::NoReplace,
            None,
            &mut ordinary_work(),
        )
    };
    assert!(rename().is_err());
    assert_eq!(std::fs::read(temporary.path().join("source"))?, b"source");
    assert_eq!(
        std::fs::read(temporary.path().join("destination"))?,
        b"destination"
    );
    std::fs::remove_file(temporary.path().join("destination"))?;
    rename()?;
    assert_eq!(
        std::fs::read(temporary.path().join("destination"))?,
        b"source"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn held_rename_uses_the_original_parent_after_its_path_is_replaced()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("parent");
    let moved = temporary.path().join("moved");
    std::fs::create_dir(&original)?;
    std::fs::write(original.join("source"), b"held")?;
    let root = test_root(&original)?;
    let directory = root.open_dir_held(Path::new(""), &mut ordinary_work())?;
    std::fs::rename(&original, &moved)?;
    std::fs::create_dir(&original)?;
    std::fs::write(original.join("source"), b"replacement")?;
    rename_held(
        &directory,
        Path::new("source"),
        &directory,
        Path::new("destination"),
        acyclic_native_runtime::RenameMode::NoReplace,
        None,
        &mut ordinary_work(),
    )?;
    sync_directory_held(&directory, &mut ordinary_work())?;
    assert_eq!(std::fs::read(moved.join("destination"))?, b"held");
    assert_eq!(std::fs::read(original.join("source"))?, b"replacement");
    assert!(!original.join("destination").exists());
    Ok(())
}
