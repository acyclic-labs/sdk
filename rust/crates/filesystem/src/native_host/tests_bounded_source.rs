use super::*;
use crate::CancellationToken;
#[cfg(unix)]
#[test]
fn concurrent_held_reads_share_one_actual_started_request_allowance()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("held");
    std::fs::write(&path, b"actual held bytes")?;
    let file = std::fs::File::open(path)?;
    let cancellation = CancellationToken::new();
    let budget = crate::WorkBudget {
        backend_read_operations: 1,
        ..crate::WorkBudget::UNBOUNDED
    };
    let work =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let read = || {
            barrier.wait();
            file_identity_and_length(&file, &mut &work)
        };
        let first = scope.spawn(read);
        let second = scope.spawn(read);
        let first = first.join().expect("first held reader did not panic");
        let second = second.join().expect("second held reader did not panic");
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let refused = if first.is_err() { first } else { second };
        assert!(matches!(
            refused,
            Err(crate::NativeTreeMaterializationError::Work(_))
        ));
    });
    work.receipt().verify(budget)?;
    Ok(())
}
#[cfg(unix)]
#[test]
fn cleanup_reservations_restore_only_their_own_non_lifo_allowance()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("leaf"), b"actual cleanup")?;
    let parent = Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
    let cancellation = CancellationToken::new();
    let budget = crate::WorkBudget {
        backend_write_operations: 2,
        ..crate::WorkBudget::default()
    };
    let mut work = DemandWork::new(budget, &cancellation);
    let earlier = work.reserve_cleanup_write()?;
    let later = work.reserve_cleanup_write()?;
    assert_eq!(work.budget.backend_write_operations, 0);
    cancellation.cancel();
    let (deleted, recorded) = work.attempt_cleanup_write(earlier, || {
        // SAFETY: held parent and static terminated leaf; exactly one native cleanup attempt consumes the earlier reserved slot.
        unsafe { libc::unlinkat(parent.as_raw_fd(), c"leaf".as_ptr(), 0) }
    });
    assert_eq!(deleted, 0);
    recorded?;
    assert_eq!(work.budget.backend_write_operations, 1);
    assert!(
        work.work
            .admit(
                &WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                },
                &work.budget
            )
            .is_err(),
        "the later reservation remains withheld"
    );
    work.cancel_cleanup_write(later);
    assert_eq!(work.budget.backend_write_operations, 2);
    work.work.verify(budget)?;
    assert!(!root.path().join("leaf").exists());
    let mut work = DemandWork::new(budget, &CancellationToken::new());
    let earlier = work.reserve_cleanup_write()?;
    let later = work.reserve_cleanup_write()?;
    work.cancel_cleanup_write(earlier);
    assert_eq!(work.budget.backend_write_operations, 1);
    work.cancel_cleanup_write(later);
    assert_eq!(work.budget.backend_write_operations, 2);
    assert_eq!(work.work, WorkCounters::UNCHARGED);
    Ok(())
}
#[cfg(windows)]
#[test]
fn native_stat_preserves_full_file_id_and_opaque_reparse_kind() -> io::Result<()> {
    use windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION;
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
    let mut information = FILE_STAT_INFORMATION::default();
    information.FileId = -1;
    information.FileAttributes = FILE_ATTRIBUTE_REPARSE_POINT.0;
    information.ReparseTag = 0x9000_001A;
    let opaque = HostStat::from_stat_information(&information, 17)?;
    assert_eq!(opaque.file_index(), u64::MAX);
    assert_eq!(opaque.volume_serial_number(), 17);
    assert!(opaque.file_type().is_file());
    assert!(!opaque.file_type().is_symlink());
    for tag in [0xA000_0003, 0xA000_000C] {
        information.ReparseTag = tag;
        assert!(
            HostStat::from_stat_information(&information, 17)?
                .file_type()
                .is_symlink()
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
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
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
    raw_name[0] = 0xff;
    let name = OsString::from_vec(raw_name);
    for _ in 0..24 {
        directory.create_dir(&name)?;
        directory = directory.open_dir(&name)?;
        expected.push(&name);
    }
    assert!(expected.as_os_str().len() > libc::PATH_MAX as usize);
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
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
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert!(matches!(
        root.stat(Path::new("file"), &mut work),
        Err(DemandError::Cancelled)
    ));
    assert!(matches!(
        root.open_file(Path::new("file"), &mut work),
        Err(DemandError::Cancelled)
    ));
    assert!(matches!(
        root.read_dir_stats(Path::new(""), &mut work),
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
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let mut limited = ordinary_work();
    assert!(matches!(
        root.read_link(Path::new("link"), 129, &mut limited),
        Err(DemandError::InvalidRequest)
    ));
    let mut complete = ordinary_work();
    assert_eq!(
        root.read_link(Path::new("link"), u64::MAX, &mut complete)?
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
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let directory = root
        .open_dir_held(Path::new(""), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    assert!(
        rename_no_replace_held(
            &directory,
            Path::new("source"),
            &directory,
            Path::new("destination"),
            None,
            &mut ordinary_work()
        )
        .map_err(ordinary_error)
        .is_err()
    );
    assert_eq!(std::fs::read(temporary.path().join("source"))?, b"source");
    assert_eq!(
        std::fs::read(temporary.path().join("destination"))?,
        b"destination"
    );
    std::fs::remove_file(temporary.path().join("destination"))?;
    rename_no_replace_held(
        &directory,
        Path::new("source"),
        &directory,
        Path::new("destination"),
        None,
        &mut ordinary_work(),
    )
    .map_err(ordinary_error)?;
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
    let root = HostRoot::open(&original, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let directory = root
        .open_dir_held(Path::new(""), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    std::fs::rename(&original, &moved)?;
    std::fs::create_dir(&original)?;
    std::fs::write(original.join("source"), b"replacement")?;
    durable_rename_held(
        &directory,
        Path::new("source"),
        &directory,
        Path::new("destination"),
    )?;
    assert_eq!(std::fs::read(moved.join("destination"))?, b"held");
    assert_eq!(std::fs::read(original.join("source"))?, b"replacement");
    assert!(!original.join("destination").exists());
    Ok(())
}
