use super::create_windows_symbolic_link;
use super::*;
use super::{
    HostRoot, NativeMetadataError, UNIX_EPOCH_FILETIME, allocated_data_ranges,
    filetime_to_unix_nanoseconds, unix_nanoseconds_to_filetime,
};
use crate::WorkCounters;
use acyclic_native_runtime::{Durability, NativeFile, OwnedWrite};
use proptest::prelude::*;
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn filetime_conversions_round_trip(nanoseconds in any::<i64>(), ticks in any::<i64>()) {
        let tick = unix_nanoseconds_to_filetime(nanoseconds);
        prop_assert_eq!(
            filetime_to_unix_nanoseconds(tick),
            nanoseconds.checked_sub(nanoseconds.rem_euclid(100))
        );
        match filetime_to_unix_nanoseconds(ticks) {
            Some(exact) => prop_assert_eq!(unix_nanoseconds_to_filetime(exact), ticks),
            None => prop_assert!(
                i128::from(ticks) - i128::from(UNIX_EPOCH_FILETIME) > i128::from(i64::MAX / 100)
                    || i128::from(ticks) - i128::from(UNIX_EPOCH_FILETIME)
                        < i128::from(i64::MIN / 100)
            ),
        }
    }
}
#[test]
fn filetime_conversions_cover_their_extremes() {
    assert_eq!(unix_nanoseconds_to_filetime(0), UNIX_EPOCH_FILETIME);
    assert_eq!(unix_nanoseconds_to_filetime(-1), UNIX_EPOCH_FILETIME - 1);
    assert_eq!(unix_nanoseconds_to_filetime(99), UNIX_EPOCH_FILETIME);
    assert_eq!(
        filetime_to_unix_nanoseconds(unix_nanoseconds_to_filetime(i64::MIN)),
        None
    );
    assert_eq!(filetime_to_unix_nanoseconds(i64::MAX), None);
    assert_eq!(filetime_to_unix_nanoseconds(i64::MIN), None);
    assert_eq!(filetime_to_unix_nanoseconds(0), None);
    assert!(
        super::metadata_time(crate::kernel::MetadataField::Value(150), 7).is_err(),
        "exact writers refuse instants between ticks"
    );
}
use bytes::Bytes;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;
#[tokio::test]
async fn held_async_copy_preserves_source_hardlinks_and_rejects_escape() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let source_path = temporary.path().join("source");
    let destination_path = temporary.path().join("destination");
    let outside_path = temporary.path().join("outside");
    std::fs::create_dir(&source_path)?;
    std::fs::create_dir(&destination_path)?;
    std::fs::create_dir(&outside_path)?;
    std::fs::write(source_path.join("file"), b"source contents")?;
    std::fs::hard_link(source_path.join("file"), source_path.join("alias"))?;
    std::fs::write(outside_path.join("untouched"), b"outside")?;
    let source = HostRoot::open(&source_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let destination = HostRoot::open(&destination_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    destination
        .copy_file_from(&source, Path::new("alias"), Path::new("copy"))
        .await?;
    assert_eq!(
        std::fs::read(destination_path.join("copy"))?,
        b"source contents"
    );
    std::fs::write(destination_path.join("copy"), b"changed")?;
    assert_eq!(std::fs::read(source_path.join("file"))?, b"source contents");
    assert_eq!(
        std::fs::read(source_path.join("alias"))?,
        b"source contents"
    );
    assert!(
        destination
            .copy_file_from(
                &source,
                Path::new("../outside/untouched"),
                Path::new("escape")
            )
            .await
            .is_err()
    );
    match std::os::windows::fs::symlink_dir(&outside_path, source_path.join("pivot")) {
        Ok(()) => assert!(
            destination
                .copy_file_from(&source, Path::new("pivot/untouched"), Path::new("escape"))
                .await
                .is_err()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
        Err(error) => return Err(error),
    }
    assert!(!destination_path.join("escape").exists());
    assert_eq!(std::fs::read(outside_path.join("untouched"))?, b"outside");
    Ok(())
}
#[tokio::test]
async fn cancelled_async_copy_leaves_no_partial_output() -> std::io::Result<()> {
    use std::time::Duration;
    let temporary = tempfile::tempdir()?;
    let source_path = temporary.path().join("source");
    let destination_path = temporary.path().join("destination");
    std::fs::create_dir(&source_path)?;
    std::fs::create_dir(&destination_path)?;
    let source_file = std::fs::File::create(source_path.join("large"))?;
    let length = 128 * 1024 * 1024;
    source_file.set_len(length)?;
    let source = HostRoot::open(&source_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let destination = HostRoot::open(&destination_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let task = tokio::spawn(async move {
        destination
            .copy_file_from(&source, Path::new("large"), Path::new("copy"))
            .await
    });
    let copy_path = destination_path.join("copy");
    let staged = || -> std::io::Result<bool> {
        for entry in std::fs::read_dir(&destination_path)? {
            if entry?
                .file_name()
                .to_string_lossy()
                .starts_with(super::STAGING_PREFIX)
            {
                return Ok(true);
            }
        }
        Ok(false)
    };
    let absent_or_complete = || match std::fs::metadata(&copy_path) {
        Ok(metadata) => assert_eq!(metadata.len(), length, "partial destination"),
        Err(error) => assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ),
            "{error}"
        ),
    };
    while !staged()? && !task.is_finished() {
        absent_or_complete();
        tokio::task::yield_now().await;
    }
    task.abort();
    let _ = task.await;
    while staged()? {
        absent_or_complete();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    absent_or_complete();
    Ok(())
}
/// Names the directory [`staged_copy_crash_child`] stages into.
const CRASH_DIRECTORY: &str = "ACYCLIC_STAGED_COPY_CRASH_DIRECTORY";
#[test]
#[ignore = "runs only as a child of a_process_dying_mid_copy_leaves_nothing"]
fn staged_copy_crash_child() -> std::io::Result<()> {
    let Some(directory) = std::env::var_os(CRASH_DIRECTORY) else {
        return Ok(());
    };
    let staged = super::StagedWindowsFile::create(cap_std::fs::Dir::open_ambient_dir(
        directory,
        cap_std::ambient_authority(),
    )?)?;
    super::reopen_windows_file(
        &staged.guard,
        windows::Win32::Storage::FileSystem::FILE_GENERIC_WRITE,
        Default::default(),
        &mut super::ordinary_work(),
    )
    .map_err(super::ordinary_error)?
    .write_all(b"partial copy")?;
    std::process::abort();
}
#[test]
fn a_process_dying_mid_copy_leaves_nothing() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let status = acyclic_native_runtime::process_status(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "native_host::windows_clone_tests::staged_copy_crash_child",
                "--test-threads=1",
            ])
            .env(CRASH_DIRECTORY, temporary.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
        std::time::Duration::from_secs(120),
    )?;
    assert!(!status.success(), "the child must die mid-copy");
    let names = std::fs::read_dir(temporary.path())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    assert!(names.is_empty(), "a crashed copy left {names:?}");
    Ok(())
}
#[test]
fn a_published_copy_keeps_only_its_final_name() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let parent =
        cap_std::fs::Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    let staged = super::StagedWindowsFile::create(parent)?;
    super::reopen_windows_file(
        &staged.guard,
        windows::Win32::Storage::FileSystem::FILE_GENERIC_WRITE,
        Default::default(),
        &mut super::ordinary_work(),
    )
    .map_err(super::ordinary_error)?
    .write_all(b"complete copy")?;
    staged.publish(std::ffi::OsStr::new("copy"))?;
    assert_eq!(
        staged
            .publish(std::ffi::OsStr::new("copy"))
            .map_err(|error| error.kind()),
        Err(std::io::ErrorKind::AlreadyExists),
        "publishing never replaces an existing name"
    );
    drop(staged);
    let names = std::fs::read_dir(temporary.path())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(names, ["copy"]);
    assert_eq!(
        std::fs::read(temporary.path().join("copy"))?,
        b"complete copy"
    );
    Ok(())
}
#[test]
fn a_staged_name_goes_while_another_process_holds_the_file() -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let temporary = tempfile::tempdir()?;
    let parent =
        cap_std::fs::Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
    let staged = super::StagedWindowsFile::create(parent)?;
    let name = std::fs::read_dir(temporary.path())?
        .next()
        .expect("created staged file")?
        .file_name();
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
        .open(temporary.path().join(name))?;
    drop(staged);
    let names = std::fs::read_dir(temporary.path())?.count();
    drop(held);
    assert_eq!(names, 0, "the staged name outlived its guard");
    Ok(())
}
#[test]
fn hydrating_a_placeholder_leaves_its_attributes_the_same() {
    use super::same_windows_attributes;
    assert!(same_windows_attributes(0x0040_0000, 0x0000_0020));
    assert!(same_windows_attributes(0x0040_0001, 0x0000_0021));
    assert!(!same_windows_attributes(0x0040_0000, 0x0000_0021));
    assert!(!same_windows_attributes(0x0000_0000, 0x0000_0020));
    assert!(same_windows_attributes(0x0000_0420, 0x0000_0020));
    assert!(same_windows_attributes(0x0000_0220, 0x0000_0020));
    assert!(!same_windows_attributes(0x0000_0420, 0x0000_0021));
    assert!(!same_windows_attributes(0x0000_0220, 0x0000_0200));
    assert!(!same_windows_attributes(0x0000_0020, 0x0000_0220));
}
#[test]
fn a_name_deleted_while_held_open_is_not_found() -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_DISPOSITION_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FileDispositionInfo, SetFileInformationByHandle,
    };
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("pending");
    std::fs::write(&path, b"held")?;
    let share = (FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0;
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(share)
        .open(&path)?;
    let deleting = std::fs::OpenOptions::new()
        .access_mode(DELETE.0)
        .share_mode(share)
        .open(&path)?;
    let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
    // SAFETY: a live handle and a correctly sized input structure.
    unsafe {
        SetFileInformationByHandle(
            windows::Win32::Foundation::HANDLE(deleting.as_raw_handle()),
            FileDispositionInfo,
            (&raw const disposition).cast(),
            u32::try_from(std::mem::size_of::<FILE_DISPOSITION_INFO>()).unwrap_or(u32::MAX),
        )
    }?;
    drop(deleting);
    assert_eq!(std::fs::read_dir(temporary.path())?.count(), 1);
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let stat = root
        .stat(
            Path::new("pending"),
            &mut crate::native_host::ordinary_work(),
        )
        .map_err(crate::native_host::ordinary_error);
    drop(held);
    assert_eq!(
        stat.map(|_| ()).map_err(|error| error.kind()),
        Err(std::io::ErrorKind::NotFound)
    );
    Ok(())
}
#[tokio::test]
async fn copy_never_replaces_an_existing_destination() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("source"), b"new")?;
    std::fs::write(temporary.path().join("existing"), b"old")?;
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let copied = root
        .copy_file_from(&root, Path::new("source"), Path::new("existing"))
        .await;
    assert_eq!(
        copied.map_err(|error| error.kind()),
        Err(std::io::ErrorKind::AlreadyExists),
        "an existing destination is never replaced"
    );
    assert_eq!(std::fs::read(temporary.path().join("existing"))?, b"old");
    let mut names = std::fs::read_dir(temporary.path())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    assert_eq!(
        names,
        ["existing", "source"],
        "a staged copy was left behind"
    );
    Ok(())
}
#[test]
fn held_windows_metadata_applies_and_verifies_file_and_directory_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let mut work = super::ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::windows::fs::MetadataExt as _;
    const FIXED_NS: i64 = 1_700_000_000_000_000_000;
    const ARCHIVE: u32 = 0x20;
    const DIRECTORY: u32 = 0x10;
    const HIDDEN: u32 = 0x2;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"payload")?;
    std::fs::create_dir(temporary.path().join("directory"))?;
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    for (path, attributes) in [
        (Path::new("file"), ARCHIVE | HIDDEN),
        (Path::new("directory"), DIRECTORY | HIDDEN),
    ] {
        super::apply_windows_metadata(
            root.open_windows_metadata_target(path, &mut work)?,
            FileMetadata {
                windows_attributes: MetadataField::Value(attributes),
                created_ns: MetadataField::Value(FIXED_NS),
                modified_ns: MetadataField::Value(FIXED_NS + 100),
                accessed_ns: MetadataField::Value(FIXED_NS + 200),
                changed_ns: MetadataField::Value(FIXED_NS + 300),
                ..FileMetadata::default()
            },
            &mut work,
        )?;
        assert_eq!(
            std::fs::symlink_metadata(temporary.path().join(path))?.file_attributes(),
            attributes
        );
    }
    let file = temporary.path().join("file");
    let mut permissions = std::fs::metadata(&file)?.permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&file, permissions.clone())?;
    super::apply_windows_metadata(
        root.open_windows_metadata_target(Path::new("file"), &mut work)?,
        FileMetadata::default(),
        &mut work,
    )?;
    super::apply_windows_metadata(
        root.open_windows_metadata_target(Path::new("file"), &mut work)?,
        FileMetadata {
            windows_attributes: MetadataField::Value(ARCHIVE),
            ..FileMetadata::default()
        },
        &mut work,
    )?;
    Ok(())
}
#[test]
fn held_windows_metadata_rejects_unrepresentable_fields_and_path_escape() -> std::io::Result<()> {
    let mut work = super::ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    const REPARSE_POINT: u32 = 0x400;
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    std::fs::create_dir(&root_path)?;
    std::fs::write(root_path.join("file"), b"inside")?;
    std::fs::write(temporary.path().join("outside"), b"outside")?;
    let root = HostRoot::open(&root_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    assert!(matches!(
        super::apply_windows_metadata(
            root.open_windows_metadata_target(Path::new("file"), &mut work)
                .map_err(std::io::Error::other)?,
            FileMetadata {
                windows_attributes: MetadataField::Value(REPARSE_POINT),
                ..FileMetadata::default()
            },
            &mut work
        ),
        Err(crate::DemandError::WindowsMetadata(NativeMetadataError(_)))
    ));
    assert!(matches!(
        super::apply_windows_metadata(
            root.open_windows_metadata_target(Path::new("file"), &mut work)
                .map_err(std::io::Error::other)?,
            FileMetadata {
                named_attributes: MetadataField::Value(crate::ObjectId {
                    kind: crate::ObjectKind::AttributePage,
                    digest: crate::Digest::from_bytes([7; 32]),
                }),
                ..FileMetadata::default()
            },
            &mut work
        ),
        Err(crate::DemandError::WindowsMetadata(NativeMetadataError(_)))
    ));
    assert!(
        root.open_windows_metadata_target(Path::new("../outside"), &mut work)
            .is_err()
    );
    assert_eq!(std::fs::read(temporary.path().join("outside"))?, b"outside");
    Ok(())
}
#[test]
fn held_windows_metadata_rejects_intermediate_reparse_escape() -> std::io::Result<()> {
    let mut work = super::ordinary_work();
    use std::os::windows::fs::MetadataExt as _;
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    let outside = temporary.path().join("outside");
    std::fs::create_dir(&root_path)?;
    std::fs::create_dir(&outside)?;
    std::fs::write(outside.join("target"), b"outside")?;
    match std::os::windows::fs::symlink_dir(&outside, root_path.join("pivot")) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
        Err(error) => return Err(error),
    }
    let before = std::fs::metadata(outside.join("target"))?.file_attributes();
    let root = HostRoot::open(&root_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    assert!(
        root.open_windows_metadata_target(Path::new("pivot/target"), &mut work)
            .is_err()
    );
    assert_eq!(
        std::fs::metadata(outside.join("target"))?.file_attributes(),
        before
    );
    assert_eq!(std::fs::read(outside.join("target"))?, b"outside");
    Ok(())
}
#[cfg(windows)]
#[test]
fn raw_link_set_denial_retains_real_created_prefix_and_prepaid_close()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_write_operations: 2,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let mut close = work.reserve_work(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let mut created = None;
    let published = std::cell::Cell::new(false);
    let set = std::cell::Cell::new(false);
    let result = create_windows_symbolic_link(
        &root.directory,
        &[108, 105, 110, 107],
        &[116, 97, 114, 103, 101, 116],
        false,
        &mut created,
        || published.set(true),
        || set.set(true),
        &mut &work,
    );
    assert!(result.is_err());
    assert!(published.get() && !set.get());
    assert!(created.is_some());
    close.started_drop_io(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    });
    drop(created.take());
    assert!(!temporary.path().join("link").try_exists()?);
    Ok(())
}
#[cfg(windows)]
#[test]
fn raw_link_original_cancellation_after_create_does_not_erase_capability()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut close = work.reserve_work(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let mut created = None;
    let set = std::cell::Cell::new(false);
    let result = create_windows_symbolic_link(
        &root.directory,
        &[108, 105, 110, 107],
        &[116, 97, 114, 103, 101, 116],
        false,
        &mut created,
        || cancellation.cancel(),
        || set.set(true),
        &mut &work,
    );
    assert!(matches!(
        result,
        Err(crate::NativeTreeMaterializationError::Cancelled)
    ));
    assert!(created.is_some() && !set.get());
    assert!(cancellation.is_cancelled());
    close.started_drop_io(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::UNCHARGED
    });
    drop(created.take());
    assert!(cancellation.is_cancelled());
    assert!(!temporary.path().join("link").try_exists()?);
    Ok(())
}
#[test]
fn held_windows_metadata_updates_final_reparse_point_without_touching_target()
-> Result<(), Box<dyn std::error::Error>> {
    let mut work = super::ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::windows::fs::MetadataExt as _;
    const FIXED_NS: i64 = 1_700_000_000_000_000_000;
    const HIDDEN: u32 = 0x2;
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    std::fs::create_dir(&root_path)?;
    let target = root_path.join("target");
    let link = root_path.join("link");
    std::fs::write(&target, b"target")?;
    match std::os::windows::fs::symlink_file(&target, &link) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let target_attributes = std::fs::symlink_metadata(&target)?.file_attributes();
    let link_attributes = std::fs::symlink_metadata(&link)?.file_attributes();
    let root = HostRoot::open(&root_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    super::apply_windows_metadata(
        root.open_windows_metadata_target(Path::new("link"), &mut work)?,
        FileMetadata {
            windows_attributes: MetadataField::Value(link_attributes | HIDDEN),
            created_ns: MetadataField::Value(FIXED_NS),
            modified_ns: MetadataField::Value(FIXED_NS + 100),
            accessed_ns: MetadataField::Value(FIXED_NS + 200),
            changed_ns: MetadataField::Value(FIXED_NS + 300),
            ..FileMetadata::default()
        },
        &mut work,
    )?;
    assert_eq!(
        std::fs::symlink_metadata(&link)?.file_attributes(),
        link_attributes | HIDDEN
    );
    assert_eq!(
        std::fs::symlink_metadata(&target)?.file_attributes(),
        target_attributes
    );
    assert_eq!(std::fs::read(&target)?, b"target");
    Ok(())
}
#[test]
fn held_windows_metadata_targets_leaf_across_rename_and_replacement()
-> Result<(), Box<dyn std::error::Error>> {
    let mut work = super::ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::windows::fs::MetadataExt as _;
    const HIDDEN: u32 = 0x2;
    const MODIFIED_NS: i64 = 1_700_000_000_000_000_000;
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("original");
    let moved = temporary.path().join("moved");
    std::fs::write(&original, b"old")?;
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let target = root.open_windows_metadata_target(Path::new("original"), &mut work)?;
    std::fs::rename(&original, &moved)?;
    std::fs::write(&original, b"new")?;
    let replacement_metadata = std::fs::metadata(&original)?;
    let replacement_attributes = replacement_metadata.file_attributes();
    let replacement_modified = replacement_metadata.last_write_time();
    let moved_attributes = std::fs::metadata(&moved)?.file_attributes();
    super::apply_windows_metadata(
        target,
        FileMetadata {
            windows_attributes: MetadataField::Value(moved_attributes | HIDDEN),
            modified_ns: MetadataField::Value(MODIFIED_NS),
            ..FileMetadata::default()
        },
        &mut work,
    )?;
    assert_eq!(
        std::fs::metadata(&moved)?.file_attributes(),
        moved_attributes | HIDDEN
    );
    assert_eq!(
        std::fs::metadata(&original)?.file_attributes(),
        replacement_attributes
    );
    assert_eq!(
        std::fs::metadata(&moved)?.last_write_time(),
        u64::try_from(unix_nanoseconds_to_filetime(MODIFIED_NS))?
    );
    assert_eq!(
        std::fs::metadata(&original)?.last_write_time(),
        replacement_modified
    );
    assert_eq!(std::fs::read(&moved)?, b"old");
    assert_eq!(std::fs::read(&original)?, b"new");
    Ok(())
}
#[tokio::test]
async fn capability_opened_overlapped_file_runs_native_io_without_reopen() -> std::io::Result<()> {
    let directory = tempfile::tempdir()?;
    let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let path = Path::new("overlapped.bin");
    let file = super::with_wide_path(path, &mut super::ordinary_work(), |path, work| {
        root.create_overlapped_file_wide(path, work)
    })
    .map_err(super::ordinary_error)?;
    // SAFETY: HostRoot used FILE_FLAG_OVERLAPPED and this new handle is transferred directly, with no other I/O or completion-port owner.
    let native = unsafe {
        NativeFile::from_overlapped_file_unchecked(file, None).map_err(|(error, _)| error)?
    };
    native
        .set_len_async(8, None)
        .expect("unmetered unit construction cannot fail")
        .await?;
    native
        .write_all_batch_async(vec![OwnedWrite {
            offset: 2,
            bytes: Bytes::from_static(b"abc"),
        }])
        .await?;
    native
        .sync_async(Durability::Full, None)
        .expect("unmetered unit construction cannot fail")
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join(path))?,
        b"\0\0abc\0\0\0"
    );
    Ok(())
}
#[cfg(feature = "native-mount")]
#[test]
fn block_clone_preserves_cow_or_leaves_no_destination() -> std::io::Result<()> {
    let parent = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT");
    let require_clone = parent.is_some();
    let directory = if let Some(parent) = parent {
        tempfile::Builder::new()
            .prefix("acyclic-clone-")
            .tempdir_in(parent)?
    } else {
        tempfile::tempdir()?
    };
    let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let source_path = Path::new("source.bin");
    let target_path = Path::new("target.bin");
    let mut source = root
        .create_file(source_path, &mut super::ordinary_work())
        .map_err(super::ordinary_error)?;
    // SAFETY: this synchronous control takes no buffers and the source handle remains live for the entire call.
    unsafe {
        DeviceIoControl(
            HANDLE(source.as_raw_handle()),
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            None,
            None,
        )
        .map_err(std::io::Error::other)?;
    }
    source.set_len(1024 * 1024)?;
    source.write_all(&[0xA5; 4096])?;
    source.seek(SeekFrom::Start(1024 * 1024 - 4096))?;
    source.write_all(&[0xA5; 4096])?;
    source.sync_all()?;
    let cloned = root
        .clone_file_from(&root, source_path, target_path, &mut super::ordinary_work())
        .map_err(super::ordinary_error)?;
    let capabilities = crate::probe_native_storage_capabilities(directory.path())
        .map_err(std::io::Error::other)?;
    if capabilities.block_cloning || require_clone {
        assert!(cloned, "advertised block cloning rejected an aligned file");
    }
    if cloned {
        let mut target = root
            .open_file(target_path, &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let mut byte = [0];
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0xA5]);
        target.seek(SeekFrom::Start(512 * 1024))?;
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0]);
        target.seek(SeekFrom::Start(1024 * 1024 - 1))?;
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0xA5]);
        source.seek(SeekFrom::Start(0))?;
        source.write_all(&[0x5A])?;
        source.sync_all()?;
        target.seek(SeekFrom::Start(0))?;
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0xA5]);
    } else {
        assert!(
            root.stat(target_path, &mut crate::native_host::ordinary_work())
                .map_err(crate::native_host::ordinary_error)
                .is_err()
        );
    }
    Ok(())
}
#[test]
fn block_clone_accepts_unaligned_file_lengths() -> std::io::Result<()> {
    let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") else {
        return Ok(());
    };
    let directory = tempfile::Builder::new()
        .prefix("acyclic-unaligned-clone-")
        .tempdir_in(parent)?;
    let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let source_path = Path::new("source.bin");
    let target_path = Path::new("target.bin");
    let mut source = root
        .create_file(source_path, &mut super::ordinary_work())
        .map_err(super::ordinary_error)?;
    // SAFETY: no buffers are passed and the source handle stays live.
    unsafe {
        DeviceIoControl(
            HANDLE(source.as_raw_handle()),
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            None,
            None,
        )
        .map_err(std::io::Error::other)?;
    }
    source.set_len(1024 * 1024 + 17)?;
    source.write_all(&[0xA5; 4096])?;
    source.seek(SeekFrom::Start(1024 * 1024))?;
    source.write_all(&[0x5A; 17])?;
    source.sync_all()?;
    let cloned = root
        .clone_file_from(&root, source_path, target_path, &mut super::ordinary_work())
        .map_err(super::ordinary_error)?;
    assert!(
        cloned,
        "advertised block cloning rejected an unaligned file"
    );
    let mut target = root
        .open_file(target_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let ranges = allocated_data_ranges(
        &target.try_clone()?.into_std(),
        0,
        1024 * 1024 + 17,
        3,
        &mut super::ordinary_work(),
    )
    .map_err(super::ordinary_error)?;
    assert_eq!(ranges.len(), 2, "clone must preserve the sparse middle");
    target.seek(SeekFrom::Start(1024 * 1024))?;
    let mut tail = [0; 17];
    target.read_exact(&mut tail)?;
    assert_eq!(tail, [0x5A; 17]);
    source.seek(SeekFrom::Start(1024 * 1024))?;
    source.write_all(&[0x11; 17])?;
    source.sync_all()?;
    target.seek(SeekFrom::Start(1024 * 1024))?;
    target.read_exact(&mut tail)?;
    assert_eq!(tail, [0x5A; 17]);
    Ok(())
}
#[test]
fn block_clone_spans_multiple_native_requests() -> std::io::Result<()> {
    let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") else {
        return Ok(());
    };
    let directory = tempfile::Builder::new()
        .prefix("acyclic-large-clone-")
        .tempdir_in(parent)?;
    let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let source_path = Path::new("source.bin");
    let target_path = Path::new("target.bin");
    let mut source = root
        .create_file(source_path, &mut super::ordinary_work())
        .map_err(super::ordinary_error)?;
    // SAFETY: no buffers are passed and the source handle stays live.
    unsafe {
        DeviceIoControl(
            HANDLE(source.as_raw_handle()),
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            None,
            None,
        )
        .map_err(std::io::Error::other)?;
    }
    let second_chunk = 1024_u64 * 1024 * 1024;
    let length = second_chunk + 64 * 1024 + 17;
    source.set_len(length)?;
    source.write_all(&[0xA5; 4096])?;
    source.seek(SeekFrom::Start(second_chunk))?;
    source.write_all(&[0x5A; 4096])?;
    source.seek(SeekFrom::Start(length - 17))?;
    source.write_all(&[0x11; 17])?;
    source.sync_all()?;
    assert!(
        root.clone_file_from(&root, source_path, target_path, &mut super::ordinary_work())
            .map_err(super::ordinary_error)?
    );
    let mut target = root
        .open_file(target_path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    target.seek(SeekFrom::Start(second_chunk))?;
    let mut byte = [0];
    target.read_exact(&mut byte)?;
    assert_eq!(byte, [0x5A]);
    target.seek(SeekFrom::Start(length - 1))?;
    target.read_exact(&mut byte)?;
    assert_eq!(byte, [0x11]);
    let ranges = allocated_data_ranges(
        &target.try_clone()?.into_std(),
        0,
        length,
        4,
        &mut super::ordinary_work(),
    )
    .map_err(super::ordinary_error)?;
    assert_eq!(ranges.len(), 3, "multi-request clone must preserve holes");
    Ok(())
}
#[test]
fn sparse_range_query_grows_only_when_ranges_require_it() -> std::io::Result<()> {
    let directory = tempfile::tempdir()?;
    let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let path = Path::new("ranges.bin");
    let mut file = root
        .create_file(path, &mut super::ordinary_work())
        .map_err(super::ordinary_error)?;
    // SAFETY: the control has no buffers and the file handle remains live.
    unsafe {
        DeviceIoControl(
            HANDLE(file.as_raw_handle()),
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            None,
            None,
        )
        .map_err(std::io::Error::other)?;
    }
    file.set_len(20 * 1024 * 1024)?;
    for index in 0..10_u64 {
        file.seek(SeekFrom::Start(index * 2 * 1024 * 1024))?;
        file.write_all(&[0xA5; 4096])?;
    }
    file.sync_all()?;
    let source = root
        .open_file(path, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    assert!(
        allocated_data_ranges(
            &source.try_clone()?.into_std(),
            0,
            20 * 1024 * 1024,
            9,
            &mut super::ordinary_work()
        )
        .is_err()
    );
    let ranges = allocated_data_ranges(
        &source.try_clone()?.into_std(),
        0,
        20 * 1024 * 1024,
        10,
        &mut super::ordinary_work(),
    )
    .map_err(super::ordinary_error)?;
    assert_eq!(ranges.len(), 10);
    assert_eq!(ranges[0].offset, 0);
    assert_eq!(ranges[9].offset, 18 * 1024 * 1024);
    Ok(())
}
