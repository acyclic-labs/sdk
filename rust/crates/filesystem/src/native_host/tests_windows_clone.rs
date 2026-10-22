use super::*;
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
    let ticks = 1_000_000_000_000_000_000_u64;
    let value = windows_time(UNIX_EPOCH_FILETIME as u64 + ticks).into_std();
    assert_eq!(
        value
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        u128::from(ticks) * 100
    );
}
use bytes::Bytes;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;
#[test]
fn hydrating_a_placeholder_leaves_its_attributes_the_same() {
    use same_windows_attributes;
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
    let root = test_root(temporary.path())?;
    let stat = with_wide_path(Path::new("pending"), &mut ordinary_work(), |path, work| {
        root.stat(path, work)
    })
    .map_err(ordinary_error);
    drop(held);
    assert_eq!(
        stat.map(|_| ()).map_err(|error| error.kind()),
        Err(std::io::ErrorKind::NotFound)
    );
    Ok(())
}
#[test]
fn held_windows_metadata_applies_and_verifies_file_and_directory_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let mut work = ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::windows::fs::MetadataExt as _;
    const FIXED_NS: i64 = 1_700_000_000_000_000_000;
    const ARCHIVE: u32 = 0x20;
    const DIRECTORY: u32 = 0x10;
    const HIDDEN: u32 = 0x2;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"payload")?;
    std::fs::create_dir(temporary.path().join("directory"))?;
    let root = test_root(temporary.path())?;
    for (path, attributes) in [
        (Path::new("file"), ARCHIVE | HIDDEN),
        (Path::new("directory"), DIRECTORY | HIDDEN),
    ] {
        apply_metadata(
            &with_wide_path(path, &mut work, |path, work| {
                root.open_windows_metadata_target(path, work)
            })?,
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
    apply_metadata(
        &root.open_windows_metadata_target(&test_path("file"), &mut work)?,
        FileMetadata::default(),
        &mut work,
    )?;
    apply_metadata(
        &root.open_windows_metadata_target(&test_path("file"), &mut work)?,
        FileMetadata {
            windows_attributes: MetadataField::Value(ARCHIVE),
            ..FileMetadata::default()
        },
        &mut work,
    )?;
    Ok(())
}
/// Sparseness is how the host stores a file's holes, not metadata a
/// checkout demands: attributes captured from a dense file apply exactly to
/// a sparse one, and the reverse, each keeping its own representation.
#[test]
fn held_windows_metadata_leaves_sparseness_to_the_file() -> std::io::Result<()> {
    let mut work = ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::windows::fs::MetadataExt as _;
    const HIDDEN: u32 = 0x2;
    const ARCHIVE: u32 = 0x20;
    const SPARSE: u32 = 0x200;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("dense"), b"payload")?;
    let sparse = std::fs::File::create(temporary.path().join("sparse"))?;
    acyclic_native_runtime::device_control_in_place(
        &sparse,
        windows::Win32::System::Ioctl::FSCTL_SET_SPARSE,
        &[],
        &mut [],
        None,
    )?;
    drop(sparse);
    let root = test_root(temporary.path())?;
    for (path, requested, observed) in [
        ("sparse", ARCHIVE | HIDDEN, ARCHIVE | HIDDEN | SPARSE),
        ("dense", ARCHIVE | HIDDEN | SPARSE, ARCHIVE | HIDDEN),
    ] {
        apply_metadata(
            &root
                .open_windows_metadata_target(&test_path(path), &mut work)
                .map_err(std::io::Error::other)?,
            FileMetadata {
                windows_attributes: MetadataField::Value(requested),
                ..FileMetadata::default()
            },
            &mut work,
        )
        .map_err(std::io::Error::other)?;
        assert_eq!(
            std::fs::symlink_metadata(temporary.path().join(path))?.file_attributes(),
            observed,
            "{path}"
        );
    }
    Ok(())
}
#[test]
fn held_windows_metadata_rejects_unrepresentable_fields_and_path_escape() -> std::io::Result<()> {
    let mut work = ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    const REPARSE_POINT: u32 = 0x400;
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    std::fs::create_dir(&root_path)?;
    std::fs::write(root_path.join("file"), b"inside")?;
    std::fs::write(temporary.path().join("outside"), b"outside")?;
    let root = test_root(&root_path)?;
    assert!(matches!(
        apply_metadata(
            &root
                .open_windows_metadata_target(&test_path("file"), &mut work)
                .map_err(std::io::Error::other)?,
            FileMetadata {
                windows_attributes: MetadataField::Value(REPARSE_POINT),
                ..FileMetadata::default()
            },
            &mut work
        ),
        Err(crate::DemandError::NativeMetadata(NativeMetadataError(_)))
    ));
    assert!(matches!(
        apply_metadata(
            &root
                .open_windows_metadata_target(&test_path("file"), &mut work)
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
        Err(crate::DemandError::NativeMetadata(NativeMetadataError(_)))
    ));
    assert!(
        root.open_windows_metadata_target(&test_path("../outside"), &mut work)
            .is_err()
    );
    assert_eq!(std::fs::read(temporary.path().join("outside"))?, b"outside");
    Ok(())
}
#[test]
fn held_windows_metadata_rejects_intermediate_reparse_escape() -> std::io::Result<()> {
    let mut work = ordinary_work();
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
    let root = test_root(&root_path)?;
    assert!(
        root.open_windows_metadata_target(&test_path("pivot/target"), &mut work)
            .is_err()
    );
    assert_eq!(
        std::fs::metadata(outside.join("target"))?.file_attributes(),
        before
    );
    assert_eq!(std::fs::read(outside.join("target"))?, b"outside");
    Ok(())
}
#[test]
fn held_windows_metadata_updates_final_reparse_point_without_touching_target()
-> Result<(), Box<dyn std::error::Error>> {
    let mut work = ordinary_work();
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
    let root = test_root(&root_path)?;
    apply_metadata(
        &root.open_windows_metadata_target(&test_path("link"), &mut work)?,
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
    let mut work = ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::windows::fs::MetadataExt as _;
    const HIDDEN: u32 = 0x2;
    const MODIFIED_NS: i64 = 1_700_000_000_000_000_000;
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("original");
    let moved = temporary.path().join("moved");
    std::fs::write(&original, b"old")?;
    let root = test_root(temporary.path())?;
    let target = root.open_windows_metadata_target(&test_path("original"), &mut work)?;
    std::fs::rename(&original, &moved)?;
    std::fs::write(&original, b"new")?;
    let replacement_metadata = std::fs::metadata(&original)?;
    let replacement_attributes = replacement_metadata.file_attributes();
    let replacement_modified = replacement_metadata.last_write_time();
    let moved_attributes = std::fs::metadata(&moved)?.file_attributes();
    apply_metadata(
        &target,
        FileMetadata {
            windows_attributes: MetadataField::Value(moved_attributes | HIDDEN),
            modified_ns: MetadataField::Value(MODIFIED_NS),
            ..FileMetadata::default()
        },
        &mut work,
    )?;
    drop(target);
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
    let root = test_root(directory.path())?;
    let path = Path::new("overlapped.bin");
    let file = with_wide_path(path, &mut ordinary_work(), |path, work| {
        root.create_file(
            path,
            windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(0),
            work,
        )
    })
    .map_err(ordinary_error)?;
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
fn sparse_file(root: &HostRoot, path: &Path, length: u64) -> std::io::Result<std::fs::File> {
    let file = with_wide_path(path, &mut ordinary_work(), |path, work| {
        root.create_file(
            path,
            windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )
    })
    .map_err(ordinary_error)?;
    // SAFETY: no buffers are passed and the file handle stays live.
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
    file.set_len(length)?;
    Ok(file)
}
#[test]
fn block_clone_preserves_sparse_data_and_cow_across_aligned_unaligned_and_large_files()
-> std::io::Result<()> {
    let parent = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT");
    let megabyte = 1024_u64 * 1024;
    let second_chunk = 1024_u64 * megabyte;
    for (length, patches) in [
        #[cfg(feature = "native-mount")]
        (
            megabyte,
            &[(0, 0xA5, 4096), (megabyte - 4096, 0xA5, 4096)][..],
        ),
        (megabyte + 17, &[(0, 0xA5, 4096), (megabyte, 0x5A, 17)][..]),
        (
            second_chunk + 64 * 1024 + 17,
            &[
                (0, 0xA5, 4096),
                (second_chunk, 0x5A, 4096),
                (second_chunk + 64 * 1024, 0x11, 17),
            ][..],
        ),
    ] {
        if parent.is_none() && length != megabyte {
            continue;
        }
        let directory = match &parent {
            Some(parent) => tempfile::Builder::new()
                .prefix("acyclic-clone-")
                .tempdir_in(parent)?,
            None => tempfile::tempdir()?,
        };
        let root = test_root(directory.path())?;
        let source_path = Path::new("source.bin");
        let target_path = Path::new("target.bin");
        let mut source = sparse_file(&root, source_path, length)?;
        for &(offset, byte, count) in patches {
            source.seek(SeekFrom::Start(offset))?;
            source.write_all(&[byte; 4096][..count])?;
        }
        source.sync_all()?;
        let cloned = with_wide_path(source_path, &mut ordinary_work(), |source, work| {
            with_wide_path(target_path, work, |destination, work| {
                root.clone_file_from(&root, source, destination, work)
            })
        })
        .map_err(ordinary_error)?;
        let mut required = parent.is_some();
        #[cfg(feature = "native-mount")]
        {
            required |= crate::probe_native_storage_capabilities(directory.path())
                .map_err(std::io::Error::other)?
                .block_cloning;
        }
        if required {
            assert!(cloned, "advertised block cloning rejected a sparse file");
        }
        if !cloned {
            assert!(
                with_wide_path(target_path, &mut ordinary_work(), |path, work| root
                    .stat(path, work))
                .map_err(ordinary_error)
                .is_err()
            );
            continue;
        }
        let mut target = with_wide_path(target_path, &mut ordinary_work(), |path, work| {
            root.open_file(
                path,
                windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT,
                work,
            )
        })
        .map_err(ordinary_error)?;
        let mut actual = [0; 4096];
        for &(offset, byte, count) in patches {
            target.seek(SeekFrom::Start(offset))?;
            target.read_exact(&mut actual[..count])?;
            assert_eq!(&actual[..count], &[byte; 4096][..count]);
        }
        target.seek(SeekFrom::Start(512 * 1024))?;
        target.read_exact(&mut actual[..1])?;
        assert_eq!(actual[0], 0);
        let ranges = allocated_data_ranges(
            &target.try_clone()?.into_std(),
            0,
            length,
            if length > second_chunk { 4 } else { 3 },
            &mut ordinary_work(),
        )
        .map_err(ordinary_error)?;
        assert_eq!(
            ranges.len(),
            if length > second_chunk { 3 } else { 2 },
            "clone must preserve holes"
        );
        let (offset, byte, count) = patches[if length == megabyte { 0 } else { 1 }];
        source.seek(SeekFrom::Start(offset))?;
        source.write_all(&[if byte == 0x5A { 0x11 } else { 0x5A }; 4096][..count])?;
        source.sync_all()?;
        target.seek(SeekFrom::Start(offset))?;
        target.read_exact(&mut actual[..count])?;
        assert_eq!(&actual[..count], &[byte; 4096][..count]);
    }
    Ok(())
}
#[test]
fn sparse_range_query_grows_only_when_ranges_require_it() -> std::io::Result<()> {
    let directory = tempfile::tempdir()?;
    let root = test_root(directory.path())?;
    let path = Path::new("ranges.bin");
    let mut file = sparse_file(&root, path, 20 * 1024 * 1024)?;
    for index in 0..10_u64 {
        file.seek(SeekFrom::Start(index * 2 * 1024 * 1024))?;
        file.write_all(&[0xA5; 4096])?;
    }
    file.sync_all()?;
    let source = with_wide_path(path, &mut ordinary_work(), |path, work| {
        root.open_file(
            path,
            windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )
    })
    .map_err(ordinary_error)?;
    assert!(
        allocated_data_ranges(
            &source.try_clone()?.into_std(),
            0,
            20 * 1024 * 1024,
            9,
            &mut ordinary_work()
        )
        .is_err()
    );
    let ranges = allocated_data_ranges(
        &source.try_clone()?.into_std(),
        0,
        20 * 1024 * 1024,
        10,
        &mut ordinary_work(),
    )
    .map_err(ordinary_error)?;
    assert_eq!(ranges.len(), 10);
    assert_eq!(ranges[0].offset, 0);
    assert_eq!(ranges[9].offset, 18 * 1024 * 1024);
    Ok(())
}

fn directory_read_admission(
    depth: usize,
    limit: u64,
    succeeded: bool,
    charged: u64,
) -> proptest::test_runner::TestCaseResult {
    // Returning the root itself needs one owned duplicate. Every nonempty
    // component's open rejects reparses itself; its unused identity needs no query.
    let required = if depth == 0 { 1 } else { depth as u64 };
    prop_assert_eq!(succeeded, limit >= required);
    prop_assert_eq!(charged, limit.min(required));
    Ok(())
}

#[test]
fn directory_walk_borrows_root_and_preserves_identity_at_every_read_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    let mut identities = [vec![root.identity()], vec![root.identity()]];
    for (index, name) in ["nested", "nést"].into_iter().enumerate() {
        let mut parent = temporary.path().to_path_buf();
        for _ in 0..3 {
            parent.push(name);
            std::fs::create_dir(&parent)?;
            identities[index].push(test_root(&parent)?.identity());
        }
    }
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // For every generated depth/name/case/read bound, the production walk must
    // retain the same directory authority and charge exactly its attempted I/O.
    runner.run(
        &(0_usize..4, any::<bool>(), any::<bool>(), 0_u64..10),
        |(depth, unicode, uppercase, limit)| {
            let name = if unicode { "nést" } else { "nested" };
            let relative = vec![name; depth].join("\\");
            let relative = if uppercase {
                relative.to_uppercase()
            } else {
                relative
            };
            let path = test_path(&relative);
            for reads in [limit, u64::MAX] {
                let budget = WorkBudget {
                    backend_read_operations: reads,
                    ..WorkBudget::UNBOUNDED
                };
                let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
                    .map_err(|(error, _)| TestCaseError::fail(error.to_string()))?;
                let result = directory_path(&root.directory, &path, false, &mut &work);
                directory_read_admission(
                    depth,
                    reads,
                    result.is_ok(),
                    work.receipt().backend_read_operations,
                )?;
                work.receipt()
                    .verify(budget)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                if let Ok(held) = result {
                    let identity = directory_identity(&held, &mut ordinary_work())
                        .map_err(|error| TestCaseError::fail(error.to_string()))?;
                    prop_assert_eq!(identity, identities[usize::from(unicode)][depth]);
                }
            }
            let cancellation = crate::CancellationToken::new();
            let cancelled = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(error, _)| TestCaseError::fail(error.to_string()))?;
            cancellation.cancel();
            prop_assert!(directory_path(&root.directory, &path, false, &mut &cancelled).is_err());
            prop_assert_eq!(cancelled.receipt().backend_read_operations, 0);
            if depth > 0 {
                // Negative control recreates the old root duplicate before the
                // same production traversal. The exact-read oracle must reject it.
                let legacy = NativeMaterializationWork::new(
                    WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .map_err(|(error, _)| TestCaseError::fail(error.to_string()))?;
                let duplicate = clone_directory(&root.directory, &mut &legacy)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let result = directory_path(&duplicate, &path, false, &mut &legacy);
                prop_assert!(
                    directory_read_admission(
                        depth,
                        u64::MAX,
                        result.is_ok(),
                        legacy.receipt().backend_read_operations,
                    )
                    .is_err()
                );
            }
            Ok(())
        },
    )?;
    Ok(())
}
