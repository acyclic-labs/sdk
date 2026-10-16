use super::*;
use std::io::Read;
use std::path::Path;
#[test]
fn a_local_root_is_classified_local() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    assert!(
        filesystem_is_local(
            &test_root(temporary.path())?.directory,
            &mut ordinary_work()
        )
        .map_err(ordinary_error)?
    );
    Ok(())
}
#[cfg(target_os = "linux")]
#[test]
fn capability_reads_do_not_change_owned_access_times() -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    use std::time::{Duration, UNIX_EPOCH};
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().to_path_buf();
    let directory = temporary.path().join("directory");
    std::fs::create_dir(&directory)?;
    let file = directory.join("file");
    std::fs::write(&file, b"body")?;
    let old = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    for path in [&file, &directory, &root_path] {
        std::fs::File::open(path)?.set_times(std::fs::FileTimes::new().set_accessed(old))?;
    }
    let root = test_root(temporary.path())?;
    let mut body = String::new();
    root.open_file(Path::new("directory/file"), &mut ordinary_work())
        .map_err(ordinary_error)?
        .read_to_string(&mut body)?;
    assert_eq!(body, "body");
    let mut work = ordinary_work();
    for path in [Path::new("directory"), Path::new("")] {
        let (mut reader, _) = root
            .read_dir_stats(path, &mut work)
            .map_err(ordinary_error)?;
        assert!(reader.next(&mut work).map_err(ordinary_error)?.is_some());
        assert!(reader.next(&mut work).map_err(ordinary_error)?.is_none());
    }
    for path in [&file, &directory, &root_path] {
        assert_eq!(std::fs::metadata(path)?.atime(), 1_700_000_000);
    }
    Ok(())
}
/// Every way of stating one file reports one handle, so its identity does
/// not depend on which query saw it, and another file reports another.
#[cfg(target_os = "linux")]
#[test]
fn every_stat_of_a_file_reports_its_one_handle() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"body")?;
    std::fs::write(temporary.path().join("other"), b"body")?;
    let root = test_root(temporary.path())?;
    let mut work = ordinary_work();

    let named = root
        .stat(Path::new("file"), &mut work)
        .map_err(ordinary_error)?;
    let opened = root
        .stat_file(&File::open(temporary.path().join("file"))?, &mut work)
        .map_err(ordinary_error)?;
    let (mut reader, _) = root
        .read_dir_stats(Path::new(""), &mut work)
        .map_err(ordinary_error)?;
    let mut listed = None;
    while let Some((entry, _)) = reader.next(&mut work).map_err(ordinary_error)? {
        if entry.is_named("file") {
            listed = entry.stat;
        }
    }
    let listed = listed.ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
    let handle = named.handle();
    assert_eq!(opened.handle(), handle);
    assert_eq!(listed.handle(), handle);
    let other = root
        .stat(Path::new("other"), &mut work)
        .map_err(ordinary_error)?;
    // A file system without handles (an overlay without exports) reports
    // none for either.
    if handle.is_some() {
        assert_ne!(other.handle(), handle);
    }
    Ok(())
}
#[test]
fn held_root_rejects_intermediate_symlink_escape_for_reads_and_writes() -> std::io::Result<()> {
    use std::os::unix::fs::symlink;
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    let outside_path = temporary.path().join("outside");
    std::fs::create_dir(&root_path)?;
    std::fs::create_dir(&outside_path)?;
    std::fs::write(outside_path.join("secret"), b"outside")?;
    symlink(&outside_path, root_path.join("pivot"))?;
    let root = test_root(&root_path)?;
    assert!(
        root.open_file(Path::new("pivot/secret"), &mut ordinary_work())
            .map_err(ordinary_error)
            .is_err()
    );
    assert!(
        root.stat(Path::new("pivot/secret"), &mut ordinary_work())
            .map_err(ordinary_error)
            .is_err()
    );
    assert!(
        root.create_file(Path::new("pivot/created"), &mut ordinary_work())
            .map_err(ordinary_error)
            .is_err()
    );
    assert!(!outside_path.join("created").exists());
    let mut secret = String::new();
    std::fs::File::open(outside_path.join("secret"))?.read_to_string(&mut secret)?;
    assert_eq!(secret, "outside");
    Ok(())
}
#[test]
fn held_special_metadata_rejects_intermediate_symlink_escape() -> std::io::Result<()> {
    use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _, symlink};
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    let outside_path = temporary.path().join("outside");
    std::fs::create_dir(&root_path)?;
    std::fs::create_dir(&outside_path)?;
    let target = outside_path.join("target");
    std::fs::write(&target, b"outside")?;
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))?;
    symlink(&outside_path, root_path.join("pivot"))?;
    let root = test_root(&root_path)?;
    #[cfg(target_os = "linux")]
    assert!(
        root.open_linux_metadata_target(Path::new("pivot/target"), &mut ordinary_work())
            .is_err()
    );
    #[cfg(target_os = "macos")]
    assert!(
        open_macos_metadata_target(
            &root.directory,
            Path::new("pivot/target"),
            &mut ordinary_work()
        )
        .is_err()
    );
    assert!(
        root.create_fifo_held(Path::new("pivot/created"), 0o600, &mut ordinary_work())
            .map_err(ordinary_error)
            .is_err()
    );
    assert!(!outside_path.join("created").exists());
    root.create_fifo_held(Path::new("fifo"), 0o600, &mut ordinary_work())
        .map_err(ordinary_error)?;
    assert!(
        std::fs::symlink_metadata(root_path.join("fifo"))?
            .file_type()
            .is_fifo()
    );
    assert_eq!(
        std::fs::metadata(&target)?.permissions().mode() & 0o777,
        0o644
    );
    assert_eq!(std::fs::read(target)?, b"outside");
    Ok(())
}
#[cfg(target_os = "linux")]
#[test]
fn held_linux_metadata_targets_inode_across_rename_and_replacement()
-> Result<(), Box<dyn std::error::Error>> {
    let mut work = ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    std::fs::create_dir(&root_path)?;
    let original = root_path.join("original");
    let moved = root_path.join("moved");
    std::fs::write(&original, b"old")?;
    let root = test_root(&root_path)?;
    let target = root.open_linux_metadata_target(Path::new("original"), &mut work)?;
    std::fs::rename(&original, &moved)?;
    std::fs::write(&original, b"new")?;
    std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o644))?;
    let owner = std::fs::metadata(&moved)?;
    let original_mode = owner.permissions().mode() & 0o7777;
    let atime_ns = 1_700_000_000_123_456_789_i64;
    let mtime_ns = 1_700_000_001_987_654_321_i64;
    let representable = FileMetadata {
        posix_mode: MetadataField::Value(0o600),
        posix_uid: MetadataField::Value(owner.uid()),
        posix_gid: MetadataField::Value(owner.gid()),
        accessed_ns: MetadataField::Value(atime_ns),
        modified_ns: MetadataField::Value(mtime_ns),
        ..FileMetadata::default()
    };
    assert!(matches!(
        apply_metadata(
            &target,
            FileMetadata {
                created_ns: MetadataField::Value(12),
                ..representable
            },
            &mut work
        ),
        Err(crate::DemandError::NativeMetadata(NativeMetadataError(_)))
    ));
    assert_eq!(
        std::fs::metadata(&moved)?.permissions().mode() & 0o7777,
        original_mode
    );
    apply_metadata(&target, representable, &mut work)?;
    let old = std::fs::metadata(&moved)?;
    assert_eq!(old.permissions().mode() & 0o777, 0o600);
    assert_eq!(old.atime() * 1_000_000_000 + old.atime_nsec(), atime_ns);
    assert_eq!(old.mtime() * 1_000_000_000 + old.mtime_nsec(), mtime_ns);
    assert_eq!(std::fs::read(&moved)?, b"old");
    assert_eq!(std::fs::read(&original)?, b"new");
    assert_eq!(
        std::fs::metadata(&original)?.permissions().mode() & 0o777,
        0o644
    );
    Ok(())
}
#[cfg(target_os = "linux")]
#[test]
fn held_linux_symlink_metadata_does_not_mutate_target() -> Result<(), Box<dyn std::error::Error>> {
    let mut work = ordinary_work();
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::unix::fs::{MetadataExt as _, symlink};
    let temporary = tempfile::tempdir()?;
    let root_path = temporary.path().join("root");
    std::fs::create_dir(&root_path)?;
    let outside = temporary.path().join("outside");
    std::fs::write(&outside, b"outside")?;
    let link = root_path.join("link");
    symlink(&outside, &link)?;
    let outside_before = std::fs::metadata(&outside)?;
    let root = test_root(&root_path)?;
    let target = root.open_linux_metadata_target(Path::new("link"), &mut work)?;
    let desired = 1_700_000_000_456_789_123_i64;
    apply_metadata(
        &target,
        FileMetadata {
            modified_ns: MetadataField::Value(desired),
            ..FileMetadata::default()
        },
        &mut work,
    )?;
    let link_after = std::fs::symlink_metadata(&link)?;
    let outside_after = std::fs::metadata(&outside)?;
    assert_eq!(
        link_after.mtime() * 1_000_000_000 + link_after.mtime_nsec(),
        desired
    );
    assert_eq!(outside_after.mtime(), outside_before.mtime());
    assert_eq!(outside_after.mtime_nsec(), outside_before.mtime_nsec());
    assert_eq!(std::fs::read(outside)?, b"outside");
    Ok(())
}
#[test]
fn root_symbolic_link_is_never_admitted() -> std::io::Result<()> {
    use std::os::unix::fs::symlink;
    let temporary = tempfile::tempdir()?;
    let real = temporary.path().join("real");
    let link = temporary.path().join("link");
    std::fs::create_dir(&real)?;
    symlink(&real, &link)?;
    assert!(test_root(&link).is_err());
    Ok(())
}
#[test]
fn bound_unix_socket_lands_inside_the_held_root() -> std::io::Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let temporary = tempfile::tempdir()?;
    std::fs::create_dir(temporary.path().join("nested"))?;
    let root = test_root(temporary.path())?;
    root.bind_unix_socket(Path::new("root.sock"), &mut ordinary_work())
        .map_err(ordinary_error)?;
    root.bind_unix_socket(Path::new("nested/child.sock"), &mut ordinary_work())
        .map_err(ordinary_error)?;
    for bound in ["root.sock", "nested/child.sock"] {
        let file_type = std::fs::symlink_metadata(temporary.path().join(bound))?.file_type();
        assert!(file_type.is_socket(), "{bound} is not a socket");
    }
    Ok(())
}
#[cfg(target_os = "macos")]
#[test]
fn bound_unix_socket_rejects_paths_beyond_sun_path() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = test_root(temporary.path())?;
    let name = format!("{}.sock", "n".repeat(128));
    match root.bind_unix_socket(Path::new(&name), &mut ordinary_work()) {
        Err(error) => assert!(matches!(error, DemandError::InvalidRequest)),
        Ok(()) => return Err(std::io::Error::other("over-long socket path was admitted")),
    }
    Ok(())
}

#[cfg(target_os = "linux")]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Root metadata always targets the held directory, even when its name is replaced.
    #[test]
    fn empty_metadata_path_keeps_the_held_root(mode in 0_u32..=0o777) {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let temporary = tempfile::tempdir().unwrap();
        let original = temporary.path().join("root");
        let moved = temporary.path().join("moved");
        std::fs::create_dir(&original).unwrap();
        let root = test_root(&original).unwrap();
        std::fs::rename(&original, &moved).unwrap();
        std::fs::create_dir(&original).unwrap();
        std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut work = ordinary_work();
        let target = root.open_linux_metadata_target(Path::new(""), &mut work).unwrap();
        proptest::prop_assert_eq!(target.metadata().unwrap().ino(), std::fs::metadata(&moved).unwrap().ino());
        apply_metadata(&target, FileMetadata {
            posix_mode: MetadataField::Value(mode),
            ..FileMetadata::default()
        }, &mut work).unwrap();
        proptest::prop_assert_eq!(std::fs::metadata(&moved).unwrap().permissions().mode() & 0o777, mode);
        proptest::prop_assert_eq!(std::fs::metadata(&original).unwrap().permissions().mode() & 0o777, 0o700);
    }
}
