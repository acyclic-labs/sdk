use super::*;
use std::io::Read;
use std::path::Path;
#[test]
fn a_local_root_is_classified_local() -> std::io::Result<()> {
    let temporary = tempfile::tempdir()?;
    assert!(
        filesystem_is_local(
            &HostRoot::open(temporary.path(), &mut ordinary_work())
                .map_err(ordinary_error)?
                .directory,
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
    let root = HostRoot::open(temporary.path(), &mut ordinary_work()).map_err(ordinary_error)?;
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
    let root = HostRoot::open(&root_path, &mut ordinary_work()).map_err(ordinary_error)?;
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
    let root = HostRoot::open(&root_path, &mut ordinary_work()).map_err(ordinary_error)?;
    assert!(
        root.set_permissions_without_open(Path::new("pivot/target"), 0o600, &mut ordinary_work())
            .map_err(ordinary_error)
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
    let root = HostRoot::open(&root_path, &mut ordinary_work()).map_err(ordinary_error)?;
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
        apply_linux_metadata(
            &target,
            FileMetadata {
                created_ns: MetadataField::Value(12),
                ..representable
            },
            &mut work
        ),
        Err(crate::DemandError::LinuxMetadata(NativeMetadataError(_)))
    ));
    assert_eq!(
        std::fs::metadata(&moved)?.permissions().mode() & 0o7777,
        original_mode
    );
    apply_linux_metadata(&target, representable, &mut work)?;
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
    let root = HostRoot::open(&root_path, &mut ordinary_work()).map_err(ordinary_error)?;
    let target = root.open_linux_metadata_target(Path::new("link"), &mut work)?;
    let desired = 1_700_000_000_456_789_123_i64;
    apply_linux_metadata(
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
    assert!(
        HostRoot::open(&link, &mut ordinary_work())
            .map_err(ordinary_error)
            .is_err()
    );
    Ok(())
}
#[test]
fn bound_unix_socket_lands_inside_the_held_root() -> std::io::Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let temporary = tempfile::tempdir()?;
    std::fs::create_dir(temporary.path().join("nested"))?;
    let root = HostRoot::open(temporary.path(), &mut ordinary_work()).map_err(ordinary_error)?;
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
    let root = HostRoot::open(temporary.path(), &mut ordinary_work()).map_err(ordinary_error)?;
    let name = format!("{}.sock", "n".repeat(128));
    match root
        .bind_unix_socket(Path::new(&name), &mut ordinary_work())
        .map_err(ordinary_error)
    {
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput),
        Ok(()) => return Err(std::io::Error::other("over-long socket path was admitted")),
    }
    Ok(())
}
