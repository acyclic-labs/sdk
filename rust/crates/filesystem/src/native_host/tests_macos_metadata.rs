use super::*;
    use super::{HostRoot, NativeMetadataError};
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::unix::fs::MetadataExt as _;
    use std::path::Path;
    use std::time::UNIX_EPOCH;
    #[test]
    fn clone_from_a_separate_held_source_is_cow_and_cannot_escape()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let source_path = temporary.path().join("source");
        let view_path = temporary.path().join("view");
        std::fs::create_dir(&source_path)?;
        std::fs::create_dir(&view_path)?;
        std::fs::write(source_path.join("file"), b"original")?;
        let source = HostRoot::open(&source_path, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let view = HostRoot::open(&view_path, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        if !view.clone_file_from(&source, Path::new("file"), Path::new("clone"), &mut super::ordinary_work()).map_err(super::ordinary_error)? {
            return Ok(()); // This volume does not support APFS cloning.
        }
        std::fs::write(source_path.join("file"), b"changed")?;
        assert_eq!(std::fs::read(view_path.join("clone"))?, b"original");
        assert!(!matches!(
            view.clone_file_from(&source, Path::new("../view/clone"), Path::new("escape"), &mut super::ordinary_work()).map_err(super::ordinary_error),
            Ok(true)
        ));
        assert!(!view_path.join("escape").exists());
        std::os::unix::fs::symlink(&view_path, source_path.join("pivot"))?;
        assert!(!matches!(
            view.clone_file_from(&source, Path::new("pivot/clone"), Path::new("escape"), &mut super::ordinary_work()).map_err(super::ordinary_error),
            Ok(true)
        ));
        assert!(!view_path.join("escape").exists());
        Ok(())
    }
    #[test]
    fn held_metadata_restores_fields_without_retargeting_a_replaced_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut work = super::ordinary_work();
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let original = root_path.join("file");
        std::fs::write(&original, b"old")?;
        let owner = std::fs::metadata(&original)?;
        let root = HostRoot::open(&root_path, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let target = root.open_macos_metadata_target(Path::new("file"), &mut work)?;
        let moved = root_path.join("moved");
        std::fs::rename(&original, &moved)?;
        std::fs::write(&original, b"replacement")?;
        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o4751),
            posix_uid: MetadataField::Value(owner.uid()),
            posix_gid: MetadataField::Value(owner.gid()),
            created_ns: MetadataField::Value(1_234_567_890_123_000_000),
            modified_ns: MetadataField::Value(1_234_567_891_234_000_000),
            accessed_ns: MetadataField::Value(1_234_567_892_345_000_000),
            ..FileMetadata::default()
        };
        target.apply(metadata, &mut work)?;
        let observed = std::fs::metadata(&moved)?;
        assert_eq!(observed.mode() & 0o7777, 0o4751);
        assert_eq!(observed.uid(), owner.uid());
        assert_eq!(observed.gid(), owner.gid());
        for (actual, expected) in [
            (observed.created()?, 1_234_567_890_123_000_000_u128),
            (observed.modified()?, 1_234_567_891_234_000_000_u128),
            (observed.accessed()?, 1_234_567_892_345_000_000_u128),
        ] {
            assert_eq!(actual.duration_since(UNIX_EPOCH)?.as_nanos(), expected);
        }
        assert_eq!(std::fs::read(&original)?, b"replacement");
        assert_ne!(std::fs::metadata(&original)?.mode() & 0o7777, 0o4751);
        assert!(
            root.open_macos_metadata_target(Path::new("../outside"), &mut work)
                .is_err()
        );
        let outside = temporary.path().join("outside");
        std::fs::write(&outside, b"untouched")?;
        std::os::unix::fs::symlink(temporary.path(), root_path.join("pivot"))?;
        assert!(
            root.open_macos_metadata_target(Path::new("pivot/outside"), &mut work)
                .is_err()
        );
        assert_eq!(std::fs::read(outside)?, b"untouched");
        root.open_macos_metadata_target(Path::new("file"), &mut work)?
            .apply(FileMetadata {
                posix_flags: MetadataField::Value(0),
                ..FileMetadata::default()
            }, &mut work)?;
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("file"), &mut work)?
                .apply(FileMetadata {
                    posix_flags: MetadataField::Value(1),
                    ..FileMetadata::default()
                }, &mut work),
            Err(crate::DemandError::MacMetadata(NativeMetadataError(_)))
        ));
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("file"), &mut work)?
                .apply(FileMetadata {
                    changed_ns: MetadataField::Value(1),
                    ..FileMetadata::default()
                }, &mut work),
            Err(crate::DemandError::MacMetadata(NativeMetadataError(_)))
        ));
        Ok(())
    }
    #[test]
    fn fifo_metadata_uses_a_nonblocking_held_handle_and_socket_metadata_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut work = super::ordinary_work();
        use std::os::unix::ffi::OsStrExt as _;
        let temporary = tempfile::tempdir()?;
        let pipe = temporary.path().join("pipe");
        let name = std::ffi::CString::new(pipe.as_os_str().as_bytes())?;
        // SAFETY: `name` is a NUL-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let socket = temporary.path().join("socket");
        let _listener = std::os::unix::net::UnixListener::bind(&socket)?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        root.open_macos_metadata_target(Path::new("pipe"), &mut work)?
            .apply(FileMetadata {
                posix_mode: MetadataField::Value(0o640),
                created_ns: MetadataField::Value(1_234_567_890_123_000_000),
                modified_ns: MetadataField::Value(1_234_567_891_234_000_000),
                accessed_ns: MetadataField::Value(1_234_567_892_345_000_000),
                ..FileMetadata::default()
            }, &mut work)?;
        let observed = std::fs::symlink_metadata(&pipe)?;
        assert_eq!(observed.mode() & 0o7777, 0o640);
        for (actual, expected) in [
            (observed.created()?, 1_234_567_890_123_000_000_u128),
            (observed.modified()?, 1_234_567_891_234_000_000_u128),
            (observed.accessed()?, 1_234_567_892_345_000_000_u128),
        ] {
            assert_eq!(actual.duration_since(UNIX_EPOCH)?.as_nanos(), expected);
        }
        root.open_macos_metadata_target(Path::new("socket"), &mut work)?
            .apply(FileMetadata::default(), &mut work)?;
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("socket"), &mut work)?
                .apply(FileMetadata {
                    posix_mode: MetadataField::Value(0o640),
                    ..FileMetadata::default()
                }, &mut work),
            Err(crate::DemandError::MacMetadata(NativeMetadataError(_)))
        ));
        let old_socket = temporary.path().join("old-socket");
        let pending = root.open_macos_metadata_target(Path::new("socket"), &mut work)?;
        std::fs::rename(&socket, old_socket)?;
        let _replacement = std::os::unix::net::UnixListener::bind(&socket)?;
        assert!(matches!(
            pending.apply(FileMetadata::default(), &mut work),
            Err(crate::DemandError::Io(_))
        ));
        Ok(())
    }
    #[test]
    fn special_node_creation_stays_inside_the_held_root() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::FileTypeExt as _;
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::os::unix::fs::symlink(&outside, root_path.join("pivot"))?;
        let root = HostRoot::open(&root_path, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        assert!(
            root.create_fifo_held(Path::new("pivot/pipe"), 0o600, &mut super::ordinary_work()).map_err(super::ordinary_error)
                .is_err()
        );
        assert!(root.bind_unix_socket(Path::new("pivot/socket"), &mut super::ordinary_work()).map_err(super::ordinary_error).is_err());
        assert!(
            root.create_device_held(
                Path::new("pivot/device"),
                u32::from(libc::S_IFCHR) | 0o600,
                0, &mut super::ordinary_work()).map_err(super::ordinary_error)
            .is_err()
        );
        assert!(std::fs::read_dir(&outside)?.next().is_none());
        root.create_fifo_held(Path::new("pipe"), 0o600, &mut super::ordinary_work()).map_err(super::ordinary_error)?;
        root.bind_unix_socket(Path::new("socket"), &mut super::ordinary_work()).map_err(super::ordinary_error)?;
        assert!(
            std::fs::symlink_metadata(root_path.join("pipe"))?
                .file_type()
                .is_fifo()
        );
        assert!(
            std::fs::symlink_metadata(root_path.join("socket"))?
                .file_type()
                .is_socket()
        );
        Ok(())
    }
