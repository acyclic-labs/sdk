use super::*;
use crate::access::Action;
use crate::native_mount::MountOpenAccess;
use crate::native_mount::adapter::tests::access::{deny, publish_test_access};

#[test]
fn native_access_nfs_open_checks_requested_rights() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"file".to_vec());
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    publish_test_access(&source, vec![deny("no-read", &[Action::Read], "file")])?;
    for access in [MountOpenAccess::Read, MountOpenAccess::ReadWrite] {
        assert!(matches!(
            context.open(&path, access, 0, &work),
            Err(NativeMountError::Source(MountSourceError::AccessDenied))
        ));
    }
    assert!(context.files.read().map_err(|_| "files lock")?.is_empty());
    context.open(&path, MountOpenAccess::Write, 0, &work)?;
    context.open(&path, MountOpenAccess::Metadata, 0, &work)?;
    Ok(())
}

#[test]
fn native_access_nfs_pins_policy_at_open_before_first_read() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"file".to_vec());
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"pinned"))?;
    let handle = context.open(&path, MountOpenAccess::Read, 0, &work)?;
    publish_test_access(&source, vec![deny("revoked", &[Action::Read], "file")])?;
    assert!(
        context
            .open(&path, MountOpenAccess::Read, 0, &work)
            .is_err()
    );
    assert_eq!(
        context
            .file_target(&path, handle, &work)?
            .0
            .0
            .read_range(0, 6)?
            .as_ref(),
        b"pinned"
    );
    Ok(())
}

#[test]
fn native_access_nfs_unlink_does_not_upgrade_existing_handles() -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    let path = MountPath::root().child(b"file".to_vec());
    let created = source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"original"))?;
    publish_test_access(
        &source,
        vec![deny("read-only", &[Action::Write, Action::Append], "file")],
    )?;
    let handle = context.open(&path, MountOpenAccess::Read, 0, &work)?;
    publish_test_access(&source, vec![])?;
    context.unlink(
        &path,
        created,
        || source.remove(&path, Some(created.node.file_id), &work),
        &work,
    )?;
    let file = context.file_target(&path, handle, &work)?.0.0;
    assert_eq!(file.read_range(0, 8)?.as_ref(), b"original");
    assert!(file.write_range(0, Bytes::from_static(b"denied")).is_err());
    Ok(())
}
