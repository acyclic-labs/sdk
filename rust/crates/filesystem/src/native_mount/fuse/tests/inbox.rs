use super::*;
use crate::access::Action;
use crate::native_mount::adapter::tests::access::{deny, publish_test_access};
use proptest::prelude::*;

fn write_inbox(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let (source, inspector) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let work = fuse_fixture_work();
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let path = name("upload");
    publish_test_access(
        &source,
        vec![deny(
            "create-only",
            &[Action::List, Action::Read, Action::Write, Action::Append],
            "upload",
        )],
    )?;
    let created = source.create_file(&path, FileMetadata::default(), None, &work)?;
    let opened = super::super::FuseOpenFile(super::super::fuse_open_file(
        source.open_created_with_meter(&path, &created, &work)?,
        &work,
    )?);
    assert!(matches!(
        opened.file().lookup_with_meter(&work),
        Err(MountSourceError::AccessDenied)
    ));
    let (inode, handle) = {
        let mut state = projection.core.state().map_err(NativeMountError::from)?;
        let inode = state.intern(
            source.as_ref(),
            super::super::fuse_clone_path(&path, &work)?,
            &created,
            None,
            true,
            &HashMap::new(),
            &work,
        )?;
        let (handle, _) = state.admit_file_handle(
            source.as_ref(),
            inode,
            &path,
            &created,
            None,
            libc::O_WRONLY,
            opened,
            true,
            &work,
        )?;
        (inode, handle)
    };
    assert_eq!(
        projection.write_handle(inode, handle, 0, bytes, &work)?,
        u32::try_from(bytes.len())?
    );
    assert_eq!(
        inspector
            .read_range(&path, 0, u32::try_from(bytes.len())?)?
            .as_ref(),
        bytes
    );
    assert!(matches!(
        projection.node_attributes(inode, Some(handle), &work),
        Err(NativeMountError::Source(MountSourceError::AccessDenied))
    ));
    Ok(())
}

#[test]
fn create_only_write_acknowledges_saved_bytes_without_metadata_disclosure()
-> Result<(), Box<dyn std::error::Error>> {
    write_inbox(b"delivered")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Acknowledged length equals the exact persisted bytes for every payload;
    // creating a file never grants List or Read to the created handle.
    #[test]
    fn create_only_write_preserves_payload_and_denies_disclosure(
        bytes in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        write_inbox(&bytes).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
