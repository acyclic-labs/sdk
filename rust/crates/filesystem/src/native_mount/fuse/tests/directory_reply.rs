use super::super::{
    AttributeChanges, FuseProjection,
    dir::{CapturedDirectory, DirectoryReply},
};
use super::*;
use proptest::prelude::*;

fn paged_aliases(
    first_size: usize,
    next_size: usize,
    reset_per_page: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    config.profile = crate::model::FilesystemProfile::Posix;
    let (source, _) = shared_sources(config)?;
    let source = Arc::new(source);
    let work = fuse_fixture_work();
    let path = |name: &[u8]| MountPath::root().child(name.to_vec());
    source.create_file(&path(b"a"), FileMetadata::default(), None, &work)?;
    source.write_range(&path(b"a"), 0, bytes::Bytes::from(vec![1; first_size]))?;
    source.hard_link(&path(b"a"), &path(b"b"), &work)?;
    let projection = FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let mut stream = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .directory_stream(source.as_ref(), ROOT_INODE, 2, &work)?;
    projection.position_directory_stream(&mut stream, 2, &work)?;
    // Force the aliases onto different storage pages in one kernel request.
    stream.page = Some(source.read_directory_with_meter(&MountPath::root(), None, 1, &work)?);
    stream.entry_position = 0;
    let entries = CapturedDirectory::default();
    let mut reply = Some(DirectoryReply::Capture(entries.clone()));
    if reset_per_page {
        // Former production ordering, through the same page emitter and oracle.
        loop {
            let first = projection.next_directory_child(&mut stream, &work)?;
            let mut state = projection.core.state().map_err(NativeMountError::from)?;
            state
                .list_directory(
                    source.as_ref(),
                    ROOT_INODE,
                    &mut stream,
                    &mut reply,
                    first,
                    &work,
                )
                .0?;
            if !stream.needs_page() {
                break;
            }
            drop(state);
            source.write_range(&path(b"a"), 0, bytes::Bytes::from(vec![2; next_size]))?;
        }
    } else {
        let mut state = projection.core.state().map_err(NativeMountError::from)?;
        let parking = state.streams.prepare_park(&work)?;
        state.streams.park(ROOT_INODE, stream, parking);
        drop(state);
        projection.list_directory(ROOT_INODE, 2, &mut reply, &work)?;
    }
    let first = entries.borrow();
    let unique = first
        .iter()
        .map(|(_, attr)| attr.ino.0)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        unique.len(),
        first.len(),
        "one inode must not appear twice in one reply"
    );
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, b"a");
    assert_eq!(first[0].1.size, first_size as u64);
    let inode = first[0].1.ino.0;
    drop(first);
    projection.set_attributes(
        inode,
        None,
        &AttributeChanges {
            mode: None,
            uid: None,
            gid: None,
            size: Some(next_size as u64),
            atime: None,
            mtime: None,
            changed: None,
            crtime: None,
            flags: None,
        },
        &work,
    )?;
    let next = CapturedDirectory::default();
    projection.list_directory(
        ROOT_INODE,
        3,
        &mut Some(DirectoryReply::Capture(next.clone())),
        &work,
    )?;
    let next = next.borrow();
    assert_eq!(next.len(), 1, "the alias must start the next request");
    assert_eq!(next[0].0, b"b");
    assert_eq!(next[0].1.size, next_size as u64);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn paged_readdirplus_keeps_one_identity_per_reply(first in 1..16usize, growth in 1..16usize) {
        paged_aliases(first, first + growth, false).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[test]
#[should_panic(expected = "one inode must not appear twice in one reply")]
fn per_page_reply_negative_control() {
    paged_aliases(5, 18, true).unwrap();
}
