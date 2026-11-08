use super::*;
use crate::access::Action;
use crate::native_mount::adapter::tests::access::{deny, publish_test_access};
use proptest::prelude::*;

#[test]
fn cached_hard_links_refuse_missing_metadata_authority_before_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, around) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let work = fuse_fixture_work();
    source.create_file(&name("original"), FileMetadata::default(), None, &work)?;
    let core = projection_core(
        source.clone(),
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )?;
    let projection = crate::native_mount::fuse::FuseProjection { core };
    let original = projection
        .core
        .request(|work| projection.lookup_entry(ROOT_INODE, std::ffi::OsStr::new("original"), work))
        .map_err(|(cause, _)| cause)?;
    publish_test_access(&source, vec![deny("metadata", &[Action::List], "**")])?;
    let refused = projection.core.request(|work| {
        projection.link_name(
            original.attr.ino.0,
            ROOT_INODE,
            std::ffi::OsStr::new("alias"),
            work,
        )
    });
    assert!(matches!(refused, Err((cause, _)) if callback_errno(&cause) == libc::EACCES));
    assert!(
        around.lookup(&name("alias"))?.is_none(),
        "a refused metadata-bearing link installed its destination"
    );
    publish_test_access(&source, vec![])?;
    let linked = projection
        .core
        .request(|work| {
            projection.link_name(
                original.attr.ino.0,
                ROOT_INODE,
                std::ffi::OsStr::new("alias"),
                work,
            )
        })
        .map_err(|(cause, _)| cause)?;
    assert_eq!(linked.attr.ino, original.attr.ino);
    assert_eq!(
        around
            .lookup(&name("alias"))?
            .ok_or("missing installed link")?
            .node
            .link_count,
        2
    );
    Ok(())
}

// A parked source page must disclose exactly the current List decisions, even
// when revocation leaves every namespace/cache stamp unchanged.
fn cached_disclosure(
    hidden: &[bool],
    deny_parent: bool,
    skip: usize,
    advance: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let work = fuse_fixture_work();
    for index in 0..hidden.len() {
        source.create_file(
            &name(&format!("file{index:02}")),
            FileMetadata::default(),
            None,
            &work,
        )?;
    }
    let core = projection_core(
        source.clone(),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let projection = crate::native_mount::fuse::FuseProjection { core };
    let mut stream = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .directory_stream(source.as_ref(), ROOT_INODE, 0, &work)?;
    assert_eq!(
        projection.open_directory(ROOT_INODE, &work)?,
        fuser::FopenFlags::empty()
    );
    projection.position_directory_stream(&mut stream, 0, &work)?;
    stream.page = Some(source.read_directory_with_meter(&MountPath::root(), None, 1, &work)?);
    stream.entry_position = 0;
    let stamp = source.view_stamp_with_meter(&work)?;
    let mut rules = hidden
        .iter()
        .enumerate()
        .filter(|(_, hidden)| **hidden)
        .map(|(index, _)| {
            deny(
                &format!("hidden-{index}"),
                &[Action::List],
                &format!("file{index:02}"),
            )
        })
        .collect::<Vec<_>>();
    if deny_parent {
        rules.push(deny("closed-directory", &[Action::List], ""));
    }
    publish_test_access(&source, rules)?;
    assert_eq!(source.view_stamp_with_meter(&work)?, stamp);
    if deny_parent {
        assert!(matches!(
            projection.open_directory(ROOT_INODE, &work),
            Err(NativeMountError::Source(MountSourceError::AccessDenied))
        ));
        assert!(matches!(
            projection.position_directory_stream(&mut stream, 0, &work),
            Err(NativeMountError::Source(MountSourceError::AccessDenied))
        ));
        return Ok(());
    }
    let skip = skip.min(hidden.iter().filter(|hidden| !**hidden).count());
    projection.position_directory_stream(&mut stream, 2 + skip as u64, &work)?;
    let mut actual = Vec::new();
    loop {
        let next = if advance {
            projection.next_directory_child(&mut stream, &work)?
        } else {
            crate::native_mount::fuse::dir::next_visible_child(source.as_ref(), &mut stream, &work)?
        };
        let Some(child) = next else {
            break;
        };
        actual.push(
            std::str::from_utf8(child.spelled.components().last().ok_or("missing child")?)?
                .to_owned(),
        );
        stream.entry_position += 1;
        stream.emitted += 1;
    }
    let expected = hidden
        .iter()
        .enumerate()
        .filter(|(_, hidden)| !**hidden)
        .skip(skip)
        .map(|(index, _)| format!("file{index:02}"))
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "directory disclosure stopped before an allowed page"
    );
    Ok(())
}

#[test]
fn native_access_fuse_parked_rows_obey_disclosure_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    cached_disclosure(&[true, false, true], false, 0, true)?;
    cached_disclosure(&[true, false, false], false, 1, true)?;
    cached_disclosure(&[false, false], true, 0, true)
}

#[test]
#[should_panic(expected = "directory disclosure stopped before an allowed page")]
fn missing_page_advance_negative_control() {
    cached_disclosure(&[true, false], false, 0, false).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn native_access_fuse_cached_rows_equal_current_list_decisions(
        hidden in prop::collection::vec(any::<bool>(), 0..12),
        deny_parent in any::<bool>(),
        skip in 0..12usize,
    ) {
        cached_disclosure(&hidden, deny_parent, skip, true).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
