use super::*;
use crate::access::Action;
use crate::native_mount::adapter::tests::access::{deny, publish_test_access};
use proptest::prelude::*;

fn cached_disclosure(hidden: &[bool], deny_parent: bool) -> TestResult {
    let (source, context, work) = checkout_context(MountPublication::Manual)?;
    for index in 0..hidden.len() {
        source.create_file(
            &MountPath::root().child(format!("file{index:02}").into_bytes()),
            FileMetadata::default(),
            None,
            &work,
        )?;
    }
    let root = MountPath::root();
    let mut directory = DirectoryRead::new(clone_path(&root, &work)?, context.cache_epochs(&work)?);
    ensure_directory_page(&context, &mut directory, &work)?;
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
            directory.next_visible(&context, &work),
            Err(NativeMountError::Source(MountSourceError::AccessDenied))
        ));
        return Ok(());
    }
    let mut actual = Vec::new();
    while let Some(path) = directory.next_visible(&context, &work)? {
        actual.push(
            std::str::from_utf8(path.components().last().ok_or("missing child")?)?.to_owned(),
        );
        assert!(directory.pop_entry());
    }
    let expected = hidden
        .iter()
        .enumerate()
        .filter(|(_, hidden)| !**hidden)
        .map(|(index, _)| format!("file{index:02}"))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn native_access_nfs_cached_rows_obey_disclosure_revocation() -> TestResult {
    cached_disclosure(&[true, false, true], false)?;
    cached_disclosure(&[false, false], true)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn native_access_nfs_cached_rows_equal_current_list_decisions(
        hidden in prop::collection::vec(any::<bool>(), 0..12),
        deny_parent in any::<bool>(),
    ) {
        cached_disclosure(&hidden, deny_parent).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
