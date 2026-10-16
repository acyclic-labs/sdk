use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // The paged uniqueness validator agrees with the volume's public native
    // key for arbitrary sets of distinct stored spellings.
    #[test]
    fn directory_validation_agrees_with_volume_identity(
        names in prop::collection::btree_set(prop_oneof![
            "[a-zA-Z]{1,4}",
            "\\p{L}{1,4}",
            prop::sample::select(vec!["Straße", "STRASSE", "ΟΣ", "ος", "ﬃ", "ffi", "İ", "i\u{307}", "ΐ"])
                .prop_map(str::to_owned),
        ], 0..16),
    ) {
        let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
        let expected = names.iter().map(|name| {
            crate::kernel::LogicalName::new(crate::kernel::NameEncoding::Utf8,
                name.as_bytes().to_vec(), config.limits.maximum_component_bytes)
                .map(|name| name.case_fold_key())
        }).collect::<Result<std::collections::BTreeSet<_>, _>>()?.len() == names.len();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let actual = runtime.block_on(async {
            let mut checkout = crate::facade::test_fixtures::folded_checkout(names.iter().cloned()).await?;
            let cancellation = crate::CancellationToken::new();
            let root = crate::kernel::NamespacePath::new(Vec::new(), config.limits)?;
            let record = checkout.lookup_no_follow(&root, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await?.value.record.ok_or("root missing")?;
            let crate::kernel::FilePayload::Directory { entries } = record.payload else {
                return Err("root is not a directory".into());
            };
            let mut accounting = crate::kernel::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED, &cancellation);
            let keys = checkout.folded_collision_keys(entries, &mut accounting, &cancellation).await?;
            for name in &names {
                let logical = crate::kernel::LogicalName::new(crate::kernel::NameEncoding::Utf8,
                    name.as_bytes().to_vec(), config.limits.maximum_component_bytes)?;
                let key = logical.case_fold_key();
                let expected = names.iter().filter(|other| {
                    crate::kernel::LogicalName::new(crate::kernel::NameEncoding::Utf8,
                        other.as_bytes().to_vec(), config.limits.maximum_component_bytes)
                        .is_ok_and(|name| name.case_fold_key() == key)
                }).count() > 1;
                assert_eq!(crate::facade::ambiguous_folded_name(&keys.value, &logical, &mut accounting)?, expected);
            }
            let unique = keys.value.is_empty();
            keys.retire(&mut accounting)?;
            Ok::<_, Box<dyn std::error::Error>>(unique)
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(actual, expected);
    }

}

fn alias_cases() -> impl Strategy<Value = (String, (&'static str, &'static str), usize)> {
    (
        "[a-z]{0,3}",
        prop::sample::select(vec![
            ("Dir", "dir"),
            ("\u{39f}\u{3a3}", "\u{3bf}\u{3c2}"),
            ("Stra\u{df}e", "STRASSE"),
            ("\u{fb03}", "FFI"),
            ("\u{130}", "i\u{307}"),
            ("\u{390}", "\u{390}"),
        ]),
        1..80usize,
    )
}

// All 64 alias cases retain their strategy, shrinking and assertions. Four
// independent partitions let libtest run the expensive native fixtures in parallel.
macro_rules! alias_properties {
    ($($name:ident),+ $(,)?) => {$(
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(16))]
            #[test]
            fn $name((prefix, pair, repetitions) in alias_cases()) {
                alias_case(prefix, pair, repetitions)?;
            }
        }
    )+};
}

alias_properties!(
    aliases_preserve_identity_and_native_paths_a,
    aliases_preserve_identity_and_native_paths_b,
    aliases_preserve_identity_and_native_paths_c,
    aliases_preserve_identity_and_native_paths_d,
);

// Identity equals the volume key; I/O and parked streams retain native spelling,
// including after a rename through another alias.
fn alias_case(
    prefix: String,
    pair: (&str, &str),
    repetitions: usize,
) -> proptest::test_runner::TestCaseResult {
    let repetitions = repetitions.min(240 / pair.0.len().max(pair.1.len()));
    let spelling = format!("{prefix}{}", pair.0.repeat(repetitions));
    let alias = format!("{prefix}{}", pair.1.repeat(repetitions));
    let original = name(&spelling);
    let (projection, source) = folded::folded_projection(&[
        original,
        name(&spelling).child(b"Child".to_vec()),
        name("new-parent"),
    ])
    .map_err(|error| TestCaseError::fail(error.to_string()))?;
    let work = fuse_fixture_work();
    let parent = folded::lookup(&projection, ROOT_INODE, &spelling, &work)?;
    prop_assert_eq!(
        folded::lookup(&projection, ROOT_INODE, &alias, &work)?,
        parent
    );
    let child = folded::lookup(&projection, parent, "Child", &work)?;
    prop_assert_eq!(folded::lookup(&projection, parent, "child", &work)?, child);
    let destination = folded::lookup(&projection, ROOT_INODE, "new-parent", &work)?;
    {
        let mut state = projection.core.state().map_err(NativeMountError::from)?;
        for inode in [parent, child] {
            let stream = state.directory_stream(source.as_ref(), inode, 0, &work)?;
            let parking = state.streams.prepare_park(&work)?;
            state.streams.park(inode, stream, parking);
        }
    }
    projection.rename_name(
        ROOT_INODE,
        std::ffi::OsStr::new(&alias),
        destination,
        std::ffi::OsStr::new("Moved"),
        0,
        &work,
    )?;
    let mut state = projection.core.state().map_err(NativeMountError::from)?;
    for (inode, expected, parent_inode) in [
        (
            parent,
            name("new-parent").child(b"Moved".to_vec()),
            destination,
        ),
        (
            child,
            name("new-parent")
                .child(b"Moved".to_vec())
                .child(b"child".to_vec()),
            parent,
        ),
    ] {
        let entry = state
            .by_inode
            .get(&inode)
            .ok_or_else(|| TestCaseError::fail("missing inode"))?;
        for binding in entry.bindings.iter() {
            let (key, _) = source
                .folded_path_with_meter(binding.native_path(), &work)?
                .ok_or_else(|| TestCaseError::fail("expected folded identity"))?;
            prop_assert_eq!(&*binding.path, &key);
            prop_assert_eq!(state.inode_by_path.get(&key), Some(&inode));
            let actual = source
                .lookup_with_meter(binding.native_path(), &work)?
                .ok_or_else(|| TestCaseError::fail("native spelling no longer resolves"))?;
            prop_assert_eq!(actual.node.file_id, entry.lookup.node.file_id);
        }
        let stream = state.directory_stream(source.as_ref(), inode, 0, &work)?;
        prop_assert_eq!(&*stream.path, &expected);
        prop_assert_eq!(stream.parent_inode, parent_inode);
        source.read_directory_with_meter(&stream.path, None, 16, &work)?;
    }
    Ok(())
}
