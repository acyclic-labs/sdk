use super::*;
use proptest::prelude::*;

async fn check_aliases(
    depth: usize,
    separate_parents: bool,
    watched: bool,
    writes: &[Vec<u8>],
) -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let names = ["a", "b"].map(|leaf| {
        let directory = if separate_parents { leaf } else { "d" };
        let mut names = vec![directory; depth];
        names.push(leaf);
        names.join("/")
    });
    for name in &names {
        std::fs::create_dir_all(root.path().join(name).parent().expect("parent"))?;
    }
    std::fs::write(root.path().join(&names[0]), b"source")?;
    std::fs::hard_link(root.path().join(&names[0]), root.path().join(&names[1]))?;
    let demand = native_source(root.path(), crate::model::FilesystemProfile::Portable).await?;
    let demand = if watched { demand } else { demand.unwatched() };
    let view = mounted_view_of(
        Arc::new(demand),
        "alternating-hard-links",
        crate::MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let paths = names
        .each_ref()
        .map(|name| mounted(&name.split('/').collect::<Vec<_>>()));
    let mut parents = vec![MountPath::root()];
    for path in &paths {
        let mut parent = MountPath::root();
        for component in &path.components()[..depth] {
            parent = parent.child(component.clone());
            if !parents.contains(&parent) {
                parents.push(parent.clone());
            }
        }
    }
    let identities = parents
        .iter()
        .map(|parent| {
            view.lookup(parent)
                .map(|found| found.expect("source ancestor").node.file_id)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let held = view.open_file(&paths[0])?;
    let identity = held.lookup()?.node.file_id;
    for (index, bytes) in writes.iter().enumerate() {
        let index = (index + 1) % 2;
        let file = view.open_file(&paths[index])?;
        file.resize(0)?;
        file.write_range(0, Bytes::copy_from_slice(bytes))?;
        for (name, path) in names.iter().zip(&paths) {
            let alias = view.open_file(path)?;
            assert_eq!(alias.lookup()?.node.file_id, identity, "{name}");
            assert_eq!(
                alias.read_range(0, bytes.len() as u32)?.as_ref(),
                bytes,
                "{name}"
            );
        }
        assert_eq!(held.read_range(0, bytes.len() as u32)?.as_ref(), bytes);
        for (parent, expected) in parents.iter().zip(&identities) {
            assert_eq!(
                view.lookup(parent)?.expect("ancestor").node.file_id,
                *expected,
                "writing {} replaced {parent:?}",
                names[index]
            );
        }
    }
    view.sync_async().await?;
    for (parent, expected) in parents.iter().zip(identities) {
        assert_eq!(
            view.lookup(parent)?
                .expect("published ancestor")
                .node
                .file_id,
            expected
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn alternating_source_alias_truncations_preserve_one_file()
-> Result<(), Box<dyn std::error::Error>> {
    check_aliases(1, false, true, &[b"first".to_vec(), b"second".to_vec()]).await
}

#[test]
fn promotions_preserve_ancestor_and_alias_identities_for_generated_writes()
-> Result<(), Box<dyn std::error::Error>> {
    // Property: every observed ancestor keeps its identity through any generated
    // alias promotion/write sequence and publication; aliases share all bytes.
    let runtime = tokio::runtime::Runtime::new()?;
    let strategy = (
        0usize..4,
        any::<bool>(),
        any::<bool>(),
        proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..32), 1..5),
    );
    let config = proptest::test_runner::Config {
        cases: 32,
        source_file: Some(file!()),
        ..Default::default()
    };
    proptest::test_runner::TestRunner::new(config).run(
        &strategy,
        |(depth, separate, watched, writes)| {
            runtime
                .block_on(check_aliases(depth, separate, watched, &writes))
                .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))
        },
    )?;
    Ok(())
}
