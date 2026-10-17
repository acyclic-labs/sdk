use super::*;
use proptest::prelude::*;

async fn check_aliases(
    depth: usize,
    separate_parents: bool,
    watched: bool,
    writes: &[Vec<u8>],
    rename_source: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut names = ["a", "b"].map(|leaf| {
        let directory = if separate_parents { leaf } else { "d" };
        let mut names = vec![directory; depth];
        names.push(leaf);
        names.join("/")
    });
    for name in &names {
        std::fs::create_dir_all(root.path().join(name).parent().expect("parent"))?;
    }
    if depth != 0 {
        for name in &names {
            std::fs::write(
                root.path().join(name).with_file_name("untouched"),
                b"untouched",
            )?;
        }
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
    let mut paths = names
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
    if rename_source && depth != 0 {
        let mut moved = Vec::new();
        for name in &mut names {
            let (directory, suffix) = name.split_once('/').expect("nested alias");
            if !moved.contains(&directory.to_owned()) {
                std::fs::rename(
                    root.path().join(directory),
                    root.path().join(format!("moved-{directory}")),
                )?;
                moved.push(directory.to_owned());
            }
            *name = format!("moved-{directory}/{suffix}");
        }
        view.fence_changes()?;
        paths = names
            .each_ref()
            .map(|name| mounted(&name.split('/').collect::<Vec<_>>()));
        for parent in parents
            .iter_mut()
            .filter(|parent| !parent.components().is_empty())
        {
            let first = String::from_utf8(parent.components()[0].clone())?;
            parent.components[0] = format!("moved-{first}").into_bytes();
        }
        for name in &names {
            let untouched = std::path::Path::new(name).with_file_name("untouched");
            let path = mounted(
                &untouched
                    .to_str()
                    .expect("portable path")
                    .split('/')
                    .collect::<Vec<_>>(),
            );
            let file = view.open_file(&path)?;
            file.write_range(0, Bytes::from_static(b"changed"))?;
            assert_eq!(file.read_range(0, 9)?.as_ref(), b"changeded");
        }
    }
    view.sync_async().await?;
    let last = writes.last().expect("nonempty writes");
    for path in &paths {
        let alias = view.open_file(path)?;
        let facts = alias.lookup()?.node;
        assert_eq!(facts.file_id, identity);
        assert_eq!(facts.logical_bytes, last.len() as u64);
        assert_eq!(alias.read_range(0, last.len() as u32)?.as_ref(), last);
    }
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
    check_aliases(
        1,
        false,
        true,
        &[b"first".to_vec(), b"second".to_vec()],
        false,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn source_directory_renames_keep_subsequent_writes_working()
-> Result<(), Box<dyn std::error::Error>> {
    for watched in [true, false] {
        check_aliases(
            1,
            false,
            watched,
            &[b"first".to_vec(), b"second".to_vec()],
            true,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn host_capture_splits_aliases_created_under_a_retained_operation()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    for complete in [false, true] {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("seed"), b"source")?;
        let view = mounted_view(source.path(), "capture-retained-operation").await?;

        // Promotion binds an operation on the long-lived checkout. The new file
        // below receives an authored ID derived from that operation and its path.
        view.open_file(&mounted(&["seed"]))?
            .write_range(0, Bytes::from_static(b"promoted"))?;
        let operation = view
            .authored
            .shared_checkout()
            .lock()
            .await
            .bound_authored_operation_id()
            .ok_or("promotion did not bind an authored operation")?;
        let paths = [mounted(&["a"]), mounted(&["z"])];
        let created = view.create_file(&paths[0], FileMetadata::default(), None, &work)?;
        view.write_range(&paths[0], 0, Bytes::from_static(b"before"))?;
        view.hard_link(&paths[0], &paths[1], &work)?;
        assert_eq!(
            view.lookup(&paths[1])?.ok_or("missing alias")?.node.file_id,
            created.node.file_id
        );

        let host = tempfile::tempdir()?;
        std::fs::write(host.path().join("a"), b"before")?;
        std::fs::hard_link(host.path().join("a"), host.path().join("z"))?;
        std::fs::write(host.path().join("save.tmp"), b"after!")?;
        std::fs::rename(host.path().join("save.tmp"), host.path().join("a"))?;
        let captured = if complete {
            paths.as_slice()
        } else {
            &paths[..1]
        };
        view.capture_host_paths(host.path(), captured)?;

        let changed = view.open_file(&paths[0])?;
        let unchanged = view.open_file(&paths[1])?;
        assert_eq!(changed.read_range(0, 6)?.as_ref(), b"after!");
        assert_eq!(unchanged.read_range(0, 6)?.as_ref(), b"before");
        assert_ne!(
            changed.lookup()?.node.file_id,
            unchanged.lookup()?.node.file_id,
            "complete={complete}"
        );
        if !complete {
            assert_eq!(unchanged.lookup()?.node.file_id, created.node.file_id);
        }
        assert_eq!(
            view.authored
                .shared_checkout()
                .lock()
                .await
                .bound_authored_operation_id(),
            Some(operation),
            "capture must retain the original authored operation"
        );
    }
    Ok(())
}

#[test]
fn promotions_preserve_ancestor_and_alias_identities_for_generated_writes()
-> Result<(), Box<dyn std::error::Error>> {
    // Property: ancestors keep their identity through alias writes, external
    // directory renames and publication; published aliases share all bytes.
    // Independent runners keep all 32 cases and their shrinking, without serial
    // native I/O. The runtime is shared by all runners.
    let runtime = tokio::runtime::Runtime::new()?;
    std::thread::scope(|scope| {
        let runners = (0..32)
            .map(|_| {
                let runtime = &runtime;
                scope.spawn(move || {
                    let strategy = (
                        0usize..4,
                        any::<bool>(),
                        any::<bool>(),
                        proptest::collection::vec(
                            proptest::collection::vec(any::<u8>(), 1..32),
                            1..5,
                        ),
                        any::<bool>(),
                    );
                    let config = proptest::test_runner::Config {
                        cases: 1,
                        source_file: Some(file!()),
                        ..Default::default()
                    };
                    proptest::test_runner::TestRunner::new(config)
                        .run(&strategy, |(depth, separate, watched, writes, rename)| {
                            runtime
                                .block_on(check_aliases(depth, separate, watched, &writes, rename))
                                .map_err(|error| {
                                    proptest::test_runner::TestCaseError::fail(error.to_string())
                                })
                        })
                        .map_err(|error| error.to_string())
                })
            })
            .collect::<Vec<_>>();
        for runner in runners {
            runner.join().expect("property runner panicked")?;
        }
        Ok::<_, String>(())
    })?;
    Ok(())
}
