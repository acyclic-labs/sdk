//! Reserved root names never cross demand, capture, publication or reopen.
//!
//! Every case iterates [`REQUIRED`], a specification kept apart from the
//! policy table, so dropping or renaming any policy row fails every case that
//! touches that name, and the policy itself must name exactly this set.
use super::checked_commands::call;
use super::*;
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    CancellationToken, OperationReceipt, PublicationPermit, WorkBudget, WorkCounters,
};

/// The root names every checkout path must reserve.
const REQUIRED: [&str; 3] = [".acyclic-sdk", ".git", ".system"];

#[test]
fn the_policy_reserves_exactly_the_required_names() {
    let mut names = crate::workspace::RESERVED_NAMES;
    names.sort_unstable();
    assert_eq!(names, REQUIRED);
}

type Head =
    acyclic_fs::Generation<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

fn write(root: &std::path::Path, relative: &str, bytes: &[u8]) -> Result<()> {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).map_err(super::super::storage)?;
    std::fs::write(path, bytes).map_err(super::super::storage)
}

fn host(root: &std::path::Path, relative: &str) -> Option<Vec<u8>> {
    std::fs::read(root.join(relative)).ok()
}

async fn open(
    directory: &std::path::Path,
    state: &std::path::Path,
    turns: usize,
) -> Result<LocalHarness> {
    let provider = scripted((0..turns).flat_map(|index| {
        [
            call(index, "fs", "status"),
            ScriptedTurn::Text(format!("captured {index}")),
        ]
    }));
    let fs = crate::tools::defaults()?
        .get("fs", None, None)?
        .unwrap()
        .clone();
    LocalHarness::open(
        super::checkout_config(directory, (model()?, provider))?
            .state_directory(state)
            .without_default_tools()
            .tool(fs),
    )
    .await
}

async fn head(harness: &LocalHarness) -> Result<Head> {
    Ok(harness
        .workspace()
        .unwrap()
        .view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(super::super::storage)?
        .value)
}

async fn read(head: &Head, path: &str) -> Result<Option<Vec<u8>>> {
    match head
        .read(
            &format!("/{path}"),
            1024,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
    {
        Ok(bytes) => Ok(Some(bytes.value.as_ref().to_vec())),
        Err(failure) if matches!(failure.error, acyclic_fs::WorkspaceError::NotFound) => Ok(None),
        Err(failure) => Err(super::super::storage(failure.error)),
    }
}

/// Commits `writes` (`None` removes) into the authored head, returning the
/// new head.
async fn commit(harness: &LocalHarness, writes: &[(String, Option<&[u8]>)]) -> Result<Head> {
    let stop = CancellationToken::new();
    let workspace = harness.workspace().unwrap().view.workspace();
    let mut transaction = workspace
        .begin_transaction(
            acyclic_fs::IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .map_err(super::super::storage)?
        .value;
    for (path, bytes) in writes {
        let path = format!("/{path}");
        match bytes {
            Some(bytes) => {
                let parent = &path[..path.rfind('/').unwrap()];
                if !parent.is_empty() {
                    transaction
                        .create_dir_all(parent, WorkBudget::UNBOUNDED, &stop)
                        .await
                        .map_err(super::super::storage)?;
                }
                transaction
                    .write(
                        &path,
                        bytes::Bytes::copy_from_slice(bytes),
                        WorkBudget::UNBOUNDED,
                        &stop,
                    )
                    .await
                    .map_err(super::super::storage)?;
            }
            None => {
                transaction
                    .remove(&path, WorkBudget::UNBOUNDED, &stop)
                    .await
                    .map_err(super::super::storage)?;
            }
        }
    }
    transaction
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .map_err(super::super::storage)?;
    head(harness).await
}

#[tokio::test]
async fn reserved_names_stay_out_of_demand_and_capture() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    for name in REQUIRED {
        for path in [format!("{name}/existing"), format!("{name}/deep/nested")] {
            write(root, &path, b"base")?;
        }
        // Control: the same name below the root is ordinary content.
        write(root, &format!("docs/{name}/existing"), b"base")?;
    }
    let harness = open(root, state.path(), 2).await?;
    for round in 0..2 {
        assert_eq!(
            harness.run("capture").await?.text,
            format!("captured {round}")
        );
        let current = head(&harness).await?;
        let expected: &[u8] = if round == 0 { b"base" } else { b"changed" };
        for name in REQUIRED {
            for path in [
                format!("{name}/existing"),
                format!("{name}/deep/nested"),
                format!("{name}/added"),
            ] {
                assert_eq!(
                    read(&current, &path).await?,
                    None,
                    "{path} entered the head"
                );
            }
            assert_eq!(
                read(&current, &format!("docs/{name}/existing"))
                    .await?
                    .as_deref(),
                Some(expected),
                "docs/{name}"
            );
            // Demand hides every reserved name, so a host edit can never
            // leave a stale copy in the head for the next round to find,
            // while the same name below the root stays ordinary content.
            let view = &harness.workspace().unwrap().view;
            let stop = CancellationToken::new();
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&stop);
            match view
                .inspect(&format!("/{name}/existing"), &mut accounting)
                .await
            {
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => {}
                Ok(lookup) => {
                    view.retire_lookup(lookup, &mut accounting)
                        .map_err(super::super::storage)?;
                    panic!("demand served {name}");
                }
                Err(error) => return Err(super::super::storage(error)),
            }
            let lookup = view
                .inspect(&format!("/docs/{name}/existing"), &mut accounting)
                .await
                .map_err(super::super::storage)?;
            view.retire_lookup(lookup, &mut accounting)
                .map_err(super::super::storage)?;
        }
        for name in REQUIRED {
            for path in [format!("{name}/existing"), format!("docs/{name}/existing")] {
                write(root, &path, b"changed")?;
            }
            write(root, &format!("{name}/added"), b"new")?;
            write(root, &format!("docs/{name}/added"), b"new")?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Publication {
    /// Single-root git materialization.
    Git,
    /// `CheckoutPublication`, shared by multi-root publication and Pi.
    Checkout,
}

async fn publish(
    harness: &LocalHarness,
    publication: Publication,
    from: Head,
    to: Head,
) -> Result<()> {
    let workspace = harness.workspace().unwrap();
    let operation = acyclic_fs::OperationId::new();
    match publication {
        Publication::Git => {
            crate::tools::git::publish_checkout_transition(
                &workspace.path,
                workspace.identity,
                &workspace.store,
                operation,
                from,
                to,
                &|| std::future::ready(Ok(())),
            )
            .await
        }
        Publication::Checkout => {
            use acyclic_fs::MultiRootMaterializer as _;
            let root = acyclic_fs::WorkspaceRootId::from_bytes([1; 16]);
            let materializer = crate::filesystem::WorkspaceRootMaterializer {
                state: workspace.store.clone(),
                physical_roots: std::collections::BTreeMap::from([(
                    root,
                    crate::filesystem::CheckoutRoot {
                        workspace_id: from.workspace().id(),
                        path: workspace.path.clone(),
                        source_root_identity: workspace.identity,
                    },
                )]),
            };
            materializer
                .materialize(operation, root, &from.workspace(), from.id(), to.id())
                .await?;
            materializer.finalize(operation, root).await
        }
    }
}

#[tokio::test]
async fn every_publication_preserves_host_reserved_names() -> Result<()> {
    for publication in [Publication::Git, Publication::Checkout] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let state = tempfile::tempdir().map_err(super::super::storage)?;
        let root = directory.path();
        for name in REQUIRED {
            write(root, &format!("{name}/existing"), b"host")?;
        }
        let harness = open(root, state.path(), 0).await?;
        let base = head(&harness).await?;
        // A transition that writes every reserved name, beside an ordinary
        // control file that publication must write.
        let mut writes: Vec<(String, Option<&[u8]>)> = Vec::new();
        for name in REQUIRED {
            writes.push((format!("{name}/existing"), Some(b"head")));
            writes.push((format!("{name}/new"), Some(b"head")));
        }
        writes.push(("docs/control".into(), Some(b"head")));
        let written = commit(&harness, &writes).await?;
        publish(&harness, publication, base, written.clone()).await?;
        assert_eq!(
            host(root, "docs/control").as_deref(),
            Some(&b"head"[..]),
            "{publication:?}"
        );
        for name in REQUIRED {
            assert_eq!(
                host(root, &format!("{name}/existing")).as_deref(),
                Some(&b"host"[..]),
                "{publication:?} overwrote {name}"
            );
            assert_eq!(
                host(root, &format!("{name}/new")),
                None,
                "{publication:?} wrote {name}"
            );
        }
        // A transition that removes every reserved entry it wrote.
        let removals: Vec<(String, Option<&[u8]>)> = writes
            .iter()
            .map(|(path, _)| (path.clone(), None))
            .collect();
        let removed = commit(&harness, &removals).await?;
        publish(&harness, publication, written, removed).await?;
        assert_eq!(host(root, "docs/control"), None, "{publication:?}");
        for name in REQUIRED {
            assert_eq!(
                host(root, &format!("{name}/existing")).as_deref(),
                Some(&b"host"[..]),
                "{publication:?} removed {name}"
            );
        }
        harness.close().await?;
    }
    Ok(())
}

/// The text the policy record carried when heads recorded one. A head may
/// hold these exact bytes; they must buy nothing.
const RECORD: &[u8] = b"acyclic-checkout-reserved-names-v2\n\
.acyclic-sdk owned=\n\
.git owned=\n\
.system owned=execution,harness-file-metadata,harness-uploads,inherited-conversation,interactions,workflows\n\
";

#[tokio::test]
async fn reopen_removes_forbidden_entries_whatever_the_head_records() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let root = directory.path();
    let owned = ".system/execution/sdk.json";
    for name in REQUIRED {
        write(root, &format!("{name}/imported"), b"host")?;
    }
    let harness = open(root, state.path(), 0).await?;
    // A head holding bytes under every reserved name, beside SDK state of its
    // own and a matching policy record planted where heads once kept one.
    let mut writes: Vec<(String, Option<&[u8]>)> = vec![
        (owned.into(), Some(b"sdk")),
        (".system/reserved-names".into(), Some(RECORD)),
    ];
    for name in REQUIRED {
        writes.push((format!("{name}/imported"), Some(b"host")));
    }
    commit(&harness, &writes).await?;
    harness.close().await?;
    let mut reconciled = None;
    for _ in 0..2 {
        let harness = open(root, state.path(), 0).await?;
        let current = head(&harness).await?;
        for path in REQUIRED
            .iter()
            .map(|name| format!("{name}/imported"))
            .chain([".system/reserved-names".to_owned()])
        {
            assert_eq!(read(&current, &path).await?, None, "{path} survived reopen");
        }
        assert_eq!(read(&current, owned).await?.as_deref(), Some(&b"sdk"[..]));
        for name in REQUIRED {
            assert_eq!(
                host(root, &format!("{name}/imported")).as_deref(),
                Some(&b"host"[..])
            );
        }
        // A reconciled head reopens without another generation.
        let id = current.id();
        assert!(reconciled.is_none_or(|reconciled| reconciled == id));
        reconciled = Some(id);
        harness.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn reopen_replaces_every_non_directory_sdk_container() -> Result<()> {
    for path in [
        ".system",
        ".system/execution",
        ".system/reserved-names/child",
    ] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let state = tempfile::tempdir().map_err(super::super::storage)?;
        let root = directory.path();
        write(root, path, b"host")?;
        let harness = open(root, state.path(), 0).await?;
        let mut writes = vec![(path.to_owned(), Some(&b"host"[..]))];
        if path != ".system" {
            writes.push((".system/workflows/sdk".into(), Some(b"sdk")));
        }
        commit(&harness, &writes).await?;
        harness.close().await?;

        let harness = open(root, state.path(), 0).await?;
        let current = head(&harness).await?;
        assert_eq!(read(&current, path).await?, None, "{path}");
        if path != ".system" {
            assert_eq!(
                read(&current, ".system/workflows/sdk").await?.as_deref(),
                Some(&b"sdk"[..]),
                "{path}"
            );
        }
        assert_eq!(host(root, path).as_deref(), Some(&b"host"[..]));
        harness.close().await?;
    }
    Ok(())
}
