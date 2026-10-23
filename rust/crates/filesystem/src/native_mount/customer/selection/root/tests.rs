use super::*;
use crate::native_mount::customer::tests::{fixture_options, head, native_lazy_source, test_work};
use proptest::prelude::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const DENIED: &[u8] = br#"{"schema_version":1,"version":2,"layers":[]}"#;

#[derive(Clone, Copy)]
enum Surface {
    Workspace,
    Lazy,
}

// The same checker is used by the public API reproducers and by the deliberately
// broken old lookup-before-admission paths. All three shapes must be identical.
fn require_indistinguishable(results: Vec<Result<(), MountLifecycleError>>) {
    assert!(
        results.len() == 3
            && results.into_iter().all(|result| matches!(
                result,
                Err(MountLifecycleError::Source(MountSourceError::AccessDenied))
            )),
        "root metadata disclosed before admission"
    );
}

async fn denied_roots(surface: Surface, bypass: bool, suffix: &str) -> TestResult {
    crate::initialize_native_mount_services()?;
    let cancel = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancel)?;
    let physical = tempfile::tempdir()?;
    let file = format!("file-{suffix}");
    std::fs::write(physical.path().join(&file), b"secret")?;
    let lazy = native_lazy_source(
        physical.path(),
        &format!(
            "denied-root-{}",
            uuid::Uuid::from_bytes(crate::OperationId::new().into_bytes())
        ),
        &cancel,
    )
    .await?;
    let workspace = lazy.workspace();
    workspace.write_text(&format!("/{file}"), "secret").await?;
    // Bind while permitted, then publish revocation. Cached authority is not a
    // substitute for the verified owner's current selection.
    let base = fixture_options(
        workspace,
        MountOptions::read_write().publication(MountPublication::Manual),
    )
    .await?;
    let mut edit = super::super::tests::pending_policy(workspace, &work, DENIED).await?;
    super::super::tests::publish(&mut edit, &work).await?;
    let destination = tempfile::tempdir()?;
    for base in [
        base,
        fixture_options(
            workspace,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?,
    ] {
        let mut results = Vec::new();
        for path in [format!("/missing-{suffix}"), format!("/{file}"), "/".into()] {
            let options = base.clone().subdirectory(path.clone());
            let result = if bypass {
                // Real raw SDK lookups reproduce each removed constructor's ordering.
                // They intentionally omit admission and feed the identical checker.
                match surface {
                    Surface::Workspace => {
                        let mut checkout = workspace
                            .checkout(
                                GenerationSelector::Head,
                                CheckoutMode::tracking_transaction(),
                                &work,
                                &cancel,
                            )
                            .await?;
                        let root = crate::workspace::customer_path_with_accounting(
                            &path,
                            workspace.volume_config(),
                            &mut KernelAccounting::native(&work, &cancel),
                            &cancel,
                        )?;
                        checkout
                            .lookup_no_follow(&root.0, &work, &cancel)
                            .await
                            .map_err(WorkspaceError::from)
                            .and_then(|found| match found.record {
                                None => Err(WorkspaceError::NotFound),
                                Some(record) if record.kind != FileKind::Directory => {
                                    Err(WorkspaceError::NotDirectory)
                                }
                                Some(_) => Ok(()),
                            })
                            .map_err(Into::into)
                    }
                    Surface::Lazy => lazy
                        .lookup(&path, &mut KernelAccounting::native(&work, &cancel))
                        .await
                        .map_err(MountLifecycleError::from)
                        .and_then(|found| {
                            if found.0.stat().kind == SourceNodeKind::Directory {
                                Ok(())
                            } else {
                                Err(WorkspaceError::NotDirectory.into())
                            }
                        }),
                }
            } else {
                match surface {
                    Surface::Workspace => workspace
                        .mount(destination.path(), options)
                        .await
                        .map(|_| ()),
                    Surface::Lazy => lazy.mount(destination.path(), options).await.map(|_| ()),
                }
            };
            results.push(result);
        }
        require_indistinguishable(results);
    }
    assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn workspace_mount_denies_missing_file_and_directory_identically() -> TestResult {
    denied_roots(Surface::Workspace, false, "workspace").await
}

#[tokio::test]
async fn lazy_mount_denies_missing_file_and_directory_identically() -> TestResult {
    denied_roots(Surface::Lazy, false, "lazy").await
}

#[tokio::test]
#[should_panic(expected = "root metadata disclosed before admission")]
async fn workspace_lookup_before_admission_negative_control() {
    denied_roots(Surface::Workspace, true, "workspace-broken")
        .await
        .unwrap();
}

#[tokio::test]
#[should_panic(expected = "root metadata disclosed before admission")]
async fn lazy_lookup_before_admission_negative_control() {
    denied_roots(Surface::Lazy, true, "lazy-broken")
        .await
        .unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    // Property: denied names disclose neither presence nor kind on either
    // constructor, independently of their spelling. Failures shrink the name.
    #[test]
    fn denied_root_shapes_are_indistinguishable(suffix in "[a-z]{1,8}", lazy in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(denied_roots(if lazy { Surface::Lazy } else { Surface::Workspace }, false, &suffix)).unwrap();
    }
}

#[tokio::test]
async fn root_admission_preserves_list_only_sparse_mounts() -> TestResult {
    crate::initialize_native_mount_services()?;
    let cancel = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancel)?;
    let fs = crate::Fs::memory();
    let workspace = fs
        .create_workspace(
            "list-only-admitted-root",
            crate::WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let mut policy: crate::access::Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    policy.layers[0][0].actions = [Action::List].into_iter().collect();
    let policy = serde_json::to_vec(&policy)?;
    let mut edit = super::super::tests::pending_policy(&workspace, &work, &policy).await?;
    super::super::tests::publish(&mut edit, &work).await?;
    let options = fixture_options(&workspace, MountOptions::read_write()).await?;
    let generation = head(&workspace, &work).await?;
    let admitted = AdmittedRoot::admit(&options, &workspace, generation, &work).await?;
    let checkout = workspace
        .checkout(
            GenerationSelector::Exact(generation),
            CheckoutMode::read_only_pinned(),
            &work,
            &cancel,
        )
        .await?;
    let source = admitted.checkout_source(checkout, &work).await?;
    source.cancel();
    Ok(())
}

async fn tracking_cut(bypass: bool, sensitive: bool, advances: u8) -> TestResult {
    crate::initialize_native_mount_services()?;
    let cancel = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancel)?;
    let workspace = crate::Fs::memory()
        .create_workspace(
            format!(
                "tracking-root-{}",
                uuid::Uuid::from_bytes(crate::OperationId::new().into_bytes())
            ),
            crate::WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let mut policy: crate::access::Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    policy.layers[0][0].resource.generations = if sensitive {
        crate::access::Generations::Head
    } else {
        crate::access::Generations::All
    };
    let mut edit =
        super::super::tests::pending_policy(&workspace, &work, &serde_json::to_vec(&policy)?)
            .await?;
    super::super::tests::publish(&mut edit, &work).await?;
    let selected = head(&workspace, &work).await?;
    let options = fixture_options(&workspace, MountOptions::read_write()).await?;
    // This publication is deliberately permitted by the policy lease: policy
    // itself is unchanged, but a Head-only decision no longer covers selected.
    for index in 0..advances {
        workspace
            .write_text("/new-head", &format!("new data {index}"))
            .await?;
    }
    let result = if bypass {
        let binding = options
            .access_refresh
            .as_ref()
            .unwrap()
            .select(selected, &work)?
            .await?;
        assert!(
            binding
                .require_access_view()?
                .current()?
                .decide(Action::List, "", 0)
                .allowed
        );
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Exact(selected),
                CheckoutMode::read_only_pinned(),
                &work,
                &cancel,
            )
            .await?;
        let root = crate::workspace::customer_path_with_accounting(
            "/",
            workspace.volume_config(),
            &mut KernelAccounting::native(&work, &cancel),
            &cancel,
        )?;
        assert!(
            checkout
                .lookup_no_follow(&root.0, &work, &cancel)
                .await?
                .record
                .is_some()
        );
        Ok(())
    } else {
        AdmittedRoot::admit(&options, &workspace, selected, &work)
            .await
            .map(|_| ())
    };
    if sensitive && advances != 0 {
        assert!(
            matches!(
                result,
                Err(MountLifecycleError::NativePolicy(
                    "selection differs from current persisted policy"
                ))
            ),
            "tracking root authorized a different generation"
        );
    } else {
        assert!(
            result.is_ok(),
            "generation-insensitive or unchanged cut refused"
        );
    }
    Ok(())
}

#[tokio::test]
async fn tracking_root_cannot_authorize_an_older_checkout() -> TestResult {
    tracking_cut(false, true, 1).await
}

#[tokio::test]
#[should_panic(expected = "tracking root authorized a different generation")]
async fn current_head_authorizing_old_root_negative_control() {
    tracking_cut(true, true, 1).await.unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    // Property: unchanged cuts remain valid; advancing data invalidates a
    // head-sensitive tracking selection, while all-history grants stay valid.
    #[test]
    fn tracking_admission_respects_generation_sensitivity(sensitive in any::<bool>(), advances in 0u8..4) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(tracking_cut(false, sensitive, advances)).unwrap();
    }
}
