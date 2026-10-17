use super::*;
use acyclic_fs::facade::FsWork;
use acyclic_fs::{CancellationToken, Lifecycle, VolumeConfig, WorkBudget};
use proptest::prelude::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn initialize() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    acyclic_fs::initialize_native_mount_services()?;
    Ok(())
}

async fn recovery(name: &str, durable: bool, recreate: bool) -> TestResult {
    initialize()?;
    let directory = tempfile::tempdir()?;
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path())).await?;
    let config = VolumeConfig::native(if durable {
        Lifecycle::Durable
    } else {
        Lifecycle::Ephemeral
    });
    let original = filesystem
        .create_workspace_with_config(
            name,
            config,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    original.write_text("/retained", "original").await?;
    let head = original
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .id();
    let recovered = if recreate {
        let mut operation = [0; 16];
        operation.copy_from_slice(&blake3::hash(name.as_bytes()).as_bytes()[..16]);
        filesystem
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                name,
                acyclic_fs::VolumeInitialization {
                    config: VolumeConfig::native(Lifecycle::Durable),
                    access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                },
                acyclic_fs::OperationId::from_bytes(operation),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
            )
            .await
            .map(|receipt| receipt.value.workspace())
            .map_err(|failure| Error::Invalid(failure.error.to_string()))
    } else {
        local_owner_workspace(&filesystem, name).await
    };
    let recovered = recovered.expect("existing workspace must recover its recorded creation");
    assert_eq!(recovered.id(), original.id());
    assert_eq!(recovered.volume_config(), config);
    assert_eq!(
        recovered
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .id(),
        head
    );
    assert_eq!(
        recovered
            .read(
                "/retained",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"original"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn existing_owner_workspaces_keep_creation_and_configuration(name in "[a-z]{1,12}", durable in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(recovery(&name, durable, false))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
#[should_panic(expected = "existing workspace must recover its recorded creation")]
async fn recreating_an_existing_owner_is_detected() {
    recovery("previous-creation", true, true).await.unwrap();
}

#[tokio::test]
async fn new_owner_creation_is_atomic_and_recovery_keeps_its_policy() -> TestResult {
    initialize()?;
    let directory = tempfile::tempdir()?;
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path())).await?;
    let first = local_owner_workspace(&filesystem, "new-owner").await?;
    let reopened = local_owner_workspace(&filesystem, "new-owner").await?;
    assert_eq!(first.id(), reopened.id());
    assert_eq!(first.volume_config(), reopened.volume_config());
    let cancel = CancellationToken::new();
    let mut checkout = reopened
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let (policy, _) = checkout
        .read_access_policy(WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    assert_eq!(
        policy
            .ok_or("new owner omitted its initial policy")?
            .bytes
            .as_ref(),
        acyclic_fs::access::INITIAL_POLICY
    );
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Preparation {
    Authorized,
    StaleOptions,
    LateCheck,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReadCeiling {
    All,
    Pinned,
    Path,
}

async fn readonly_import(bytes: &[u8], flow: Preparation, ceiling: ReadCeiling) -> TestResult {
    use acyclic_fs::demand::native::NativeDemandSource;
    use acyclic_fs::{
        LazyWorkspace, MemoryLazyWorkspaceStore, MountOptions, MountPublication, PublicationPermit,
    };
    initialize()?;
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("hot"))?;
    std::fs::write(source.path().join("hot/file"), bytes)?;
    let destination = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let demand = NativeDemandSource::open(
        std::borrow::Cow::Owned(source.path().to_path_buf()),
        config.profile,
        config.limits,
        None,
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?
    .value;
    let identity = uuid::Uuid::from_bytes(acyclic_fs::OperationId::new().into_bytes());
    let name = format!("readonly-import-{identity}");
    let lazy = LazyWorkspace::attach(
        &acyclic_fs::Fs::memory(),
        &name,
        Arc::new(demand),
        MemoryLazyWorkspaceStore::default(),
        acyclic_fs::VolumeInitialization {
            config,
            access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
        },
        acyclic_fs::OperationId::new(),
        &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
    )
    .await?
    .0;
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let original = lazy.workspace().head(&work, &cancellation).await?.id();
    let options = if ceiling != ReadCeiling::All {
        use acyclic_fs::access::{
            Action, Capability, GenerationRange, Generations, Grant, ResourcePattern,
        };
        let issuer = crate::core::AuthorityIssuer::new(
            "readonly-import",
            [37; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: "readonly-import".into(),
            },
        );
        let capability = Capability {
            layers: vec![vec![Grant {
                actions: [Action::List, Action::Read, Action::ReadHistory]
                    .into_iter()
                    .collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: Some(
                        uuid::Uuid::from_bytes(lazy.workspace().id().volume_id().into_bytes())
                            .to_string(),
                    ),
                    generations: if ceiling == ReadCeiling::Pinned {
                        Generations::Range(GenerationRange { first: 1, last: 1 })
                    } else {
                        Generations::All
                    },
                    path: if ceiling == ReadCeiling::Path {
                        "hot/**"
                    } else {
                        "**"
                    }
                    .into(),
                },
            }]],
            expires_at: None,
        };
        let grants = crate::Capabilities::new([capability.to_scope_label()?]);
        let scope = issuer.issue("native-volume", &grants, None, None, &mut None)?;
        authenticated_mount_options(
            lazy.workspace(),
            original,
            NativeAccessAuthority {
                verifier: &issuer.verifier(&mut None)?,
                scope: &scope,
                ceiling: &grants,
            },
            false,
            &work,
        )
        .await?
        .native_context(&work, None, None)
    } else {
        local_owner_mount_options(lazy.workspace(), "readonly-import", false).await?
    }
    .publication(MountPublication::Manual)
    .subdirectory("/hot");
    // The owner's actual ledger is retained by its options.
    let view = options.require_access_view()?.clone();
    assert_eq!(view.current()?.data_generation(), Some(original));
    let plain = MountOptions::read_only()
        .access_view(view.clone())
        .publication(MountPublication::Manual)
        .subdirectory("/hot");
    let expected = "materialization requires the authenticated selection owner";
    assert!(matches!(
        lazy.prepare_native_working_set(
            plain,
            &acyclic_fs::MaterializeOptions::native(destination.path()),
            WorkBudget::UNBOUNDED,
            &cancellation,
            PublicationPermit::Unrestricted,
        )
        .await,
        Err(acyclic_fs::MountLifecycleError::NativePolicy(reason)) if reason == expected
    ));
    assert_eq!(
        lazy.workspace().head(&work, &cancellation).await?.id(),
        original
    );
    let prepared = if flow == Preparation::StaleOptions {
        let exact = lazy
            .exactify(
                "/hot",
                PublicationPermit::Unrestricted,
                &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await?;
        exact
            .prepare_native_working_set(
                &options,
                &acyclic_fs::MaterializeOptions::native(destination.path()),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
    } else {
        if flow == Preparation::LateCheck {
            lazy.exactify(
                "/hot",
                PublicationPermit::Unrestricted,
                &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await?;
        }
        lazy.prepare_native_working_set(
            options,
            &acyclic_fs::MaterializeOptions::native(destination.path()),
            WorkBudget::UNBOUNDED,
            &cancellation,
            PublicationPermit::Unrestricted,
        )
        .await
    };
    if ceiling != ReadCeiling::All {
        if ceiling == ReadCeiling::Path {
            assert_eq!(
                lazy.workspace().head(&work, &cancellation).await?.id(),
                original,
                "refused preparation imported an unresolved tree"
            );
        }
        assert!(
            matches!(
                prepared,
                Err(acyclic_fs::MountLifecycleError::NativePolicy(
                    "materialized working sets require unrestricted disclosure"
                ))
            ),
            "exactification widened a pinned capability or failed outside authorization"
        );
        assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
        return Ok(());
    }
    let prepared = prepared.expect("read-only exactification must retain authorization");
    assert_ne!(
        lazy.workspace().head(&work, &cancellation).await?.id(),
        original
    );
    assert_eq!(
        view.current()?.data_generation(),
        Some(original),
        "rebinding changed an existing pinned view"
    );
    assert_eq!(std::fs::read(prepared.path().join("hot/file"))?, bytes);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn readonly_import_reauthorizes_the_exact_generation(bytes in prop::collection::vec(any::<u8>(), 1..32)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(readonly_import(&bytes, Preparation::Authorized, ReadCeiling::All))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
#[should_panic(expected = "read-only exactification must retain authorization")]
async fn unchanged_preimport_options_are_detected() {
    readonly_import(b"imported", Preparation::StaleOptions, ReadCeiling::All)
        .await
        .unwrap();
}

#[tokio::test]
async fn exactification_cannot_widen_a_pinned_capability() -> TestResult {
    readonly_import(b"pinned", Preparation::Authorized, ReadCeiling::Pinned).await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn rejected_readonly_preparation_never_imports(bytes in prop::collection::vec(any::<u8>(), 1..32)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(readonly_import(&bytes, Preparation::Authorized, ReadCeiling::Path))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
#[should_panic(expected = "refused preparation imported an unresolved tree")]
async fn late_preparation_admission_is_detected() {
    readonly_import(b"late", Preparation::LateCheck, ReadCeiling::Path)
        .await
        .unwrap();
}

async fn deny_materialization<A, O>(workspace: &acyclic_fs::Workspace<A, O>) -> TestResult
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let cancel = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let policy = acyclic_fs::access::Policy {
        schema_version: acyclic_fs::access::POLICY_SCHEMA_VERSION,
        version: 2,
        layers: vec![vec![]],
    };
    checkout
        .write_access_policy(
            &serde_json::to_vec(&policy)?,
            acyclic_fs::NamedAttributeWriteMode::Upsert,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| failure.error)?;
    checkout
        .commit(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancel,
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                )
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}

async fn preparation_disclosure(bytes: &[u8], foreign: bool, bypass: bool) -> TestResult {
    initialize()?;
    let fs = acyclic_fs::Fs::memory();
    let identity = uuid::Uuid::from_bytes(acyclic_fs::OperationId::new().into_bytes());
    let first = preparation_workspace(&fs, &format!("preparation-{identity}")).await?;
    first.write_text("/secret", "first secret").await?;
    let target = if foreign {
        preparation_workspace(&fs, &format!("preparation-{identity}-foreign")).await?
    } else {
        first.clone()
    };
    target
        .write("/secret", bytes::Bytes::copy_from_slice(bytes))
        .await?;
    let cancel = CancellationToken::new();
    let generation = target.head(WorkBudget::UNBOUNDED, &cancel).await?.value;
    let options = local_owner_mount_options(&first, "preparation", foreign)
        .await?
        .publication(acyclic_fs::MountPublication::Manual);
    deny_materialization(&target).await?;
    let destination = tempfile::tempdir()?;
    let materialize = acyclic_fs::MaterializeOptions::native(destination.path());
    let result = if bypass {
        // Former preparation order: raw copying happened before any admission.
        let mut checkout = target
            .checkout(
                acyclic_fs::model::GenerationSelector::Exact(generation.id()),
                acyclic_fs::model::CheckoutMode::read_only_pinned(),
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?
            .value;
        acyclic_fs::native_mount::materialize_checkout(
            &mut checkout,
            &materialize,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| failure.error)?;
        assert_eq!(std::fs::read(destination.path().join("secret"))?, bytes);
        Ok(())
    } else {
        generation
            .prepare_native_working_set(&options, &materialize, WorkBudget::UNBOUNDED, &cancel)
            .await
            .map(|_| ())
    };
    let expected = if foreign {
        "selection differs from its authenticated volume or configuration"
    } else {
        "materialized working sets require unrestricted disclosure"
    };
    assert!(
        matches!(result, Err(acyclic_fs::MountLifecycleError::NativePolicy(reason)) if reason == expected)
            && std::fs::read_dir(destination.path())?.count() == 0,
        "working-set admission disclosed bytes"
    );
    Ok(())
}

async fn preparation_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    fs: &acyclic_fs::Fs<A, O>,
    name: &str,
) -> TestResult<acyclic_fs::Workspace<A, O>> {
    Ok(fs
        .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
            name,
            acyclic_fs::VolumeInitialization {
                config: VolumeConfig::native(Lifecycle::Ephemeral),
                access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
            },
            acyclic_fs::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            None,
        )
        .await
        .map_err(|failure| failure.error)?
        .value
        .workspace())
}

#[tokio::test]
async fn revoked_policy_rejects_previously_bound_generation_before_copy() -> TestResult {
    preparation_disclosure(b"revoked secret", false, false).await
}

#[tokio::test]
async fn foreign_volume_rejects_before_copy() -> TestResult {
    preparation_disclosure(b"foreign secret", true, false).await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn denied_preparation_never_discloses(bytes in prop::collection::vec(any::<u8>(), 1..32), foreign in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(preparation_disclosure(&bytes, foreign, false))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
#[should_panic(expected = "working-set admission disclosed bytes")]
async fn cached_policy_copy_negative_control() {
    preparation_disclosure(b"revoked secret", false, true)
        .await
        .unwrap();
}

#[tokio::test]
#[should_panic(expected = "working-set admission disclosed bytes")]
async fn late_foreign_volume_check_negative_control() {
    preparation_disclosure(b"foreign secret", true, true)
        .await
        .unwrap();
}
