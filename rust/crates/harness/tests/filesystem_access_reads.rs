use acyclic_fs::access::*;
use acyclic_fs::{CancellationToken, Fs, WorkBudget};
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey,
    conversation::{
        ContentGrant, ContentResidencyVerifier, FileRef, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer, Scope},
    filesystem::{FilesystemContentVerifier, FilesystemHost},
    resources::{GenerationRef, ProviderRef},
};
use proptest::prelude::*;
use std::{error::Error, sync::Arc};
#[path = "support/access_policy.rs"]
mod policy_support;

#[path = "filesystem_access_coarse.rs"]
mod coarse;
#[path = "filesystem_access_copies.rs"]
mod copies;
#[path = "filesystem_access_fork_fence.rs"]
mod fork_fence;
#[path = "filesystem_access_imports.rs"]
mod imports;
#[path = "file_query/objects.rs"]
mod objects;
#[path = "filesystem_access_observation.rs"]
mod observation;
#[path = "filesystem_access_ranges.rs"]
mod ranges;

#[path = "support/content_context.rs"]
mod context;

struct Fixture<O = acyclic_fs::MemoryObjectBackend> {
    fs: Fs<acyclic_fs::MemoryAuthorityBackend, O>,
    host: Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, O>>,
    issuer: AuthorityIssuer,
    volume: VolumeRef,
    scope: Scope,
    file: FileRef,
}

impl Fixture {
    async fn new() -> Result<Self, Box<dyn Error>> {
        Self::with_policy(true).await
    }

    async fn with_policy(initialize: bool) -> Result<Self, Box<dyn Error>> {
        Self::with_filesystem(Fs::memory(), initialize).await
    }
}

impl<O: acyclic_fs::AsyncObjectStore + 'static> Fixture<O> {
    fn generation(&self) -> Result<GenerationRef, Box<dyn Error>> {
        Ok(GenerationRef::new(
            self.volume.provider(),
            &hex::decode(self.file.version())?,
            None,
            None,
        )?
        .0)
    }

    async fn with_filesystem(
        fs: Fs<acyclic_fs::MemoryAuthorityBackend, O>,
        initialize: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let provider = ProviderRef::new(
            "access-test".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(fs.clone(), &provider, None)?;
        let volume = VolumeRef::new(
            provider,
            format!("project-{}", uuid::Uuid::new_v4()),
            VolumeClass::Project,
            VolumeOwner::Project("owner".into()),
            &mut None,
        )?;
        if initialize {
            host.create_volume(&volume).await?;
        } else {
            fs.create_workspace(
                volume.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        }
        let issuer = AuthorityIssuer::new(
            "owner",
            [19; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "read-task".into(),
            },
        );
        let scope = issuer.issue(
            "legacy",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([7; 16])),
            None,
            &mut None,
        )?;
        let writer = ContentGrant::verify(
            &issuer.verifier(&mut None)?,
            &scope,
            &volume,
            VolumeOperation::Write,
            4096,
            &mut None,
        )?;
        let file = if initialize {
            host.put_content(
                &volume,
                &writer,
                "secrets/key",
                b"private",
                "text/plain",
                "key",
                4096,
                &IdempotencyKey::new("file")?,
                None,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await?
        } else {
            Self::seed_without_policy(&host, &volume).await?
        };
        Ok(Self {
            fs,
            host,
            issuer,
            volume,
            scope,
            file,
        })
    }

    async fn seed_without_policy(
        host: &FilesystemHost<acyclic_fs::MemoryAuthorityBackend, O>,
        volume: &VolumeRef,
    ) -> Result<FileRef, Box<dyn Error>> {
        // Seed a deliberately policy-less volume through the low-level SDK
        // owner; the content boundary itself must refuse all new writes.
        let workspace = acyclic_harness::filesystem::workspace_ref(
            volume.provider().clone(),
            &volume.storage_name(&mut None)?,
        )?;
        let generation = host
            .apply(
                &workspace,
                None,
                &[
                    acyclic_harness::filesystem::WorkspaceMutation::CreateDirectory {
                        path: "/secrets".into(),
                    },
                    acyclic_harness::filesystem::WorkspaceMutation::PutFile {
                        path: "/secrets/key".into(),
                        bytes: bytes::Bytes::from_static(b"private").into(),
                    },
                ],
                &IdempotencyKey::new("raw-seed")?,
            )
            .await?;
        Ok(FileRef::new(
            volume.clone(),
            "secrets/key".into(),
            hex::encode(generation.as_resource().key()),
            acyclic_harness::conversation::FileDescriptor::from_bytes(
                b"private",
                "text/plain",
                &mut None,
            )?,
            "key".into(),
            &mut None,
        )?)
    }

    fn reader(
        &self,
        scope: &Scope,
    ) -> Result<Arc<FilesystemContentVerifier<acyclic_fs::MemoryAuthorityBackend, O>>, Box<dyn Error>>
    {
        Ok(FilesystemContentVerifier::new(
            self.host.clone(),
            &self.issuer.verifier(&mut None)?,
            scope,
            4096,
            None,
        )?)
    }

    async fn policy(&self, deny: bool, version: u64) -> Result<(), Box<dyn Error>> {
        let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
        policy.version = version;
        if deny {
            let mut rule = policy
                .layers
                .first_mut()
                .and_then(|layer| layer.first_mut())
                .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture rule"))?
                .clone();
            rule.id = "hidden-secret".into();
            rule.effect = Effect::Deny;
            rule.actions = [Action::Read].into_iter().collect();
            rule.resource.path = "secrets/**".into();
            policy
                .layers
                .first_mut()
                .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture layer"))?
                .push(rule);
        }
        self.publish_policy(policy).await
    }

    async fn workspace(
        &self,
        volume: &VolumeRef,
    ) -> Result<acyclic_fs::Workspace<acyclic_fs::MemoryAuthorityBackend, O>, Box<dyn Error>> {
        Ok(self
            .fs
            .open_workspace(
                volume.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?
            .value)
    }

    async fn publish_policy(&self, policy: Policy) -> Result<(), Box<dyn Error>> {
        let cancellation = CancellationToken::new();
        let workspace = self
            .fs
            .open_workspace(
                self.volume.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        policy_support::publish(&workspace, &policy).await
    }

    fn narrowed(&self, grants_path: bool, expired: bool) -> Result<Scope, Box<dyn Error>> {
        self.narrowed_for(&[Action::Read, Action::ReadHistory], grants_path, expired)
    }

    fn narrowed_for(
        &self,
        actions: &[Action],
        grants_path: bool,
        expired: bool,
    ) -> Result<Scope, Box<dyn Error>> {
        let capability = Capability {
            layers: vec![vec![Grant {
                actions: actions.iter().copied().collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::All,
                    path: if grants_path { "**" } else { "public/**" }.into(),
                },
            }]],
            expires_at: expired.then_some(1),
        };
        Ok(self.issuer.issue(
            "ceiling",
            &Capabilities::new([
                self.volume.capability(VolumeOperation::Read, &mut None)?,
                capability.to_scope_label()?,
            ]),
            self.scope.agent(),
            None,
            &mut None,
        )?)
    }
}

#[tokio::test]
async fn legacy_task_volume_grant_cannot_bypass_persisted_denial() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let reader = fixture.reader(&fixture.scope)?;
    let context =
        context::content_context(reader.clone(), None, fixture.scope.capabilities().clone())
            .await?;
    assert_eq!(
        context.read_file(&fixture.file, None).await?.as_ref(),
        b"private"
    );
    fixture.policy(true, 2).await?;
    assert!(matches!(
        context.read_file(&fixture.file, None).await,
        Err(acyclic_harness::Error::Unauthorized(_))
    ));
    fixture.policy(false, 3).await?;
    assert_eq!(
        context.read_file(&fixture.file, None).await?.as_ref(),
        b"private"
    );
    Ok(())
}

#[tokio::test]
async fn public_host_grants_keep_their_scope_and_observe_current_policy()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let volume = ContentGrant::verify_read(&verifier, &fixture.scope, &fixture.file)?;
    let exact_scope = fixture.issuer.issue(
        "exact",
        &Capabilities::new([fixture.file.read_capability(&mut None)?]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?;
    let exact = ContentGrant::verify_file_read(&verifier, &exact_scope, &fixture.file)?;
    let directory = ContentGrant::verify_directory_read(
        &verifier,
        &fixture.scope,
        &fixture.volume,
        "secrets",
        &mut None,
    )?;
    let generation = fixture.generation()?;
    for (revision, denied) in [false, true, false].into_iter().enumerate() {
        fixture.policy(denied, 2 + revision as u64).await?;
        for grant in [&volume, &exact, &directory] {
            assert_eq!(
                fixture
                    .host
                    .read_content(&fixture.file, grant, 4096, None)
                    .await
                    .is_ok(),
                !denied,
            );
        }
        let prefix = fixture
            .host
            .read_private_prefix(
                &fixture.volume,
                &directory,
                fixture.file.path(),
                &generation,
                3,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await;
        assert_eq!(prefix.is_ok(), !denied);
        if !denied {
            assert_eq!(prefix?.as_ref(), b"pri");
        }
        let full = fixture
            .host
            .read_private_path(
                &fixture.volume,
                &directory,
                fixture.file.path(),
                Some(&generation),
                4096,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await;
        assert_eq!(full.is_ok(), !denied);
        if !denied {
            let (reference, bytes) = full?;
            assert_eq!(reference, fixture.file);
            assert_eq!(bytes.as_ref(), b"private");
        }
    }
    let write = ContentGrant::verify(
        &verifier,
        &fixture.scope,
        &fixture.volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?;
    assert!(
        fixture
            .host
            .read_content(&fixture.file, &write, 4096, None)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn legacy_label_cannot_bypass_capability_path_or_expiry() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    for (grants_path, expired, allowed) in [
        (true, false, true),
        (false, false, false),
        (true, true, false),
    ] {
        let scope = fixture.narrowed(grants_path, expired)?;
        assert_eq!(
            fixture
                .reader(&scope)?
                .read(&fixture.file, 4096, None, None)?
                .await
                .is_ok(),
            allowed
        );
    }
    Ok(())
}

#[tokio::test]
async fn content_reads_fail_closed_without_a_persisted_policy() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::with_policy(false).await?;
    assert!(matches!(
        fixture
            .reader(&fixture.scope)?
            .read(&fixture.file, 4096, None, None)?
            .await,
        Err(acyclic_harness::Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn reference_metadata_needs_list_but_known_content_does_not() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    for explicit_deny in [false, true] {
        let scope = if explicit_deny {
            let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
            policy.version = 2;
            let mut deny = policy
                .layers
                .first_mut()
                .and_then(|layer| layer.first_mut())
                .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture rule"))?
                .clone();
            deny.id = "no-reference-disclosure".into();
            deny.effect = Effect::Deny;
            deny.actions = [Action::List].into_iter().collect();
            deny.resource.path = "secrets/**".into();
            policy
                .layers
                .first_mut()
                .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture layer"))?
                .push(deny);
            fixture.publish_policy(policy).await?;
            fixture.scope.clone()
        } else {
            fixture.narrowed(true, false)?
        };
        let grant = ContentGrant::verify_directory_read(
            &verifier,
            &scope,
            &fixture.volume,
            "secrets",
            &mut None,
        )?;
        let generation = fixture.generation()?;
        assert_eq!(
            fixture
                .host
                .read_content(&fixture.file, &grant, 4096, None)
                .await?
                .as_ref(),
            b"private"
        );
        assert_eq!(
            fixture
                .host
                .read_private_prefix(
                    &fixture.volume,
                    &grant,
                    fixture.file.path(),
                    &generation,
                    3,
                    acyclic_harness::conversation::ContentAccess::default()
                )
                .await?
                .as_ref(),
            b"pri"
        );
        assert!(matches!(
            fixture
                .host
                .read_private_path(
                    &fixture.volume,
                    &grant,
                    fixture.file.path(),
                    Some(&generation),
                    4096,
                    acyclic_harness::conversation::ContentAccess::default()
                )
                .await,
            Err(acyclic_harness::Error::Unauthorized(_))
        ));
    }
    Ok(())
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn private_raw_file_reads_keep_bounds_and_meter_the_supplied_ledger()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let workspace = acyclic_harness::filesystem::workspace_ref(
        fixture.volume.provider().clone(),
        &fixture.volume.storage_name(&mut None)?,
    )?;
    for (index, (body, media_type)) in [
        (b"raw bytes".as_slice(), "text/plain"),
        (b"raw\xffbytes".as_slice(), "application/octet-stream"),
        (b"raw\0bytes".as_slice(), "application/octet-stream"),
    ]
    .into_iter()
    .enumerate()
    {
        let generation = fixture
            .host
            .apply(
                &workspace,
                None,
                &[acyclic_harness::filesystem::WorkspaceMutation::PutFile {
                    path: "/raw".into(),
                    bytes: body.to_vec().into(),
                }],
                &IdempotencyKey::new(format!("raw/{index}"))?,
            )
            .await?;
        let grant = ContentGrant::verify_directory_read(
            &fixture.issuer.verifier(&mut None)?,
            &fixture.scope,
            &fixture.volume,
            "",
            &mut None,
        )?;
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?;
        let (reference, bytes) = fixture
            .host
            .read_private_path(
                &fixture.volume,
                &grant,
                "raw",
                Some(&generation),
                4096,
                acyclic_harness::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io),
                },
            )
            .await?;
        assert_eq!(bytes.as_ref(), body);
        assert_eq!(reference.path(), "raw");
        assert_eq!(reference.descriptor().media_type(), media_type);
        assert_eq!(reference.descriptor().byte_length(), 9);
        assert!(
            fixture
                .host
                .read_private_path(
                    &fixture.volume,
                    &grant,
                    "raw",
                    Some(&generation),
                    3,
                    acyclic_harness::conversation::ContentAccess {
                        ceiling: None,
                        io: Some(&io)
                    },
                )
                .await
                .is_err()
        );
        assert!(work.receipt().backend_read_operations > 0);
    }
    Ok(())
}

#[tokio::test]
async fn structured_only_reads_and_ambiguous_payloads_use_the_same_gate()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let allow = Capability {
        layers: vec![vec![Grant {
            actions: [Action::Read, Action::ReadHistory].into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: "**".into(),
            },
        }]],
        expires_at: None,
    };
    let only = fixture.issuer.issue(
        "structured",
        &Capabilities::new([allow.to_scope_label()?]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?;
    assert_eq!(
        fixture
            .reader(&only)?
            .read(&fixture.file, 4096, None, None)?
            .await?
            .as_ref(),
        b"private"
    );
    let narrower = allow.clone().attenuate(vec![], None);
    let ambiguous = fixture.issuer.issue(
        "ambiguous",
        &Capabilities::new([
            fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            allow.to_scope_label()?,
            narrower.to_scope_label()?,
        ]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?;
    assert!(matches!(
        fixture
            .reader(&ambiguous)?
            .read(&fixture.file, 4096, None, None)?
            .await,
        Err(acyclic_harness::Error::Unauthorized(_))
    ));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn production_read_is_policy_intersected_with_the_presented_ceiling(
        deny in any::<bool>(), grants_path in any::<bool>(), expired in any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let fixture = Fixture::new().await?;
            fixture.policy(deny, 2).await?;
            let scope = fixture.narrowed(grants_path, expired)?;
            let allowed = fixture.reader(&scope)?.read(&fixture.file, 4096, None, None)?.await.is_ok();
            assert_eq!(allowed, !deny && grants_path && !expired);
            let grant = ContentGrant::verify_read(
                &fixture.issuer.verifier(&mut None)?, &scope, &fixture.file,
            )?;
            let allowed = fixture.host.read_content(&fixture.file, &grant, 4096, None).await.is_ok();
            assert_eq!(allowed, !deny && grants_path && !expired);
            let grant = ContentGrant::verify_directory_read(
                &fixture.issuer.verifier(&mut None)?, &scope, &fixture.volume, "secrets", &mut None,
            )?;
            let allowed = fixture.host.read_private_prefix(
                &fixture.volume, &grant, fixture.file.path(), &fixture.generation()?, 3, acyclic_harness::conversation::ContentAccess::default(),
            ).await.is_ok();
            assert_eq!(allowed, !deny && grants_path && !expired);
            let scope = fixture.narrowed_for(&[Action::List, Action::Read, Action::ReadHistory], grants_path, expired)?;
            let grant = ContentGrant::verify_directory_read(
                &fixture.issuer.verifier(&mut None)?, &scope, &fixture.volume, "secrets", &mut None,
            )?;
            let allowed = fixture.host.read_private_path(
                &fixture.volume, &grant, fixture.file.path(), Some(&fixture.generation()?), 4096, acyclic_harness::conversation::ContentAccess::default(),
            ).await.is_ok();
            assert_eq!(allowed, !deny && grants_path && !expired);
            Ok::<_, Box<dyn Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[cfg(feature = "test-support")]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn bounded_access_records_roundtrip(
        path in "[a-zΣß?*]{1,12}",
        expiry in prop::option::of(any::<u64>()),
        name in "[a-zΣß]{1,12}",
        malformed in prop::collection::vec(any::<u8>(), 0..4096),
        generation_key in any::<[u8; 32]>(),
    ) {
        for legacy in [
            include_bytes!("../../harness-core/src/filesystem/access/legacy-read.json").as_slice(),
            include_bytes!("../../harness-core/src/filesystem/access/legacy-write.json").as_slice(),
            include_bytes!("../../harness-core/src/filesystem/access/legacy-native.json").as_slice(),
        ] {
            acyclic_harness::filesystem::check_access_record(legacy)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
        }
        let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
        policy.layers.first_mut().and_then(|layer| layer.first_mut()).ok_or(acyclic_fs::access::InvalidPolicy("missing fixture rule"))?.resource.path = path;
        policy.layers.first_mut().and_then(|layer| layer.first_mut()).ok_or(acyclic_fs::access::InvalidPolicy("missing fixture rule"))?.conditions.expires_at = expiry;
        acyclic_harness::filesystem::check_access_record(&serde_json::to_vec(&policy)?)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let mut labels: Vec<String> = serde_json::from_slice(include_bytes!("../../harness-fuzz/corpus/filesystem_access/ceiling-set"))?;
        labels.push(name.clone());
        acyclic_harness::filesystem::check_access_record(&serde_json::to_vec(&labels)?)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let mut file: serde_json::Value = serde_json::from_slice(include_bytes!("../../harness-fuzz/corpus/filesystem_access/file-ref"))?;
        *file.get_mut("path").ok_or_else(|| TestCaseError::fail("missing fixture path"))? = format!("notes/{name}").into();
        let file: FileRef = serde_json::from_value(file)?;
        acyclic_harness::filesystem::check_access_record(&serde_json::to_vec(&file)?)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let generation = GenerationRef::new(file.volume().provider(), &generation_key, None, None)?.0;
        acyclic_harness::filesystem::check_access_record(&serde_json::to_vec(&generation)?)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let receipt = (file, None::<GenerationRef>);
        acyclic_harness::filesystem::check_access_record(&serde_json::to_vec(&receipt)?)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        acyclic_harness::filesystem::check_access_record(&malformed)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[path = "filesystem_access_listing.rs"]
mod listing;

#[path = "filesystem_access_runtime.rs"]
mod runtime;

#[path = "filesystem_access_writes.rs"]
mod writes;

#[path = "filesystem_access_publication.rs"]
mod publication;

#[path = "filesystem_access_write_ceilings.rs"]
mod write_ceilings;

#[tokio::test]
async fn unsupported_authenticated_facts_never_remove_a_principal_dependent_deny()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let reader = fixture.reader(&fixture.scope)?;
    let context =
        context::content_context(reader, None, fixture.scope.capabilities().clone()).await?;
    for selector in 0..6 {
        let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
        policy.version = 2 + selector;
        let mut deny = policy
            .layers
            .first_mut()
            .and_then(|layer| layer.first_mut())
            .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture rule"))?
            .clone();
        deny.id = "principal-dependent-deny".into();
        deny.effect = Effect::Deny;
        match selector {
            0 => deny.principal.group = Some("restricted".into()),
            1 => {
                deny.principal
                    .attributes
                    .insert("restricted".into(), "true".into());
            }
            2 => deny.conditions.role = Some("restricted".into()),
            3 => deny.conditions.tag = Some("restricted".into()),
            4 => deny.conditions.relation = Some(Relation::SameTaskTree),
            _ => deny.resource.lineage_root = Some("restricted".into()),
        }
        policy
            .layers
            .first_mut()
            .ok_or(acyclic_fs::access::InvalidPolicy("missing fixture layer"))?
            .push(deny);
        fixture.publish_policy(policy).await?;
        assert!(matches!(
            context.read_file(&fixture.file, None).await,
            Err(acyclic_harness::Error::FilesystemAccessConfiguration(
                "unauthenticated principal facts"
            ))
        ));
    }
    Ok(())
}

fn revoked_policy(id: &str, action: Action) -> Result<Policy, Box<dyn Error>> {
    let mut policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
    policy.version += 1;
    let layer = policy
        .layers
        .first_mut()
        .ok_or(InvalidPolicy("missing fixture layer"))?;
    let mut deny = layer
        .first()
        .ok_or(InvalidPolicy("missing fixture rule"))?
        .clone();
    deny.id = id.into();
    deny.effect = Effect::Deny;
    deny.actions = [action].into_iter().collect();
    layer.push(deny);
    Ok(policy)
}

#[tokio::test]
async fn empty_pinned_reads_keep_zero_byte_bounds_and_current_authority()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let writer = ContentGrant::verify(
        &verifier,
        &fixture.scope,
        &fixture.volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?;
    let empty = fixture
        .host
        .put_content(
            &fixture.volume,
            &writer,
            "secrets/empty",
            b"",
            "text/plain",
            "empty",
            4096,
            &IdempotencyKey::new("empty")?,
            None,
            Default::default(),
        )
        .await?;
    let grant = ContentGrant::verify_read(&verifier, &fixture.scope, &empty)?;
    let reader = fixture.reader(&fixture.scope)?;
    assert!(
        fixture
            .host
            .read_content(&empty, &grant, 0, None)
            .await?
            .is_empty()
    );
    reader.verify(&empty, 0, None)?.await?;
    assert!(
        fixture
            .host
            .read_content(&fixture.file, &grant, 0, None)
            .await
            .is_err()
    );
    assert!(reader.verify(&fixture.file, 0, None)?.await.is_err());
    for (path, expired) in [(false, false), (true, true)] {
        let scoped = fixture.narrowed(path, expired)?;
        assert!(
            fixture
                .reader(&scoped)?
                .verify(&empty, 0, None)?
                .await
                .is_err()
        );
    }
    fixture.policy(true, 2).await?;
    assert!(
        fixture
            .host
            .read_content(&empty, &grant, 0, None)
            .await
            .is_err()
    );
    assert!(reader.verify(&empty, 0, None)?.await.is_err());
    Ok(())
}
