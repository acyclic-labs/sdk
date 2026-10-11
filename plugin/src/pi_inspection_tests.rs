use super::*;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey,
    conversation::{ContentGrant, ContentResidencyVerifier, VolumeClass, VolumeOperation, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentVerifier, FilesystemHost, workspace_ref},
    resources::ProviderRef,
};
use std::sync::Arc;

#[test]
fn selected_cursor_rejects_every_changed_selector() -> Result<()> {
    let provider = ProviderRef::new("selected", "filesystem", "2")?;
    let volume = VolumeRef::new(provider.clone(), "source", VolumeClass::Project, VolumeOwner::Project("original".into()))?;
    let generation = GenerationRef::new(provider.clone(), [2; 32], None)?;
    let source = TaskId::from_bytes([3; 16]);
    let cursor = PiInspectionCursor { source, volume: volume.clone(), generation: generation.clone(), directory: "dir".into(), after: "last".into() };
    cursor.require(source, &volume, &generation, "dir")?;
    assert!(cursor.require(TaskId::from_bytes([4; 16]), &volume, &generation, "dir").is_err());
    let foreign = VolumeRef::new(provider.clone(), "foreign", VolumeClass::Project, VolumeOwner::Project("original".into()))?;
    assert!(cursor.require(source, &foreign, &generation, "dir").is_err());
    assert!(cursor.require(source, &volume, &GenerationRef::new(provider, [5; 32], None)?, "dir").is_err());
    assert!(cursor.require(source, &volume, &generation, "other").is_err());
    for invalid in ["", "a/b", "a\\b", ".."] {
        let mut forged = cursor.clone();
        forged.after = invalid.into();
        assert!(forged.require(source, &volume, &generation, "dir").is_err());
    }
    assert!(validate_page("", 0).is_err());
    assert!(validate_page("", MAX_PRIVATE_DIRECTORY_PAGE as u32 + 1).is_err());
    assert!(validate_page("../outside", 1).is_err());
    Ok(())
}

/// Actual SDK authority verifier, canonical Filesystem pages/import plan and CAS.
/// No native-path existence is interpreted as authorization or import success.
#[tokio::test]
async fn retained_pages_and_destination_import_preserve_source_and_concurrent_target() -> Result<()> {
    let provider = ProviderRef::new("pi-selected", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(acyclic_fs::Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([11; 16]);
    let other = AgentId::from_bytes([12; 16]);
    let source = VolumeRef::new(provider.clone(), "source", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
    let target = VolumeRef::new(provider.clone(), "target", VolumeClass::AgentPrivate, VolumeOwner::Agent(other))?;
    let issuer = AuthorityIssuer::new("selected-owner", [13; 32], Authority { kind: AggregateKind::Conversation, id: "original-owner".into() });
    let source_scope = issuer.root_for_agent(agent, "source-owner", Capabilities::new([
        source.capability(VolumeOperation::Read)?, source.capability(VolumeOperation::Write)?, source.directory_read_capability("")?,
    ]));
    let target_scope = issuer.root_for_agent(other, "destination-owner", Capabilities::new([target.capability(VolumeOperation::Write)?]));
    let source_writer = ContentGrant::verify(&issuer.verifier(), &source_scope, &source, VolumeOperation::Write)?;
    let source_reader = ContentGrant::verify(&issuer.verifier(), &source_scope, &source, VolumeOperation::Read)?;
    let target_writer = ContentGrant::verify(&issuer.verifier(), &target_scope, &target, VolumeOperation::Write)?;
    host.create_volume(&source).await?;
    host.create_volume(&target).await?;
    for name in ["a.txt", "b.txt"] {
        host.put_content(&source, &source_writer, name, name.as_bytes(), "text/plain", name, 1024, &IdempotencyKey::new(format!("seed-{name}"))?).await?;
    }
    let source_workspace = workspace_ref(provider.clone(), &source.storage_name()?)?;
    let selected = host.resolve(&source_workspace).await?.generation;
    let reader = FilesystemContentVerifier::new(Arc::clone(&host), issuer.verifier(), source_scope.clone(), 1024)?;
    let first = ContentResidencyVerifier::list_private_directory(&reader, &source, "", "", Some(&selected), None, 1).await?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    host.put_content(&source, &source_writer, "late.txt", b"late", "text/plain", "late.txt", 1024, &IdempotencyKey::new("source-late")?).await?;
    let source_current = host.resolve(&source_workspace).await?.generation;
    let second = ContentResidencyVerifier::list_private_directory(&reader, &source, "", "", Some(&selected), Some(&first.entries[0].name), 1).await?;
    assert_eq!(second.entries[0].name, "b.txt");
    assert!(!second.has_more);
    let foreign_generation = host.resolve(&workspace_ref(provider.clone(), &target.storage_name()?)?).await?.generation;
    assert!(ContentResidencyVerifier::list_private_directory(&reader, &source, "", "", Some(&foreign_generation), None, 1).await.is_err());
    let rejected = issuer.root_for_agent(agent, "wrong-writer", Capabilities::new([target.capability(VolumeOperation::Write)?]));
    assert!(ContentGrant::verify(&issuer.verifier(), &rejected, &target, VolumeOperation::Write).is_err());
    let plan = host.prepare_volume_import(&source, &source_reader, &selected, &target, &target_writer).await?;
    host.put_content(&target, &target_writer, "user.txt", b"concurrent user edit", "text/plain", "user.txt", 1024, &IdempotencyKey::new("concurrent-user-edit")?).await?;
    let current_target = host.resolve(&workspace_ref(provider.clone(), &target.storage_name()?)?).await?.generation;
    let outcome = plan.apply(&IdempotencyKey::new("original-import")?).await?;
    assert!(!matches!(outcome, acyclic_fs::JoinOutcome::Applied(_) | acyclic_fs::JoinOutcome::AlreadyApplied(_)));
    assert_eq!(host.resolve(&workspace_ref(provider, &target.storage_name()?)?).await?.generation, current_target);
    assert_eq!(host.resolve(&source_workspace).await?.generation, source_current);
    let denied_scope = issuer.root_for_agent(other, "no-source-read", Capabilities::new(std::iter::empty::<String>()));
    let denied_reader = FilesystemContentVerifier::new(Arc::clone(&host), issuer.verifier(), denied_scope, 1024)?;
    assert!(ContentResidencyVerifier::list_private_directory(&denied_reader, &source, "", "", Some(&selected), None, 1).await.is_err());
    let fresh_plan = host.prepare_volume_import(&source, &source_reader, &selected, &target, &target_writer).await?;
    assert!(matches!(fresh_plan.apply(&IdempotencyKey::new("fresh-original-import")?).await?,
        acyclic_fs::JoinOutcome::Applied(_) | acyclic_fs::JoinOutcome::AlreadyApplied(_)));
    assert_eq!(host.resolve(&source_workspace).await?.generation, source_current);
    Ok(())
}

#[test]
fn selected_writeback_plan_cannot_smuggle_unselected_paths() -> Result<()> {
    use acyclic_fs::MaterializationEdit;
    let paths = vec!["selected".to_owned()];
    require_selected_edits(&[MaterializationEdit::Remove { path: "selected/file".into() }], &paths)?;
    assert!(require_selected_edits(&[MaterializationEdit::Remove { path: "selected-other/file".into() }], &paths).is_err());
    assert!(require_selected_edits(&[MaterializationEdit::Rename { from: "selected/file".into(), to: "foreign/file".into() }], &paths).is_err());
    Ok(())
}

#[tokio::test]
async fn native_preparation_never_writes_root_and_concurrent_user_edit_is_preserved() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("original");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("selected.txt"), b"original")?;
    std::fs::write(root.join("outside.txt"), b"outside")?;
    let identity = acyclic_fs::capture_root_identity(&root)?;
    let filesystem = acyclic_fs::Fs::memory();
    let workspace = filesystem.attach_directory("native-selected", &root, acyclic_fs::SourceOptions::default()).await?;
    let from = workspace.head().await?;
    workspace.write_text("/selected.txt", "child-selected").await?;
    let candidate = workspace.head().await?;
    let state = acyclic_fs::LocalCoreStateStore::open_owned(directory.path().join("state"))?;
    let operation_directory = directory.path().join("operation");
    let operation = acyclic_fs::OperationId::from_bytes([72; 16]);
    let options = acyclic_fs::MaterializeOptions::native(&operation_directory);
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = crate::native_root_source::budget();
    let publication = || acyclic_fs::NativeWorkspacePublication {
        root: &root, source_root_identity: identity, operation_directory: &operation_directory,
        operation_id: operation, from: from.id(), to: candidate.id(), excluded_names: &[],
        options: &options, budget, cancellation: &cancellation,
    };
    let prepared = acyclic_fs::prepare_native_generation_transition(&from, &candidate, &state, publication()).await?;
    assert_eq!(prepared.value.phase, acyclic_fs::MaterializationPhase::Prepared);
    assert_eq!(prepared.value.root_identity, Some(identity.to_bytes()));
    require_selected_edits(&prepared.value.plan.edits, &["selected.txt".into()])?;
    assert_eq!(std::fs::read(root.join("selected.txt"))?, b"original");
    std::fs::write(root.join("selected.txt"), b"concurrent-user")?;
    std::fs::write(root.join("outside.txt"), b"outside-user")?;
    assert!(acyclic_fs::publish_native_generation_transition(&from, &candidate, &state, publication()).await.is_err());
    assert_eq!(std::fs::read(root.join("selected.txt"))?, b"concurrent-user");
    assert_eq!(std::fs::read(root.join("outside.txt"))?, b"outside-user");
    assert_eq!(from.read("/selected.txt", 1024).await?.as_ref(), b"original");
    assert_eq!(candidate.read("/selected.txt", 1024).await?.as_ref(), b"child-selected");
    Ok(())
}

#[tokio::test]
async fn native_frozen_reader_pins_epoch_cursor_and_never_promotes_source() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::{demand::native::NativeDemandSource, model::{FilesystemProfile, VolumeLimits}};
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("a.txt"), b"a")?;
    std::fs::write(directory.path().join("b.txt"), b"b")?;
    let source = Arc::new(NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default()).await?);
    let filesystem = acyclic_fs::Fs::memory();
    let workspace = acyclic_fs::LazyWorkspace::attach(&filesystem, "actual-native-reader", Arc::clone(&source), acyclic_fs::MemoryLazyWorkspaceStore::default()).await?;
    let frozen = workspace.snapshot().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = crate::native_root_source::budget();
    let first = workspace.list_directory_at_snapshot_measured(frozen, "/", None, 1, budget, &cancellation).await?;
    assert_eq!(first.value.entries.len(), 1);
    let cursor = first.value.next.expect("real native source continuation");
    let encoded = cursor.encode_measured(budget, &cancellation)?;
    let decoded = acyclic_fs::LazyDirectoryCursor::decode_measured(&encoded.value, workspace.workspace().profile(), workspace.workspace().limits(), budget, &cancellation)?;
    let second = workspace.list_directory_at_snapshot_measured(frozen, "/", Some(decoded.value), 1, budget, &cancellation).await?;
    assert_eq!(second.value.entries.len(), 1);
    let bytes = workspace.read_at_snapshot_measured(frozen, "/a.txt", 1, budget, &cancellation).await?;
    assert_eq!(bytes.value.as_ref(), b"a");
    assert_eq!(workspace.snapshot().await?, frozen);
    assert!(workspace.list_directory_at_snapshot_measured(frozen, "/other", Some(cursor), 1, budget, &cancellation).await.is_err());
    let mut forged = frozen;
    forged.source.epoch = forged.source.epoch.checked_add(1).expect("finite epoch");
    assert!(workspace.read_at_snapshot_measured(forged, "/a.txt", 1, budget, &cancellation).await.is_err());
    workspace.workspace().write_text("/authored.txt", "immutable-authored").await?;
    let authored_frozen = workspace.snapshot().await?;
    source.invalidate();
    assert!(workspace.read_at_snapshot_measured(frozen, "/a.txt", 1, budget, &cancellation).await.is_err());
    assert_eq!(workspace.read_at_snapshot_measured(authored_frozen, "/authored.txt", 1024,
        budget, &cancellation).await?.value.as_ref(), b"immutable-authored");
    assert_eq!(std::fs::read(directory.path().join("a.txt"))?, b"a");
    assert_eq!(std::fs::read(directory.path().join("b.txt"))?, b"b");
    Ok(())
}

#[tokio::test]
async fn selected_frozen_native_baseline_is_not_an_empty_root_and_source_stays_immutable() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::{demand::native::NativeDemandSource, model::{FilesystemProfile, VolumeLimits}};
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("selected.txt"), b"original-selected")?;
    std::fs::write(directory.path().join("outside.txt"), b"original-outside")?;
    let source = Arc::new(NativeDemandSource::open(directory.path(), FilesystemProfile::Portable, VolumeLimits::default()).await?);
    let filesystem = acyclic_fs::Fs::memory();
    let provider = ProviderRef::new("selected-baseline", "filesystem", "2")?;
    let agent = AgentId::from_bytes([83; 16]);
    let volume = VolumeRef::new(provider, "original", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
    let issuer = AuthorityIssuer::new("baseline-owner", [84; 32], Authority { kind: AggregateKind::Conversation, id: "original-baseline-owner".into() });
    let scope = issuer.root_for_agent(agent, "original-baseline-writer", Capabilities::new([volume.capability(VolumeOperation::Write)?]));
    let verifier = issuer.verifier();
    let workspace = acyclic_fs::LazyWorkspace::attach(&filesystem, volume.storage_name()?, Arc::clone(&source), acyclic_fs::MemoryLazyWorkspaceStore::default()).await?;
    let frozen = workspace.snapshot().await?;
    workspace.workspace().write_text("/selected.txt", "imported-selected").await?;
    workspace.workspace().write_text("/unselected-authored.txt", "must-not-write-back").await?;
    let imported = workspace.workspace().head().await?;
    let authored_before = imported.id();
    let paths = vec!["/selected.txt".to_owned()];
    let cancellation = acyclic_fs::CancellationToken::new();
    let result = workspace.exactify_snapshot_selected_at_generation_measured(frozen, &imported, &paths,
        "original-operation-selected-baseline", acyclic_fs::IdempotencyKey::from_bytes([85; 16]),
        acyclic_fs::IdempotencyKey::from_bytes([86; 16]), acyclic_fs::PublicationPermit::Unrestricted,
        crate::native_root_source::budget(), &cancellation, |budget| {
            let verification = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write);
            async move {
                let work = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                work.verify_within(&budget).map_err(|error| acyclic_fs::OperationFailure::new(acyclic_fs::FsError::Work(error), acyclic_fs::WorkCounters::default()))?;
                verification.map_err(|_| acyclic_fs::OperationFailure::new(acyclic_fs::FsError::MutationNotAllowed, work))?;
                Ok(acyclic_fs::OperationReceipt { value: (), work })
            }
        }).await?;
    let (baseline, candidate) = result.value;
    assert_ne!(baseline.workspace_id(), workspace.workspace().id());
    assert_eq!(candidate.workspace_id(), baseline.workspace_id());
    assert_eq!(baseline.read("/selected.txt", 1024).await?.as_ref(), b"original-selected");
    assert_eq!(candidate.read("/selected.txt", 1024).await?.as_ref(), b"imported-selected");
    assert!(candidate.read("/unselected-authored.txt", 1024).await.is_err());
    let denied_scope = issuer.root_for_agent(AgentId::from_bytes([88; 16]), "foreign-baseline-writer",
        Capabilities::new([volume.capability(VolumeOperation::Write)?]));
    let denied = workspace.exactify_snapshot_selected_at_generation_measured(frozen, &imported, &paths,
        "rejected-original-operation", acyclic_fs::IdempotencyKey::from_bytes([89; 16]),
        acyclic_fs::IdempotencyKey::from_bytes([90; 16]), acyclic_fs::PublicationPermit::Unrestricted,
        crate::native_root_source::budget(), &cancellation, |_| {
            let verification = ContentGrant::verify(&verifier, &denied_scope, &volume, VolumeOperation::Write);
            async move {
                let work = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                verification.map_err(|_| acyclic_fs::OperationFailure::new(acyclic_fs::FsError::MutationNotAllowed, work))?;
                Ok(acyclic_fs::OperationReceipt { value: (), work })
            }
        }).await;
    assert!(denied.is_err());
    assert_eq!(workspace.workspace().head().await?.id(), authored_before);
    assert_eq!(std::fs::read(directory.path().join("selected.txt"))?, b"original-selected");
    assert_eq!(std::fs::read(directory.path().join("outside.txt"))?, b"original-outside");
    assert_eq!(workspace.read_at_snapshot_measured(frozen, "/selected.txt", 1024,
        crate::native_root_source::budget(), &cancellation).await?.value.as_ref(), b"original-selected");
    Ok(())
}

#[tokio::test]
async fn native_writeback_rejects_replaced_original_root_identity() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("original");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("selected.txt"), b"original")?;
    let identity = acyclic_fs::capture_root_identity(&root)?;
    let filesystem = acyclic_fs::Fs::memory();
    let workspace = filesystem.attach_directory("original-identity", &root, acyclic_fs::SourceOptions::default()).await?;
    let from = workspace.head().await?;
    workspace.write_text("/selected.txt", "candidate").await?;
    let candidate = workspace.head().await?;
    let state = acyclic_fs::LocalCoreStateStore::open_owned(directory.path().join("state"))?;
    let operation_directory = directory.path().join("operation");
    let options = acyclic_fs::MaterializeOptions::native(&operation_directory);
    let cancellation = acyclic_fs::CancellationToken::new();
    let publication = || acyclic_fs::NativeWorkspacePublication {
        root: &root, source_root_identity: identity, operation_directory: &operation_directory,
        operation_id: acyclic_fs::OperationId::from_bytes([87; 16]), from: from.id(), to: candidate.id(),
        excluded_names: &[], options: &options, budget: crate::native_root_source::budget(), cancellation: &cancellation,
    };
    acyclic_fs::prepare_native_generation_transition(&from, &candidate, &state, publication()).await?;
    let retained_original = directory.path().join("retained-original");
    std::fs::rename(&root, &retained_original)?;
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("selected.txt"), b"replacement-owner")?;
    assert_ne!(acyclic_fs::capture_root_identity(&root)?, identity);
    assert!(acyclic_fs::publish_native_generation_transition(&from, &candidate, &state, publication()).await.is_err());
    assert_eq!(std::fs::read(root.join("selected.txt"))?, b"replacement-owner");
    assert_eq!(std::fs::read(retained_original.join("selected.txt"))?, b"original");
    Ok(())
}

#[tokio::test]
async fn canonical_selected_import_preserves_metadata_source_and_destination_conflicts() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::{demand::native::NativeDemandSource, model::{FilesystemProfile, VolumeLimits}};
    let source_directory = tempfile::tempdir()?;
    let destination_directory = tempfile::tempdir()?;
    let filesystem = acyclic_fs::Fs::memory();
    let source_native = Arc::new(NativeDemandSource::open(source_directory.path(),
        FilesystemProfile::Portable, VolumeLimits::default()).await?);
    let destination_native = Arc::new(NativeDemandSource::open(destination_directory.path(),
        FilesystemProfile::Portable, VolumeLimits::default()).await?);
    let source_owner = AgentId::from_bytes([101; 16]);
    let destination_owner = AgentId::from_bytes([102; 16]);
    let provider = ProviderRef::new("canonical-selected-import", "filesystem", "2")?;
    let source_volume = VolumeRef::new(provider.clone(), "source", VolumeClass::AgentPrivate, VolumeOwner::Agent(source_owner))?;
    let destination_volume = VolumeRef::new(provider, "destination", VolumeClass::AgentPrivate, VolumeOwner::Agent(destination_owner))?;
    let issuer = AuthorityIssuer::new("actual-selected-import-owner", [103; 32],
        Authority { kind: AggregateKind::Conversation, id: "selected-import-fixture".into() });
    let read_scope = issuer.root_for_agent(source_owner, "source-read-only",
        Capabilities::new([source_volume.capability(VolumeOperation::Read)?]));
    let write_scope = issuer.root_for_agent(destination_owner, "actual-destination-write",
        Capabilities::new([destination_volume.capability(VolumeOperation::Write)?]));
    let foreign_scope = issuer.root_for_agent(source_owner, "foreign-destination-writer",
        Capabilities::new([destination_volume.capability(VolumeOperation::Write)?]));
    let verifier = issuer.verifier();
    let source = acyclic_fs::LazyWorkspace::attach(&filesystem, source_volume.storage_name()?,
        Arc::clone(&source_native), acyclic_fs::MemoryLazyWorkspaceStore::default()).await?;
    let destination = acyclic_fs::LazyWorkspace::attach(&filesystem, destination_volume.storage_name()?,
        destination_native, acyclic_fs::MemoryLazyWorkspaceStore::default()).await?;
    source.workspace().write_text("/selected.txt", "source-original").await?;
    let mut metadata = source.workspace().begin_transaction().await?;
    metadata.set_metadata("/selected.txt", acyclic_fs::kernel::FileMetadata {
        modified_ns: acyclic_fs::kernel::MetadataField::Value(123_456),
        ..Default::default()
    }).await?;
    metadata.commit().await?;
    destination.workspace().write_text("/selected.txt", "destination-original").await?;
    destination.workspace().write_text("/outside.txt", "outside-original").await?;
    let frozen_source = source.snapshot().await?;
    let frozen_destination = destination.snapshot().await?;
    let portable = acyclic_fs::path::PortablePath::parse("/selected.txt", source.workspace().limits())?;
    let paths = [acyclic_fs::kernel::NamespacePath::from_portable_in_profile(&portable,
        source.workspace().profile(), source.workspace().limits())?];
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = crate::native_root_source::budget();
    ContentGrant::verify(&verifier, &read_scope, &source_volume, VolumeOperation::Read)?;
    assert!(ContentGrant::verify(&verifier, &read_scope, &source_volume, VolumeOperation::Write).is_err());
    let plan = source.prepare_selected_import_measured(frozen_source, &paths, budget, &cancellation).await?.value;
    let rejected = destination.import_selected_measured_guarded(frozen_destination, plan,
        acyclic_fs::OperationId::from_bytes([104; 16]), acyclic_fs::PublicationPermit::Unrestricted,
        budget, &cancellation, |_| {
            let verification = ContentGrant::verify(&verifier, &foreign_scope, &destination_volume, VolumeOperation::Write);
            async move {
                verification.map_err(|_| acyclic_fs::OperationFailure::before_work(acyclic_fs::FsError::MutationNotAllowed))?;
                Ok(acyclic_fs::OperationReceipt { value: (), work: Default::default() })
            }
        }).await;
    assert!(rejected.is_err());
    assert_eq!(destination.workspace().read("/selected.txt", 1024).await?.as_ref(), b"destination-original");
    destination.workspace().write_text("/selected.txt", "concurrent-selected").await?;
    destination.workspace().write_text("/outside.txt", "concurrent-outside").await?;
    let plan = source.prepare_selected_import_measured(frozen_source, &paths, budget, &cancellation).await?.value;
    let stale = destination.import_selected_measured_guarded(frozen_destination, plan,
        acyclic_fs::OperationId::from_bytes([105; 16]), acyclic_fs::PublicationPermit::Unrestricted,
        budget, &cancellation, |_| {
            let verification = ContentGrant::verify(&verifier, &write_scope, &destination_volume, VolumeOperation::Write);
            async move {
                verification.map_err(|_| acyclic_fs::OperationFailure::before_work(acyclic_fs::FsError::MutationNotAllowed))?;
                Ok(acyclic_fs::OperationReceipt { value: (), work: Default::default() })
            }
        }).await;
    assert!(stale.is_err());
    assert_eq!(destination.workspace().read("/selected.txt", 1024).await?.as_ref(), b"concurrent-selected");
    assert_eq!(destination.workspace().read("/outside.txt", 1024).await?.as_ref(), b"concurrent-outside");
    let current_destination = destination.snapshot().await?;
    let plan = source.prepare_selected_import_measured(frozen_source, &paths, budget, &cancellation).await?.value;
    let imported = destination.import_selected_measured_guarded(current_destination, plan,
        acyclic_fs::OperationId::from_bytes([106; 16]), acyclic_fs::PublicationPermit::Unrestricted,
        budget, &cancellation, |_| {
            let verification = ContentGrant::verify(&verifier, &write_scope, &destination_volume, VolumeOperation::Write);
            async move {
                verification.map_err(|_| acyclic_fs::OperationFailure::before_work(acyclic_fs::FsError::MutationNotAllowed))?;
                Ok(acyclic_fs::OperationReceipt { value: (), work: Default::default() })
            }
        }).await?.value;
    let exact = destination.workspace().generation(imported.authored_generation).await?;
    assert_eq!(exact.read("/selected.txt", 1024).await?.as_ref(), b"source-original");
    assert_eq!(exact.stat("/selected.txt").await?.metadata.modified_ns, Some(123_456));
    assert_eq!(exact.read("/outside.txt", 1024).await?.as_ref(), b"concurrent-outside");
    assert_eq!(source.snapshot().await?, frozen_source);
    assert_eq!(source.workspace().read("/selected.txt", 1024).await?.as_ref(), b"source-original");
    assert!(!source_directory.path().join("selected.txt").exists());
    assert!(!destination_directory.path().join("selected.txt").exists());
    Ok(())
}

#[tokio::test]
async fn selected_import_does_not_infer_deletion_from_a_stale_native_source() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::{demand::native::NativeDemandSource, model::{FilesystemProfile, VolumeLimits}};
    let directory = tempfile::tempdir()?;
    let native = Arc::new(NativeDemandSource::open(directory.path(), FilesystemProfile::Portable,
        VolumeLimits::default()).await?);
    let filesystem = acyclic_fs::Fs::memory();
    let source = acyclic_fs::LazyWorkspace::attach(&filesystem, "selected-negative-proof",
        Arc::clone(&native), acyclic_fs::MemoryLazyWorkspaceStore::default()).await?;
    let frozen = source.snapshot().await?;
    let portable = acyclic_fs::path::PortablePath::parse("/absent.txt", source.workspace().limits())?;
    let paths = [acyclic_fs::kernel::NamespacePath::from_portable_in_profile(&portable,
        source.workspace().profile(), source.workspace().limits())?];
    native.invalidate();
    let cancellation = acyclic_fs::CancellationToken::new();
    assert!(source.prepare_selected_import_measured(frozen, &paths,
        crate::native_root_source::budget(), &cancellation).await.is_err());
    assert!(!directory.path().join("absent.txt").exists());
    Ok(())
}

#[tokio::test]
async fn canonical_selected_directory_import_pins_nested_generation_and_metadata() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::{demand::native::NativeDemandSource, model::{FilesystemProfile, VolumeLimits}};
    let source_directory = tempfile::tempdir()?;
    let destination_directory = tempfile::tempdir()?;
    let filesystem = acyclic_fs::Fs::memory();
    let source_owner = AgentId::from_bytes([111; 16]);
    let destination_owner = AgentId::from_bytes([112; 16]);
    let provider = ProviderRef::new("selected-directory-import", "filesystem", "2")?;
    let source_volume = VolumeRef::new(provider.clone(), "source", VolumeClass::AgentPrivate, VolumeOwner::Agent(source_owner))?;
    let destination_volume = VolumeRef::new(provider, "destination", VolumeClass::AgentPrivate, VolumeOwner::Agent(destination_owner))?;
    let issuer = AuthorityIssuer::new("selected-directory-owner", [113; 32],
        Authority { kind: AggregateKind::Conversation, id: "selected-directory-fixture".into() });
    let read_scope = issuer.root_for_agent(source_owner, "source-read",
        Capabilities::new([source_volume.capability(VolumeOperation::Read)?]));
    let write_scope = issuer.root_for_agent(destination_owner, "destination-write",
        Capabilities::new([destination_volume.capability(VolumeOperation::Write)?]));
    let verifier = issuer.verifier();
    let source = acyclic_fs::LazyWorkspace::attach(&filesystem, source_volume.storage_name()?,
        Arc::new(NativeDemandSource::open(source_directory.path(), FilesystemProfile::Portable, VolumeLimits::default()).await?),
        acyclic_fs::MemoryLazyWorkspaceStore::default()).await?;
    let destination = acyclic_fs::LazyWorkspace::attach(&filesystem, destination_volume.storage_name()?,
        Arc::new(NativeDemandSource::open(destination_directory.path(), FilesystemProfile::Portable, VolumeLimits::default()).await?),
        acyclic_fs::MemoryLazyWorkspaceStore::default()).await?;
    let mut tree = source.workspace().begin_transaction().await?;
    tree.create_dir_all("/selected/nested").await?;
    tree.write_text("/selected/nested/file.txt", "original-nested").await?;
    tree.set_metadata("/selected", acyclic_fs::kernel::FileMetadata {
        modified_ns: acyclic_fs::kernel::MetadataField::Value(654_321), ..Default::default()
    }).await?;
    tree.set_metadata("/selected/nested/file.txt", acyclic_fs::kernel::FileMetadata {
        modified_ns: acyclic_fs::kernel::MetadataField::Value(123_456), ..Default::default()
    }).await?;
    tree.commit().await?;
    destination.workspace().write_text("/outside.txt", "original-outside").await?;
    let frozen = source.snapshot().await?;
    let portable = acyclic_fs::path::PortablePath::parse("/selected", source.workspace().limits())?;
    let paths = [acyclic_fs::kernel::NamespacePath::from_portable_in_profile(&portable,
        source.workspace().profile(), source.workspace().limits())?];
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = crate::native_root_source::budget();
    ContentGrant::verify(&verifier, &read_scope, &source_volume, VolumeOperation::Read)?;
    assert!(ContentGrant::verify(&verifier, &read_scope, &source_volume, VolumeOperation::Write).is_err());
    let plan = source.prepare_selected_import_measured(frozen, &paths, budget, &cancellation).await?.value;
    source.workspace().write_text("/selected/nested/file.txt", "later-source").await?;
    destination.workspace().write_text("/outside.txt", "concurrent-outside").await?;
    let current_destination = destination.snapshot().await?;
    let imported = destination.import_selected_measured_guarded(current_destination, plan,
        acyclic_fs::OperationId::from_bytes([114; 16]), acyclic_fs::PublicationPermit::Unrestricted,
        budget, &cancellation, |_| {
            let verification = ContentGrant::verify(&verifier, &write_scope, &destination_volume, VolumeOperation::Write);
            async move {
                verification.map_err(|_| acyclic_fs::OperationFailure::before_work(acyclic_fs::FsError::MutationNotAllowed))?;
                Ok(acyclic_fs::OperationReceipt { value: (), work: Default::default() })
            }
        }).await?.value;
    let exact = destination.workspace().generation(imported.authored_generation).await?;
    assert_eq!(exact.read("/selected/nested/file.txt", 1024).await?.as_ref(), b"original-nested");
    assert_eq!(exact.stat("/selected").await?.metadata.modified_ns, Some(654_321));
    assert_eq!(exact.stat("/selected/nested/file.txt").await?.metadata.modified_ns, Some(123_456));
    assert_eq!(exact.read("/outside.txt", 1024).await?.as_ref(), b"concurrent-outside");
    assert_eq!(source.workspace().generation(frozen.authored_generation).await?
        .read("/selected/nested/file.txt", 1024).await?.as_ref(), b"original-nested");
    assert_eq!(source.workspace().read("/selected/nested/file.txt", 1024).await?.as_ref(), b"later-source");
    assert!(!source_directory.path().join("selected").exists());
    assert!(!destination_directory.path().join("selected").exists());
    Ok(())
}
