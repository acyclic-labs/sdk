use super::*;
use crate::{conversation::FileDescriptor, core::AggregateKind};
use std::sync::Arc;

fn seed() -> Result<ForkSeed> {
    serde_json::from_str(include_str!("../../fixtures/v2/fork-seed.json"))
        .map_err(|error| Error::Invalid(error.to_string()))
}

fn file(
    volume: &VolumeRef,
    path: &str,
    version: &str,
    bytes: &[u8],
    media: &str,
) -> Result<FileRef> {
    FileRef::new(
        volume.clone(),
        path.into(),
        version.into(),
        FileDescriptor::from_bytes(bytes, media, &mut None)?,
        path.rsplit('/').next().expect("file name").into(),
        &mut None,
    )
}

struct Files {
    provider: ProviderRef,
    files: Vec<(FileRef, acyclic_stream::OwnedBytes)>,
}

fn files(provider: &ProviderRef, bodies: &[(&FileRef, &[u8])]) -> Arc<dyn ForkSeedVerifier> {
    Arc::new(Files {
        provider: provider.clone(),
        files: bodies
            .iter()
            .map(|(file, bytes)| ((*file).clone(), bytes.to_vec().into()))
            .collect(),
    })
}

impl ForkSeedVerifier for Files {
    fn provider(&self) -> &ProviderRef {
        &self.provider
    }
    fn verify<'a, 'w: 'a>(
        &'a self,
        _: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>,
    ) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |_| async { Ok(()) })
    }
    fn verify_file<'a, 'w: 'a>(
        &'a self,
        _: &'a ForkSeed,
        file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>,
    ) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            if file.volume().provider() != &self.provider {
                return Err(Error::Unauthorized("wrong file provider".into()));
            }
            let bytes = &self
                .files
                .iter()
                .find(|(reference, _)| reference == file)
                .ok_or_else(|| Error::NotFound("pinned file".into()))?
                .1;
            file.descriptor().verify(bytes.as_ref(), work)
        })
    }
    fn read_manifest<'a, 'w: 'a>(
        &'a self,
        _: &'a ForkSeed,
        manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>,
    ) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        admit_fork_future(work, |_| async move {
            let bytes = &self
                .files
                .iter()
                .find(|(file, _)| file == manifest)
                .ok_or_else(|| Error::NotFound("manifest".into()))?
                .1;
            Ok((bytes.clone(), 0))
        })
    }
}

#[test]
fn inherited_prefix_accepts_more_than_a_thousand_readers() -> Result<()> {
    let readers = (0_u64..=1_024)
        .map(|index| {
            let mut bytes = [0_u8; 16];
            bytes[..8].copy_from_slice(&index.to_le_bytes());
            AgentId::from_bytes(bytes)
        })
        .collect::<Vec<_>>();
    let mut conversation = crate::conversation::ConversationState::default();
    conversation.bind(AgentId::from_bytes([255; 16]))?;
    InheritedConversationPrefix::select(
        Authority {
            kind: AggregateKind::Conversation,
            id: "parent".into(),
        },
        0,
        0,
        &readers,
        &conversation,
        &mut None,
    )?;
    Ok(())
}

#[test]
fn child_result_capture_rejects_alias_partial_and_foreign_generation() -> Result<()> {
    let mut seed = seed()?;
    let provider = seed.child_private_volume.provider().clone();
    seed.child_result_volume = Some(VolumeRef::new(
        provider.clone(),
        "independent-child-result".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(seed.child_agent),
        &mut None,
    )?);
    seed.child_result_generation =
        Some(GenerationRef::new(provider.clone(), [73; 32].into(), None)?);
    seed.validate(&mut None)?;
    let mut mutation = seed.clone();
    mutation.child_result_generation = None;
    assert_eq!(
        mutation.validate(&mut None).unwrap_err().code(),
        crate::wire::ErrorCode::Invalid
    );
    mutation = seed.clone();
    mutation.child_result_volume = Some(seed.child_private_volume.clone());
    assert_eq!(
        mutation.validate(&mut None).unwrap_err().code(),
        crate::wire::ErrorCode::Invalid
    );
    mutation = seed;
    mutation.child_result_generation = Some(GenerationRef::new(
        ProviderRef::new(
            provider.namespace().into(),
            provider.family().into(),
            "foreign-result-provider".into(),
            &mut None,
        )?,
        [73; 32].into(),
        None,
    )?);
    assert_eq!(
        mutation.validate(&mut None).unwrap_err().code(),
        crate::wire::ErrorCode::Invalid
    );
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn physical_fork_clone_partial_denial_releases_only_new_fields_and_cancels_before_copy()
-> Result<()> {
    use acyclic_fs::{
        CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError,
    };
    let request: ForkRequest =
        serde_json::from_str(include_str!("../../fixtures/v2/fork-request.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let budget = acyclic_fs::WorkBudget {
        allocation_operations: 100_000,
        peak_allocation_bytes: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        items_examined: 100_000,
        ..acyclic_fs::WorkBudget::default()
    };
    let limited = acyclic_fs::WorkBudget {
        bytes_copied: 4 + request.parent.id.len() as u64,
        ..budget
    };
    let native = NativeMaterializationWork::new(limited, &CancellationToken::new())
        .expect("fork-copy ledger");
    let mut io = crate::store::history::native_work_io(&native).expect("fork-copy observer");
    let held = io.clone_text("held")?;
    assert!(matches!(
        request.copy(&mut Some(&mut io)),
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            ..
        })
    ));
    let guard = io.into_custody();
    assert_eq!(guard.bytes(), held.capacity() as u64);
    drop(held);
    drop(guard);
    native
        .admit_retained(
            limited.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes(),
        )
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    let token = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(budget, &token).expect("original fork ledger");
    let mut io = crate::store::history::native_work_io(&cancelled).expect("cancelled observer");
    let before = cancelled.receipt();
    token.cancel();
    assert!(request.copy(&mut Some(&mut io)).is_err());
    assert_eq!(
        cancelled.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(cancelled.receipt().bytes_copied, before.bytes_copied);
    drop(io);
    Ok(())
}

#[test]
fn project_capture_requires_new_volume_with_independent_owner() -> Result<()> {
    let provider = ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?;
    let project = |id: &str, owner: &str, key| -> Result<ResourceRevision> {
        Ok(ResourceRevision::Project {
            volume: VolumeRef::new(
                provider.clone(),
                id.into(),
                VolumeClass::Project,
                VolumeOwner::Project(owner.into()),
                &mut None,
            )?,
            generation: GenerationRef::new(provider.clone(), [key; 32].into(), None)?,
        })
    };
    let source = project("parent", "project", 1)?;
    CapturedResource {
        source: source.clone(),
        revision: project("child", "project", 2)?,
    }
    .validate(&mut None)?;
    assert!(
        CapturedResource {
            source: source.clone(),
            revision: source.clone()
        }
        .validate(&mut None)
        .is_err()
    );
    CapturedResource {
        source,
        revision: project("foreign", "other", 3)?,
    }
    .validate(&mut None)
}

#[test]
fn fork_history_must_be_exact_parent_prefix() -> Result<()> {
    let mut seed = seed()?;
    seed.validate(&mut None)?;
    let filesystem = seed.child_private_volume.provider().clone();
    let stream = ProviderRef::new("test".into(), "stream".into(), "2".into(), &mut None)?;
    let only_files = CompositeForkVerifier::new(&[files(&filesystem, &[])], &mut None)?;
    assert!(matches!(
        futures::executor::block_on(only_files.verify(&seed, &mut None)),
        Err(Error::Unsupported(_))
    ));
    let complete =
        CompositeForkVerifier::new(&[files(&filesystem, &[]), files(&stream, &[])], &mut None)?;
    futures::executor::block_on(complete.verify(&seed, &mut None))?;
    let ResourceRevision::Project { volume: source, .. } = &seed.resources[1].source else {
        panic!("fixture project")
    };
    let ResourceRevision::Project {
        volume: destination,
        ..
    } = &seed.resources[1].revision
    else {
        panic!("fixture child project")
    };
    let mut missing = seed.clone();
    missing.reference_grants.push(ReferenceGrant {
        file: file(source, "files/missing.txt", "one", b"missing", "text/plain")?,
        reader: seed.child_agent,
        attachment_manifest: None,
    });
    missing.validate(&mut None)?;
    assert!(matches!(
        futures::executor::block_on(complete.verify(&missing, &mut None)),
        Err(Error::NotFound(_))
    ));
    let objects = ProviderRef::new("test".into(), "objects".into(), "2".into(), &mut None)?;
    let volume = VolumeRef::new(
        objects.clone(),
        "attachments".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
        &mut None,
    )?;
    let member = file(source, "files/actual.txt", "one", b"actual", "text/plain")?;
    let forged = file(source, "files/forged.txt", "two", b"forged", "text/plain")?;
    let bytes = serde_json::to_vec(&vec![crate::conversation::Attachment {
        file: member.clone(),
        label: None,
    }])
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let manifest = file(
        &volume,
        "lists/one.json",
        "one",
        &bytes,
        "application/vnd.acyclic.harness.attachments+json",
    )?;
    let mut manifests = seed.clone();
    manifests.attachment_manifests.push(manifest.clone());
    manifests.reference_grants.push(ReferenceGrant {
        file: forged.clone(),
        reader: seed.child_agent,
        attachment_manifest: Some(manifest.clone()),
    });
    assert!(matches!(
        manifests.validate(&mut None),
        Err(Error::Invalid(_))
    ));
    manifests.reference_grants.push(ReferenceGrant {
        file: manifest.clone(),
        reader: seed.child_agent,
        attachment_manifest: None,
    });
    manifests.validate(&mut None)?;
    let attached = AgentId::from_bytes([9; 16]);
    let mut attached_only = manifests.clone();
    attached_only.attached_agents.push(attached);
    attached_only.reference_grants.push(ReferenceGrant {
        file: manifest.clone(),
        reader: attached,
        attachment_manifest: Some(manifest.clone()),
    });
    attached_only.reference_grants[0].file = member.clone();
    attached_only.validate(&mut None)?;
    let filesystem_files = files(&filesystem, &[(&member, b"actual"), (&forged, b"forged")]);
    let verifier = CompositeForkVerifier::new(
        &[
            filesystem_files.clone(),
            files(&stream, &[]),
            files(&objects, &[(&manifest, &bytes)]),
        ],
        &mut None,
    )?;
    futures::executor::block_on(verifier.verify(&attached_only, &mut None))?;
    let missing_manifest = CompositeForkVerifier::new(
        &[filesystem_files, files(&stream, &[]), files(&objects, &[])],
        &mut None,
    )?;
    assert!(matches!(
        futures::executor::block_on(missing_manifest.verify(&manifests, &mut None)),
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        futures::executor::block_on(verifier.verify(&manifests, &mut None)),
        Err(Error::Invalid(_))
    ));
    manifests.reference_grants[0].file = member;
    futures::executor::block_on(verifier.verify(&manifests, &mut None))?;
    let shared = VolumeRef::new(
        filesystem,
        "shared".into(),
        VolumeClass::SessionShared,
        VolumeOwner::Session("session".into()),
        &mut None,
    )?;
    let optional = ResourceRevision::SharedVolume(shared.clone());
    let report = ForkReport {
        request: ForkRequest {
            operation_id: seed.operation_id,
            parent: seed.parent.clone(),
            parent_revision: seed.parent_revision,
            authorized_source: seed.authorized_source.clone(),
            child: seed.child.clone(),
            child_agent: seed.child_agent,
            attached_agents: Vec::new(),
            boundary: None,
            preparation: ForkPreparation {
                child_project_volume: destination.clone(),
                child_private_volume: seed.child_private_volume.clone(),
                child_result_volume: seed.child_result_volume.clone(),
                summary: None,
                inherited_through_sequence: 0,
                maximum_inherited_messages: MAX_FORK_INHERITED_MESSAGES,
                maximum_inherited_bytes: MAX_FORK_INHERITED_BYTES,
                maximum_inherited_references: u32::try_from(MAX_FORK_REFERENCES)
                    .expect("portable count"),
            },
            selections: seed
                .resources
                .iter()
                .map(|capture| ForkSelection {
                    required: true,
                    revision: capture.source.clone(),
                })
                .chain([ForkSelection {
                    required: false,
                    revision: optional.clone(),
                }])
                .collect(),
        },
        captures: seed
            .resources
            .iter()
            .cloned()
            .map(Capture::Captured)
            .chain([Capture::Indeterminate(OperationId::from_bytes([9; 16]))])
            .collect(),
        child_private_volume: seed.child_private_volume.clone(),
        child_private_generation: seed.child_private_generation.clone(),
        child_result_volume: seed.child_result_volume.clone(),
        child_result_generation: seed.child_result_generation.clone(),
        summary: None,
        history_witness: seed.history_witness,
        inherited_context: Vec::new(),
        shared_grants: Vec::new(),
        reference_grants: Vec::new(),
        attachment_manifests: Vec::new(),
        inherited_through_sequence: 0,
    };
    report.validate(&mut None)?;
    let mut duplicate = report.request.clone();
    duplicate.selections.push(duplicate.selections[0].clone());
    assert!(duplicate.validate(&mut None).is_err());
    let mut optional_project = report.request.clone();
    optional_project.selections[1].required = false;
    assert!(optional_project.validate(&mut None).is_err());
    let mut no_history = report.request.clone();
    no_history.selections.remove(0);
    assert!(no_history.validate(&mut None).is_err());
    let mut stale = report.request.clone();
    stale.parent_revision += 1;
    assert!(stale.validate(&mut None).is_err());
    let mut changed = report.clone();
    changed.child_private_volume = VolumeRef::new(
        changed.child_private_volume.provider().clone(),
        "different-child-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(changed.request.child_agent),
        &mut None,
    )?;
    assert!(changed.validate(&mut None).is_err());
    changed = report.clone();
    if let Capture::Captured(capture) = &mut changed.captures[1]
        && let ResourceRevision::Project { volume, .. } = &mut capture.revision
    {
        *volume = VolumeRef::new(
            volume.provider().clone(),
            "different-child-project".into(),
            VolumeClass::Project,
            volume.owner().clone(),
            &mut None,
        )?;
    }
    assert!(changed.validate(&mut None).is_err());
    changed = report.clone();
    changed.inherited_through_sequence = 1;
    assert!(changed.validate(&mut None).is_err());
    let mut widened = report.request.clone();
    widened.preparation.maximum_inherited_messages = 16_385;
    widened.validate(&mut None)?;
    let published = report.into_seed(&mut None)?;
    assert_eq!(published.omissions.len(), 1);
    assert_eq!(published.omissions[0].selection.revision, optional);
    assert!(matches!(
        published.omissions[0].outcome,
        Capture::Indeterminate(_)
    ));
    let mut shared_seed = seed.clone();
    shared_seed.resources.push(CapturedResource {
        source: optional.clone(),
        revision: optional,
    });
    assert!(shared_seed.validate(&mut None).is_err());
    shared_seed.shared_grants.push(SharedGrant {
        volume: shared.clone(),
        child_agent: seed.child_agent,
        operations: BTreeSet::from([VolumeOperation::Read]),
    });
    shared_seed.validate(&mut None)?;
    assert!(
        shared_seed
            .shared_capabilities(shared_seed.child_agent)?
            .contains(&shared.capability(VolumeOperation::Read, &mut None)?)
    );
    shared_seed.shared_grants[0].child_agent = AgentId::from_bytes([3; 16]);
    assert!(shared_seed.validate(&mut None).is_err());
    let inherited = file(
        &seed.child_private_volume,
        ".system/inherited-conversation/prefix.json",
        "one",
        b"one",
        "application/vnd.acyclic.harness.inherited-conversation+json",
    )?;
    seed.inherited_context = vec![inherited.clone(), inherited.clone()];
    seed.inherited_through_sequence = 1;
    assert!(seed.validate(&mut None).is_err());
    seed.inherited_context = vec![inherited.clone()];
    seed.attached_agents.push(attached);
    seed.validate(&mut None)?;
    let access = seed.attached_read_capabilities(attached)?;
    assert!(access.contains(&inherited.read_capability()?));
    assert!(
        !access.contains(
            &seed
                .child_private_volume
                .capability(VolumeOperation::Write, &mut None)?
        )
    );
    assert!(
        seed.reference_capabilities(AgentId::from_bytes([7; 16]))
            .is_err()
    );
    seed.inherited_context.clear();
    seed.parent_revision += 1;
    assert!(seed.validate(&mut None).is_err());
    Ok(())
}
