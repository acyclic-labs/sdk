use super::*;
use crate::{conversation::FileDescriptor, core::AggregateKind};
use std::sync::Arc;

fn seed() -> Result<ForkSeed> {
    serde_json::from_str(include_str!("../../fixtures/v2/fork-seed.json"))
        .map_err(|error| Error::Invalid(error.to_string()))
}

fn file(volume: &VolumeRef, path: &str, bytes: &[u8], media: &str) -> Result<FileRef> {
    FileRef::new(
        volume.clone(),
        path.into(),
        "one".into(),
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
    let result = VolumeRef::new(
        provider.clone(),
        "result".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(seed.child_agent),
        &mut None,
    )?;
    let generation = GenerationRef::new(&provider, &[73; 32], None, None)?.0;
    let foreign = ProviderRef::new(
        provider.namespace().into(),
        provider.family().into(),
        "foreign".into(),
        &mut None,
    )?;
    seed.child_result_volume = Some(result.clone());
    seed.child_result_generation = Some(generation.clone());
    seed.validate(&mut None)?;
    for (volume, generation) in [
        (result.clone(), None),
        (seed.child_private_volume.clone(), Some(generation)),
        (
            result,
            Some(GenerationRef::new(&foreign, &[73; 32], None, None)?.0),
        ),
    ] {
        seed.child_result_volume = Some(volume);
        seed.child_result_generation = generation;
        assert!(seed.validate(&mut None).is_err());
    }
    Ok(())
}

#[test]
fn project_capture_requires_new_volume_with_independent_owner() -> Result<()> {
    let mut capture = seed()?.resources.remove(1);
    let original = capture.revision.clone();
    capture.revision = capture.source.clone();
    assert!(capture.validate(&mut None).is_err());
    capture.revision = original;
    let ResourceRevision::Project { volume, .. } = &mut capture.revision else {
        panic!("project fixture")
    };
    *volume = VolumeRef::new(
        volume.provider().clone(),
        "foreign".into(),
        VolumeClass::Project,
        VolumeOwner::Project("other".into()),
        &mut None,
    )?;
    capture.validate(&mut None)
}

#[test]
fn fork_history_must_be_exact_parent_prefix() -> Result<()> {
    use futures::executor::block_on;
    let mut seed = seed()?;
    let filesystem = seed.child_private_volume.provider().clone();
    let stream = seed.resources[0].source.provider().clone();
    let filesystem_files = files(&filesystem, &[]);
    let only_files = CompositeForkVerifier::new(&[filesystem_files.clone()], &mut None)?;
    assert!(matches!(
        block_on(only_files.verify(&seed, &mut None)),
        Err(Error::Unsupported(_))
    ));
    let complete = CompositeForkVerifier::new(&[filesystem_files, files(&stream, &[])], &mut None)?;
    block_on(complete.verify(&seed, &mut None))?;
    let ResourceRevision::Project { volume: source, .. } = &seed.resources[1].source else {
        panic!("project fixture")
    };
    let member = file(source, "files/actual.txt", b"actual", "text/plain")?;
    let forged = file(source, "files/forged.txt", b"forged", "text/plain")?;
    seed.reference_grants.push(ReferenceGrant {
        file: member.clone(),
        reader: seed.child_agent,
        attachment_manifest: None,
    });
    assert!(matches!(
        block_on(complete.verify(&seed, &mut None)),
        Err(Error::NotFound(_))
    ));
    seed.reference_grants.clear();
    let objects = ProviderRef::new("test".into(), "objects".into(), "2".into(), &mut None)?;
    let volume = VolumeRef::new(
        objects.clone(),
        "attachments".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
        &mut None,
    )?;
    let bytes = serde_json::to_vec(&vec![crate::conversation::Attachment {
        file: member.clone(),
        label: None,
    }])
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let manifest = file(
        &volume,
        "lists/one.json",
        &bytes,
        "application/vnd.acyclic.harness.attachments+json",
    )?;
    seed.attachment_manifests.push(manifest.clone());
    seed.reference_grants.push(ReferenceGrant {
        file: forged.clone(),
        reader: seed.child_agent,
        attachment_manifest: Some(manifest.clone()),
    });
    assert!(seed.validate(&mut None).is_err());
    seed.reference_grants.push(ReferenceGrant {
        file: manifest.clone(),
        reader: seed.child_agent,
        attachment_manifest: None,
    });
    let filesystem_files = files(&filesystem, &[(&member, b"actual"), (&forged, b"forged")]);
    let verifier = CompositeForkVerifier::new(
        &[
            filesystem_files.clone(),
            files(&stream, &[]),
            files(&objects, &[(&manifest, &bytes)]),
        ],
        &mut None,
    )?;
    let missing = CompositeForkVerifier::new(
        &[filesystem_files, files(&stream, &[]), files(&objects, &[])],
        &mut None,
    )?;
    assert!(matches!(
        block_on(missing.verify(&seed, &mut None)),
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        block_on(verifier.verify(&seed, &mut None)),
        Err(Error::Invalid(_))
    ));
    seed.reference_grants[0].file = member;
    let attached = AgentId::from_bytes([9; 16]);
    seed.attached_agents.push(attached);
    seed.reference_grants.push(ReferenceGrant {
        file: manifest.clone(),
        reader: attached,
        attachment_manifest: Some(manifest),
    });
    block_on(verifier.verify(&seed, &mut None))?;
    seed.attachment_manifests.clear();
    seed.reference_grants.clear();
    seed.attached_agents.clear();

    let mut request: ForkRequest =
        serde_json::from_str(include_str!("../../fixtures/v2/fork-request.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let shared = VolumeRef::new(
        filesystem,
        "shared".into(),
        VolumeClass::SessionShared,
        VolumeOwner::Session("session".into()),
        &mut None,
    )?;
    let optional = ResourceRevision::SharedVolume(shared.clone());
    request.selections.push(ForkSelection {
        required: false,
        revision: optional.clone(),
    });
    let report = ForkReport {
        request,
        captures: seed
            .resources
            .iter()
            .cloned()
            .map(Capture::Captured)
            .chain([Capture::Indeterminate(OperationId::from_bytes([9; 16]))])
            .collect(),
        child_private_volume: seed.child_private_volume.clone(),
        child_private_generation: seed.child_private_generation.clone(),
        child_result_volume: None,
        child_result_generation: None,
        summary: None,
        history_witness: seed.history_witness,
        inherited_context: Vec::new(),
        shared_grants: Vec::new(),
        reference_grants: Vec::new(),
        attachment_manifests: Vec::new(),
        inherited_through_sequence: 0,
    };
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
    assert!(
        matches!(published.omissions.as_slice(), [ForkOmission { selection: ForkSelection { revision, .. }, outcome: Capture::Indeterminate(_) }] if revision == &optional)
    );
    seed.resources.push(CapturedResource {
        source: optional.clone(),
        revision: optional,
    });
    assert!(seed.validate(&mut None).is_err());
    seed.shared_grants.push(SharedGrant {
        volume: shared.clone(),
        child_agent: seed.child_agent,
        operations: BTreeSet::from([VolumeOperation::Read]),
    });
    assert!(
        seed.shared_capabilities(seed.child_agent)?
            .contains(&shared.capability(VolumeOperation::Read, &mut None)?)
    );
    seed.shared_grants[0].child_agent = AgentId::from_bytes([3; 16]);
    assert!(seed.validate(&mut None).is_err());
    seed.shared_grants[0].child_agent = seed.child_agent;
    let inherited = file(
        &seed.child_private_volume,
        ".system/inherited-conversation/prefix.json",
        b"one",
        "application/vnd.acyclic.harness.inherited-conversation+json",
    )?;
    seed.inherited_context = vec![inherited.clone(), inherited.clone()];
    seed.inherited_through_sequence = 1;
    assert!(seed.validate(&mut None).is_err());
    seed.inherited_context = vec![inherited.clone()];
    seed.attached_agents.push(attached);
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
