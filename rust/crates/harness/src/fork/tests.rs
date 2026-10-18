use super::*;
use crate::{conversation::FileDescriptor, core::AggregateKind};
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
#[path = "isolation_properties.rs"]
mod isolation_properties;

#[cfg(not(target_arch = "wasm32"))]
#[path = "summary_properties.rs"]
mod summary_properties;

fn seed() -> Result<ForkSeed> {
    serde_json::from_str(include_str!("../../fixtures/v2/fork-seed.json"))
        .map_err(|error| Error::Invalid(error.to_string()))
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    // Conversations retain exactly one authenticated history selection. Task
    // forks carry no conversation history, prefix, summary or history witness.
    #[test]
    fn fork_kind_preserves_exact_history_and_round_trips(
        include_history in proptest::bool::ANY,
        through in 0u64..4,
        witness in proptest::bool::ANY,
    ) {
        use proptest::prelude::*;
        let kinds = [AggregateKind::Agent, AggregateKind::Conversation, AggregateKind::Session, AggregateKind::Turn, AggregateKind::Task];
        for parent in 0..kinds.len() {
        for child in 0..kinds.len() {
        let mut seed = seed().unwrap();
        seed.parent.kind = kinds[parent];
        seed.child.kind = kinds[child];
        if !include_history {
            seed.resources.retain(|capture| !matches!(capture.revision, ResourceRevision::History(_)));
        }
        seed.inherited_through_sequence = through;
        seed.history_witness = witness.then_some([1; 32]);
        let expected = through == 0 && match (kinds[parent], kinds[child]) {
            (AggregateKind::Conversation, AggregateKind::Conversation) => include_history,
            (AggregateKind::Task, AggregateKind::Task) => !include_history && !witness,
            _ => false,
        };
        prop_assert_eq!(seed.validate(&mut None).is_ok(), expected);
        if expected {
            let encoded = serde_json::to_vec(&seed).unwrap();
            let decoded: ForkSeed = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(decoded, seed.clone());
            prop_assert!(seed.validate(&mut None).is_ok());
        }
        let project = seed.resources.iter().find_map(|capture| match &capture.revision {
            ResourceRevision::Project { volume, .. } => Some(volume.clone()), _ => None,
        }).unwrap();
        let request = ForkRequest {
            operation_id: seed.operation_id, parent: seed.parent, parent_revision: seed.parent_revision,
            child: seed.child, child_agent: seed.child_agent, attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: project, child_private_volume: seed.child_private_volume,
                child_result_volume: None, summary: None, inherited_through_sequence: through,
                maximum_inherited_messages: 4, maximum_inherited_bytes: 65536, maximum_inherited_references: 1,
            },
            selections: seed.resources.into_iter().map(|capture| ForkSelection { required: true, revision: capture.source }).collect(),
            boundary: None, authorized_source: None,
        };
        // The request has no materialized context yet: a conversation may ask
        // for a nonzero prefix, while a task may only ask for a fresh context.
        let expected_request = match (kinds[parent], kinds[child]) {
            (AggregateKind::Conversation, AggregateKind::Conversation) => include_history,
            (AggregateKind::Task, AggregateKind::Task) => !include_history && through == 0,
            _ => false,
        };
        prop_assert_eq!(request.validate(&mut None).is_ok(), expected_request);
        if expected_request {
            let encoded = serde_json::to_vec(&request).unwrap();
            let decoded: ForkRequest = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(decoded, request);
        }
        }
        }
    }
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

struct Provider(ProviderRef);

fn files(provider: &ProviderRef) -> Arc<dyn ForkSeedVerifier> {
    Arc::new(Provider(provider.clone()))
}

impl ForkSeedVerifier for Provider {
    fn provider(&self) -> &ProviderRef {
        &self.0
    }
    fn verify<'a, 'w: 'a>(
        &'a self,
        _: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>,
    ) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |_| async { Ok(()) })
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
    let filesystem_files = files(&filesystem);
    let only_files =
        CompositeForkVerifier::new(std::slice::from_ref(&filesystem_files), &mut None)?;
    assert!(matches!(
        block_on(only_files.verify(&seed, &mut None)),
        Err(Error::Unsupported(_))
    ));
    let complete = CompositeForkVerifier::new(&[filesystem_files, files(&stream)], &mut None)?;
    block_on(complete.verify(&seed, &mut None))?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let budget = WorkBudget {
            allocation_operations: 64,
            peak_allocation_bytes: 65_536,
            items_examined: 65_536,
            ..WorkBudget::default()
        };
        let native = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let retained = {
            let io = crate::filesystem::PreparationIo::from_native_work(&native)?;
            CompositeForkVerifier::new(&[], &mut Some(&mut io.history_io()))?
        };
        let available =
            budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        assert!(native.admit_retained(available).is_err());
        drop(retained);
        native.admit_retained(available).map_err(Error::from)?;
    }
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
    let attached = AgentId::from_bytes([9; 16]);
    seed.attached_agents.push(attached);
    let access = seed.attached_read_capabilities(attached)?;
    assert!(access.contains(&inherited.read_capability(&mut None)?));
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
