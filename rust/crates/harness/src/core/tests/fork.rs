use super::*;
use crate::fork::{CapturedResource, ResourceRevision};
use crate::resources::{ProviderRef, ResourceKind, ResourceRef, StreamRef};

#[test]
fn fork_is_invisible_until_one_seed_event_commits() -> Result<()> {
    let mut seed: ForkSeed =
        crate::contract::json_from_slice(include_bytes!("../../../fixtures/v2/fork-seed.json"))
            .unwrap();
    seed.operation_id = operation(8);
    seed.parent = issuer().audience;
    seed.parent_revision = 1;
    let history = |revision: &str| -> Result<ResourceRevision> {
        Ok(ResourceRevision::History(StreamRef::new(
            ProviderRef::new("acyclic".into(), "stream".into(), "1".into(), &mut None)?,
            seed.parent.stream_path(&mut None)?.into_bytes(),
            Some(revision.into()),
        )?))
    };
    seed.resources[0] = CapturedResource {
        source: history("1")?,
        revision: history("1")?,
    };
    if let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[2].source {
        *volume = VolumeRef::new(
            volume.provider().clone(),
            "parent-private".into(),
            crate::conversation::VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?;
    }
    let bind = Action::BindConversation {
        agent: AgentId::from_bytes([1; 16]),
    };
    let publication = |seed| {
        command(
            operation(8),
            1,
            Action::PublishFork {
                seed: Box::new(seed),
            },
        )
    };
    let extension = ResourceRevision::Extension {
        name: "example.message".into(),
        version: 1,
        implementation_digest: [9; 32],
        reference: ResourceRef::new(
            ResourceKind::Artifact,
            ProviderRef::new("test".into(), "objects".into(), "1".into(), &mut None)?,
            b"extension-state".to_vec().into(),
            Some("immutable-1".into()),
            &mut None,
        )?,
    };
    let mut extension_seed = seed.clone();
    extension_seed.resources.push(CapturedResource {
        source: extension.clone(),
        revision: extension,
    });
    let mut reset = extension_seed.clone();
    if let ResourceRevision::Extension { reference, .. } = &mut reset.resources[3].revision {
        *reference = ResourceRef::new(
            ResourceKind::Artifact,
            reference.provider().clone(),
            b"child-extension-state".to_vec().into(),
            Some("immutable-1".into()),
            &mut None,
        )?;
    }
    for (policy, selected, allowed) in [
        (ExtensionForkPolicy::Inherit, &extension_seed, true),
        (ExtensionForkPolicy::Reset, &extension_seed, false),
        (ExtensionForkPolicy::Reject, &extension_seed, false),
        (ExtensionForkPolicy::Inherit, &reset, false),
        (ExtensionForkPolicy::Reset, &reset, true),
    ] {
        let mut candidate = reducer_fixture();
        Arc::make_mut(candidate.schemas.schemas.as_mut().unwrap())
            .get_mut(&("example.message".into(), 1))
            .unwrap()
            .fork_policy = policy;
        apply(&mut candidate, 1, bind.clone())?;
        assert_eq!(
            candidate.plan(&publication(selected.clone())).is_ok(),
            allowed
        );
    }
    if let ResourceRevision::Extension {
        implementation_digest,
        ..
    } = &mut extension_seed.resources[3].revision
    {
        *implementation_digest = [8; 32];
    }
    extension_seed.resources[3].source = extension_seed.resources[3].revision.clone();
    let mut reducer = reducer_fixture();
    apply(&mut reducer, 1, bind)?;
    assert!(reducer.plan(&publication(extension_seed)).is_err());
    #[cfg(feature = "filesystem")]
    {
        use crate::filesystem::PreparationIo;
        use acyclic_fs::{CancellationToken, WorkBudget};
        let budget = WorkBudget {
            bytes_copied: 16_777_216,
            bytes_encoded: 16_777_216,
            bytes_hashed: 16_777_216,
            items_examined: 1_000_000,
            allocation_operations: 100_000,
            peak_allocation_bytes: 268_435_456,
            ..WorkBudget::default()
        };
        let token = CancellationToken::new();
        let prefix = InheritedConversationPrefix::select(
            seed.parent.clone(),
            1,
            0,
            &[],
            &reducer.projection.conversation,
            &mut None,
        )?;
        let bytes = crate::contract::canonical_json_bytes(&prefix)?;
        let mut materialized = seed.clone();
        materialized.inherited_context.push(FileRef::new(
            seed.child_private_volume.clone(),
            ".system/inherited-conversation/prefix.json".into(),
            "generation-1".into(),
            FileDescriptor::from_bytes(
                &bytes,
                "application/vnd.acyclic.harness.inherited-conversation+json",
                &mut None,
            )?,
            "inherited-conversation.json".into(),
            &mut None,
        )?);
        let work = PreparationIo::from_budget(budget, &token)?;
        reducer.validate_fork_reference_ownership(
            &materialized,
            None,
            &mut Some(&mut work.history_io()),
        )?;
        for limited in [
            WorkBudget {
                bytes_encoded: 1,
                ..budget
            },
            WorkBudget {
                peak_allocation_bytes: 1,
                ..budget
            },
        ] {
            let work = PreparationIo::from_budget(limited, &token)?;
            assert!(
                reducer
                    .validate_fork_reference_ownership(
                        &materialized,
                        None,
                        &mut Some(&mut work.history_io())
                    )
                    .is_err()
            );
        }
        let mut hostile = seed.clone();
        hostile.attachment_manifests.push(request_file(b"[]")?);
        assert!(
            reducer
                .validate_fork_reference_ownership(&hostile, None, &mut None)
                .is_err()
        );
    }
    let publish = publication(seed.clone());
    let capabilities = Capabilities::new(["event:append", "fork:publish"]);
    for agent in [None, Some(AgentId::from_bytes([2; 16]))] {
        let mut foreign = publish.clone();
        foreign.scope = issuer().issue("foreign", &capabilities, agent, None, &mut None)?;
        assert!(reducer.plan(&foreign).is_err());
    }
    let ResourceRevision::Project { volume, .. } = &seed.resources[1].source else {
        panic!("project fixture")
    };
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let mut unreadable = publish.clone();
    let grants = publish
        .scope
        .capabilities()
        .iter()
        .filter(|grant| *grant != read);
    let grants = Capabilities::new(grants);
    let parent_agent = AgentId::from_bytes([1; 16]);
    unreadable.scope = issuer().issue("current", &grants, Some(parent_agent), None, &mut None)?;
    assert!(reducer.plan(&unreadable).is_err());
    let ApplyResult::Applied { event } = reducer.plan(&publish)? else {
        panic!("fresh fork replayed")
    };
    assert!(reducer.fork(&seed.child).is_none());
    let mut foreign = event.clone();
    let agent = Some(AgentId::from_bytes([2; 16]));
    let scope = issuer().issue("foreign", &capabilities, agent, None, &mut None)?;
    foreign.scope = RecordedScope::from_scope(&scope);
    foreign.attestation = issuer()
        .verifier(&mut None)?
        .attest_event(&foreign, &mut None)?;
    assert!(reducer.apply_committed(foreign, &mut None).is_err());
    reducer.apply_committed(event, &mut None)?;
    assert_eq!(reducer.fork(&seed.child), Some(&seed));
    assert_eq!(restore(&reducer, &issuer())?, reducer);
    let mut reused = seed.clone();
    reused.operation_id = operation(9);
    reused.child.id = "conversation-3".into();
    reused.parent_revision = 2;
    reused.resources[0] = CapturedResource {
        source: history("2")?,
        revision: history("2")?,
    };
    let publication = Action::PublishFork {
        seed: Box::new(reused),
    };
    assert!(apply(&mut reducer, 9, publication).is_err());
    Ok(())
}
