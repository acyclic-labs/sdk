//! Exact fork policy, admission, and publication.
use super::*;

#[test]
fn fork_is_invisible_until_one_seed_event_commits() -> Result<()> {
    use crate::conversation::VolumeClass;
    use crate::fork::{CapturedResource, ResourceRevision};
    use crate::resources::{ProviderRef, ResourceKind, ResourceRef, StreamRef};
    let parent = issuer().audience.clone();
    let child = Authority {
        kind: AggregateKind::Conversation,
        id: "conversation-2".into(),
    };
    let child_agent = AgentId::from_bytes([6; 16]);
    let mut seed: ForkSeed =
        crate::contract::json_from_slice(include_bytes!("../../../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    seed.operation_id = operation(8);
    seed.parent = parent.clone();
    seed.parent_revision = 1;
    seed.child = child.clone();
    seed.child_agent = child_agent;
    seed.resources.rotate_right(1);
    let provider = seed.child_private_volume.provider().clone();
    seed.child_private_volume = VolumeRef::new(
        provider.clone(),
        "child-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    if let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[0].source {
        *volume = VolumeRef::new(
            provider,
            "parent-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?;
    }
    if let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[0].revision {
        *volume = seed.child_private_volume.clone();
    }
    let history = |revision: &str| -> Result<ResourceRevision> {
        Ok(ResourceRevision::History(StreamRef::new(
            ProviderRef::new("acyclic".into(), "stream".into(), "1".into(), &mut None)?,
            parent.stream_path(&mut None)?.into_bytes(),
            Some(revision.into()),
        )?))
    };
    seed.resources[1] = CapturedResource {
        source: history("1")?,
        revision: history("1")?,
    };
    let bind = |reducer: &mut Reducer| -> Result<()> {
        reducer.apply(command(
            operation(1),
            0,
            Action::BindConversation {
                agent: AgentId::from_bytes([1; 16]),
            },
        ))?;
        Ok(())
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
        let mut registry = SchemaRegistry::new();
        registry.register(
            "example.message",
            1,
            json!({"type":"object"}),
            [9; 32],
            policy,
            [],
            None,
        )?;
        let mut candidate = Reducer::new(parent.clone(), issuer().verifier(&mut None)?, registry);
        bind(&mut candidate)?;
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
    bind(&mut reducer)?;
    assert!(matches!(
        reducer.plan(&publication(extension_seed)),
        Err(Error::Conflict(_))
    ));
    #[cfg(feature = "filesystem")]
    {
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
        let mut work = crate::store::history::HistoryWork::new(budget, &token);
        reducer.validate_fork_reference_ownership(&seed, None, &mut Some(&mut work))?;
        reducer.require_fresh_fork_with_work(&seed, &mut Some(&mut work))?;
        let mut narrow = crate::store::history::HistoryWork::new(
            WorkBudget {
                items_examined: 1,
                ..budget
            },
            &token,
        );
        assert!(
            reducer
                .require_fresh_fork_with_work(&seed, &mut Some(&mut narrow))
                .is_err()
        );
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let mut work = crate::store::history::HistoryWork::new(budget, &cancelled);
        assert!(
            reducer
                .validate_fork_reference_ownership(&seed, None, &mut Some(&mut work))
                .is_err()
        );
        let prefix = InheritedConversationPrefix::select(
            parent.clone(),
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
        reducer.validate_fork_reference_ownership(&materialized, None, &mut None)?;
        let mut work = crate::store::history::HistoryWork::new(budget, &token);
        reducer.validate_fork_reference_ownership(&materialized, None, &mut Some(&mut work))?;
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
            let mut work = crate::store::history::HistoryWork::new(limited, &token);
            assert!(
                reducer
                    .validate_fork_reference_ownership(&materialized, None, &mut Some(&mut work))
                    .is_err()
            );
        }
        let mut hostile = seed.clone();
        hostile.attachment_manifests.push(request_file(b"[]")?);
        assert!(matches!(
            reducer.validate_fork_reference_ownership(&hostile, None, &mut None),
            Err(Error::Invalid(_))
        ));
    }
    let publish = publication(seed.clone());
    for agent in [None, Some(AgentId::from_bytes([2; 16]))] {
        let mut foreign = publish.clone();
        foreign.scope = issuer().issue(
            "foreign",
            &Capabilities::new(["event:append", "fork:publish"]),
            agent,
            None,
            &mut None,
        )?;
        assert!(matches!(
            reducer.plan(&foreign),
            Err(Error::Unauthorized(_))
        ));
    }
    let read = seed
        .resources
        .iter()
        .find_map(|capture| match &capture.source {
            ResourceRevision::Project { volume, .. } => {
                Some(volume.capability(VolumeOperation::Read, &mut None))
            }
            _ => None,
        })
        .expect("project fixture")?;
    let mut unreadable = publish.clone();
    unreadable.scope = issuer().issue(
        "current",
        &Capabilities::new(
            publish
                .scope
                .capabilities()
                .iter()
                .filter(|grant| *grant != read),
        ),
        Some(AgentId::from_bytes([1; 16])),
        None,
        &mut None,
    )?;
    assert!(matches!(
        reducer.plan(&unreadable),
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(reducer.revision(), 1);
    let ApplyResult::Applied { event } = reducer.plan(&publish)? else {
        panic!("fresh fork replayed")
    };
    assert!(reducer.fork(&child).is_none());
    let mut foreign = event.clone();
    foreign.scope = RecordedScope::from_scope(&issuer().issue(
        "foreign",
        &Capabilities::new(["event:append", "fork:publish"]),
        Some(AgentId::from_bytes([2; 16])),
        None,
        &mut None,
    )?);
    foreign.attestation = issuer()
        .verifier(&mut None)?
        .attest_event(&foreign, &mut None)?;
    assert!(matches!(
        reducer.apply_committed(foreign, &mut None),
        Err(Error::Unauthorized(_))
    ));
    reducer.apply_committed(event, &mut None)?;
    assert_eq!(reducer.fork(&child), Some(&seed));
    let wire = crate::contract::canonical_json_bytes(&reducer.snapshot(usize::MAX, &mut None)?.0)?;
    let snapshot = crate::contract::json_from_slice(&wire)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let restored = Reducer::restore(
        snapshot,
        issuer().verifier(&mut None)?,
        schemas(),
        &mut None,
    )?;
    assert_eq!(restored, reducer);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_snapshot_round_trip(&reducer, &issuer(), usize::MAX)?;
    let mut reused = seed;
    reused.operation_id = operation(9);
    reused.child.id = "conversation-3".into();
    reused.parent_revision = 2;
    reused.resources[1] = CapturedResource {
        source: history("2")?,
        revision: history("2")?,
    };
    reused.validate(&mut None)?;
    assert!(matches!(
        reducer.plan(&command(
            operation(9),
            2,
            Action::PublishFork {
                seed: Box::new(reused)
            }
        )),
        Err(Error::Conflict(_))
    ));
    Ok(())
}
