//! tests fork semantics.
use super::*;
#[test]
    fn fork_is_invisible_until_one_seed_event_commits() -> Result<()> {
        use crate::conversation::{VolumeClass, VolumeOwner, VolumeRef};
        use crate::fork::{CapturedResource, ResourceRevision};
        use crate::resources::{GenerationRef, ProviderRef, ResourceKind, ResourceRef, StreamRef};

        let parent = Authority { kind: AggregateKind::Conversation, id: "conversation-1".into() };
        let child = Authority { kind: AggregateKind::Conversation, id: "conversation-2".into() };
        let child_agent = crate::AgentId::from_bytes([6; 16]);
        let mut seed: ForkSeed = serde_json::from_str(include_str!("../../../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        seed.operation_id = operation(8);
        seed.parent = parent.clone();
        seed.parent_revision = 1;
        seed.child = child.clone();
        seed.child_agent = child_agent;
        seed.resources.rotate_right(1);
        let provider = seed.child_private_volume.provider().clone();
        seed.child_private_volume = VolumeRef::new(provider.clone(), "child-private".into(),
            VolumeClass::AgentPrivate, VolumeOwner::Agent(child_agent), &mut None)?;
        if let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[0].source {
            *volume = VolumeRef::new(provider.clone(), "parent-private".into(), VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])), &mut None)?;
        }
        if let ResourceRevision::PrivateVolume { volume, .. } = &mut seed.resources[0].revision {
            *volume = seed.child_private_volume.clone();
        }
        let history = ResourceRevision::History(StreamRef::new(
            ProviderRef::new("acyclic".into(), "stream".into(), "1".into(), &mut None)?, parent.stream_path(&mut None)?.into_bytes(),
            Some("1".into()))?);
        seed.resources[1] = CapturedResource { source: history.clone(), revision: history };
        let extension = ResourceRevision::Extension {
            name: "example.message".into(),
            version: 1,
            implementation_digest: [9; 32],
            reference: ResourceRef::new(
                ResourceKind::Artifact,
                ProviderRef::new("test".into(), "objects".into(), "1".into(), &mut None)?,
                b"extension-state".to_vec().into(),
                Some("immutable-1".into()), &mut None
            )?,
        };
        let mut extension_seed = seed.clone();
        extension_seed.resources.push(CapturedResource {
            source: extension.clone(),
            revision: extension,
        });
        let mut reset_seed = extension_seed.clone();
        if let ResourceRevision::Extension { reference, .. } = &mut reset_seed.resources[3].revision
        {
            *reference = ResourceRef::new(
                ResourceKind::Artifact,
                ProviderRef::new("test".into(), "objects".into(), "1".into(), &mut None)?,
                b"child-extension-state".to_vec().into(),
                Some("immutable-1".into()), &mut None
            )?;
        }
        for (policy, selected_seed, allowed) in [
            (ExtensionForkPolicy::Inherit, &extension_seed, true),
            (ExtensionForkPolicy::Reset, &extension_seed, false),
            (ExtensionForkPolicy::Reject, &extension_seed, false),
            (ExtensionForkPolicy::Inherit, &reset_seed, false),
            (ExtensionForkPolicy::Reset, &reset_seed, true),
        ] {
            let mut bindings = SchemaRegistry::new();
            bindings.register("example.message", 1, json!({"type": "object"}), [9; 32], policy, [], None)?;
            let mut candidate = Reducer::new(parent.clone(), issuer().verifier(&mut None).expect("verifier copy"), bindings);
            candidate.apply(command(operation(1), 0, Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            }))?;
            let result = candidate.plan(&command(operation(8), 1, Action::PublishFork {
                seed: Box::new((*selected_seed).clone()),
            }));
            assert_eq!(result.is_ok(), allowed, "fork policy {policy:?}: {result:?}");
        }
        let mut mismatched = extension_seed;
        if let ResourceRevision::Extension {
            implementation_digest,
            ..
        } = &mut mismatched.resources[3].revision
        {
            *implementation_digest = [8; 32];
        }
        mismatched.resources[3].source = mismatched.resources[3].revision.clone();
        let mut candidate = Reducer::new(parent.clone(), issuer().verifier(&mut None).expect("verifier copy"), schemas());
        candidate.apply(command(
            operation(1),
            0,
            Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            },
        ))?;
        assert!(matches!(
            candidate.plan(&command(
                operation(8),
                1,
                Action::PublishFork {
                    seed: Box::new(mismatched)
                },
            )),
            Err(Error::Conflict(_))
        ));
        let mut reducer = reducer_fixture();
        reducer.apply(command(
            operation(1),
            0,
            Action::BindConversation {
                agent: crate::AgentId::from_bytes([1; 16]),
            },
        ))?;
        #[cfg(feature = "filesystem")]
        {
            use acyclic_fs::{CancellationToken, WorkBudget};
            let budget = WorkBudget {
                bytes_copied: 16_777_216, bytes_encoded: 16_777_216, bytes_hashed: 16_777_216,
                items_examined: 1_000_000, allocation_operations: 100_000, peak_allocation_bytes: 268_435_456,
                ..WorkBudget::default()
            };
            let cancellation = CancellationToken::new();
            let mut work = crate::store::history::HistoryWork::new(budget, &cancellation);
            reducer.validate_fork_reference_ownership_with_work(&seed, &mut Some(&mut work))?;
            reducer.require_fresh_fork_with_work(&seed, &mut Some(&mut work))?;
            let mut narrow = crate::store::history::HistoryWork::new(
                WorkBudget { items_examined: 1, ..budget }, &cancellation);
            assert!(reducer.require_fresh_fork_with_work(&seed, &mut Some(&mut narrow)).is_err());
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            let mut work = crate::store::history::HistoryWork::new(budget, &cancelled);
            assert!(reducer.validate_fork_reference_ownership_with_work(&seed, &mut Some(&mut work)).is_err());

            let prefix = InheritedConversationPrefix::select(parent.clone(), 1, 0, &[], &reducer.conversation, &mut None)?;
            let prefix_bytes = crate::contract::canonical_json_bytes(&prefix)?;
            let mut materialized = seed.clone();
            materialized.inherited_context.push(FileRef::new(
                seed.child_private_volume.clone(), ".system/inherited-conversation/prefix.json".into(), "generation-1".into(),
                FileDescriptor::from_bytes(&prefix_bytes,
                    "application/vnd.acyclic.harness.inherited-conversation+json", &mut None)?,
                "inherited-conversation.json".into(),
             &mut None,)?);
            reducer.validate_fork_reference_ownership_with_work(&materialized, &mut None)?;
            let mut work = crate::store::history::HistoryWork::new(budget, &cancellation);
            reducer.validate_fork_reference_ownership_with_work(&materialized, &mut Some(&mut work))?;
            let mut work = crate::store::history::HistoryWork::new(
                WorkBudget { bytes_encoded: 1, ..budget }, &cancellation);
            assert!(reducer.validate_fork_reference_ownership_with_work(&materialized, &mut Some(&mut work)).is_err());
            let mut hostile_manifests = seed.clone();
            hostile_manifests.attachment_manifests.push(request_file(b"[]")?);
            assert!(matches!(reducer.validate_fork_reference_ownership_with_work(&hostile_manifests, &mut None), Err(Error::Invalid(_))));
            let mut work = crate::store::history::HistoryWork::new(
                WorkBudget { peak_allocation_bytes: 1, ..budget }, &cancellation);
            assert!(reducer.validate_fork_reference_ownership_with_work(&materialized, &mut Some(&mut work)).is_err());
        }
        let mut foreign_command = command(
            operation(8),
            1,
            Action::PublishFork {
                seed: Box::new(seed.clone()),
            },
        );
        foreign_command.scope = issuer().issue("foreign-agent", &Capabilities::new(["event:append", "fork:publish"]), Some(crate::AgentId::from_bytes([2; 16])), None, &mut None).expect("scope proof");
        assert!(matches!(
            reducer.plan(&foreign_command),
            Err(Error::Unauthorized(_))
        ));
        foreign_command.scope = issuer().issue("agentless-root", &Capabilities::new(["event:append", "fork:publish"]), None, None, &mut None).expect("scope proof");
        assert!(matches!(
            reducer.plan(&foreign_command),
            Err(Error::Unauthorized(_))
        ));
        let mut unreadable_source = command(
            operation(8), 1, Action::PublishFork { seed: Box::new(seed.clone()) },
        );
        let project_read = seed.resources.iter().find_map(|resource| match &resource.source {
            ResourceRevision::Project { volume, .. } => Some(volume.capability(VolumeOperation::Read, &mut None)),
            _ => None,
        }).ok_or_else(|| Error::Invalid("fixture project source is absent".into()))??;
        unreadable_source.scope = issuer().issue("current-agent", &Capabilities::new(unreadable_source.scope.capabilities().iter().filter(|grant| *grant != project_read)), Some(crate::AgentId::from_bytes([1;16])), None, &mut None).expect("scope proof");
        assert!(matches!(reducer.plan(&unreadable_source), Err(Error::Unauthorized(_))));
        assert_eq!(reducer.revision(), 1);
        let planned = reducer.plan(&command(
            operation(8),
            1,
            Action::PublishFork {
                seed: Box::new(seed.clone()),
            },
        ))?;
        assert!(reducer.fork(&child).is_none());
        let ApplyResult::Applied { event } = planned else {
            return Err(Error::Invalid("new fork unexpectedly replayed".into()));
        };
        let mut foreign_event = event.clone();
        foreign_event.scope = RecordedScope::from_scope(&issuer().issue("foreign-agent", &Capabilities::new(["event:append", "fork:publish"]), Some(crate::AgentId::from_bytes([2; 16])), None, &mut None).expect("scope proof"));
        foreign_event.attestation = issuer().verifier(&mut None).expect("verifier copy").attest_event(&foreign_event, &mut None)?;
        assert!(matches!(
            reducer.apply_committed(foreign_event, &mut None),
            Err(Error::Unauthorized(_))
        ));
        reducer.apply_committed(event, &mut None)?;
        assert_eq!(reducer.fork(&child), Some(&seed));
        let wire = crate::contract::canonical_json_bytes(&reducer.snapshot(usize::MAX, &mut None)?.0)?;
        let snapshot = crate::contract::json_from_slice(&wire)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let restored = Reducer::restore(snapshot, issuer().verifier(&mut None).expect("verifier copy"), schemas(), &mut None)?;
        assert_eq!(restored, reducer);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        native_snapshot_round_trip(&reducer, &issuer(), usize::MAX)?;
        reducer = restored;
        let mut reused_private = seed;
        reused_private.operation_id = operation(9);
        reused_private.child.id = "conversation-3".into();
        reused_private.parent_revision = 2;
        let next_history = ResourceRevision::History(StreamRef::new(
            ProviderRef::new("acyclic".into(), "stream".into(), "1".into(), &mut None)?,
            parent.stream_path(&mut None)?.into_bytes(),
            Some("2".into()),
        )?);
        reused_private.resources[1].source = next_history.clone();
        reused_private.resources[1].revision = next_history;
        reused_private.validate(&mut None)?;
        let duplicate = reducer.plan(&command(
            operation(9),
            2,
            Action::PublishFork {
                seed: Box::new(reused_private),
            },
        ));
        assert!(
            matches!(duplicate, Err(Error::Conflict(_))),
            "{duplicate:?}"
        );
        Ok(())
    }
