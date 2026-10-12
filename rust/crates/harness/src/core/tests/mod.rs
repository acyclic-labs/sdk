use super::*;

    use crate::conversation::FileDescriptor;

    use serde_json::json;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn original_proof_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 4096,
            peak_allocation_bytes: 16_777_216,
            bytes_copied: 16_777_216,
            bytes_encoded: 16_777_216,
            bytes_hashed: 16_777_216,
            source_bytes_read: 16_777_216,
            items_examined: 16_777_216,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn native_snapshot_round_trip(reducer: &Reducer, issuer: &AuthorityIssuer, maximum_events: usize) -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let source = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("snapshot ledger");
        let held = reducer.snapshot(maximum_events, &mut Some(&mut crate::store::history::native_work_io(&source).map_err(Error::from)?))?.0;
        let copy = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("copied payload ledger");
        let mut io = crate::store::history::native_work_io(&copy).map_err(Error::from)?;
        let input = held.copy(&mut Some(&mut io))?;
        drop(io);
        let copy_receipt = copy.receipt();
        let metadata = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("restore metadata ledger");
        let mut restored = Reducer::restore_native(input, issuer.verifier(&mut None)?, reducer.schemas.clone(), &metadata)?;
        restored.set_resident_event_limit(maximum_events)?;
        assert_eq!(&restored, reducer);
        assert_eq!(copy.receipt(), copy_receipt);
        let available = original_proof_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        assert!(copy.admit_retained(available).is_err());
        assert!(metadata.admit_retained(available).is_err());
        drop(restored);
        copy.admit_retained(available).map_err(Error::from)?;
        metadata.admit_retained(available).map_err(Error::from)?;
        assert!(source.admit_retained(available).is_err());
        drop(held);
        source.admit_retained(available).map_err(Error::from)?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn native_component_event(reference: &mut Reducer, action: Action) -> Result<Event> {
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&(reference.revision() + 1).to_le_bytes());
        let result = reference.apply(command(OperationId::from_bytes(id), reference.revision(), action))?;
        let ApplyResult::Applied { event } = result else { panic!("fresh canonical model event") };
        reference.mark_indexed_terminal_effect_with_work(&event, &mut None)?;
        Ok(event)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn native_component_apply(
        state: &mut Reducer, event: Event, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work).expect("original native observer");
        let result = state.apply_committed(event, &mut Some(&mut io))?;
        let ApplyResult::Applied { event } = result else { panic!("fresh canonical model application") };
        state.mark_indexed_terminal_effect_with_work(&event, &mut Some(&mut io))?;
        drop(event);
        drop(io);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn native_component_fixture(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Reducer, Reducer)> {
        use crate::store::history::HistoryIo;
        let owner = issuer();
        let registry = schemas();
        let mut reference = Reducer::new(owner.audience.clone(), owner.verifier(&mut None).expect("verifier copy"), registry.clone());
        reference.set_resident_event_limit(1)?;
        let mut io = crate::store::history::native_work_io(work).expect("original constructor observer");
        let authority = io.clone_authority(&owner.audience)?;
        let verifier = owner.verifier(&mut Some(&mut io))?;
        let shared_registry = registry.copy(&mut Some(&mut io))?;
        let mut state = Reducer::new_with_preclaimed_work(
            authority, verifier, shared_registry, &mut Some(&mut io),
        )?;
        drop(io);
        state.set_resident_event_limit(1)?;
        let event = native_component_event(&mut reference, Action::AppendCustom {
            schema: "example.message".into(), version: 1,
            content: request_file(br#"{"text":"original primed entry"}"#)?,
        })?;
        native_component_apply(&mut state, event, work)?;
        assert_eq!(state, reference);
        Ok((state, reference))
    }

    /// One generated reducer input; indices select earlier operations.
    #[derive(Clone, Debug)]
    pub(super) enum Step {
        Lifecycle(usize),
        Custom(u8),
        Bind(u8),
        Resubmit(usize),
        ConflictingIntent(usize),
        StaleRevision,
    }

    pub(in crate::core) fn step() -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        prop_oneof![
            (0..6_usize).prop_map(Step::Lifecycle),
            any::<u8>().prop_map(Step::Custom),
            any::<u8>().prop_map(Step::Bind),
            any::<usize>().prop_map(Step::Resubmit),
            any::<usize>().prop_map(Step::ConflictingIntent),
            Just(Step::StaleRevision),
        ]
    }

    pub(in crate::core) fn pick_admitted(
        admitted: &[(Command, Event)],
        pick: usize,
    ) -> std::result::Result<&(Command, Event), proptest::test_runner::TestCaseError> {
        admitted
            .get(pick % admitted.len())
            .ok_or_else(|| proptest::test_runner::TestCaseError::fail("no admitted operation"))
    }

    pub(in crate::core) fn operation(value: u8) -> OperationId {
        OperationId::from_bytes([value; 16])
    }

    pub(in crate::core) fn request_file(bytes: &[u8]) -> Result<FileRef> {
        use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
        use crate::resources::ProviderRef;
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "effects".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
             &mut None,)?,
            "requests/request.json".into(),
            "generation-1".into(),
            FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
            "request.json".into(),
         &mut None,)
    }

    pub(in crate::core) fn schema_file(schema: &Value) -> Result<FileRef> {
        use crate::conversation::FileDescriptor;
        let bytes =
            serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?;
        FileRef::new(
            request_file(b"null")?.volume().clone(),
            "schemas/result.json".into(),
            "generation-1".into(),
            FileDescriptor::from_bytes(&bytes, "application/schema+json", &mut None)?,
            "result.json".into(),
         &mut None,)
    }

    pub(in crate::core) fn issuer() -> AuthorityIssuer {
        AuthorityIssuer::new(
            "test",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "conversation-1".into(),
            },
        )
    }

    pub(in crate::core) fn schemas() -> SchemaRegistry {
        let mut schemas = SchemaRegistry::new();
        assert!(
            schemas
                .register(
                    "example.message",
                    1,
                    json!({
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "additionalProperties": true
                    }),
                    [9; 32],
                    ExtensionForkPolicy::Inherit, [], None,
                )
                .is_ok()
        );
        schemas
    }

    pub(in crate::core) fn reducer_fixture() -> Reducer {
        let owner = issuer();
        Reducer::new(owner.audience.clone(), owner.verifier(&mut None).expect("verifier copy"), schemas())
    }

    pub(in crate::core) fn command(operation_id: OperationId, expected_revision: u64, action: Action) -> Command {
        let capabilities = Capabilities::new([
            "event:append",
            "lifecycle:manage",
            "effect:run",
            "effect:plan",
            "effect:provider:example.provider",
            "interaction:open",
            "interaction:respond",
            "fork:publish",
            "conversation:bind",
            "conversation:append",
            "conversation:select_context",
        ]);
        let mut capabilities: Vec<String> = capabilities.iter().map(str::to_owned).collect();
        if let Action::PublishFork { seed } = &action {
            for resource in &seed.resources {
                if let crate::fork::ResourceRevision::PrivateVolume { volume, .. }
                    | crate::fork::ResourceRevision::Project { volume, .. } = &resource.source
                {
                    capabilities.push(
                        volume
                            .capability(crate::conversation::VolumeOperation::Read, &mut None)
                            .expect("valid source reference"),
                    );
                }
            }
        }
        let capabilities = Capabilities::new(capabilities);
        let scope = if matches!(&action, Action::PublishFork { .. }) {
            issuer().issue("root", &capabilities, Some(crate::AgentId::from_bytes([1; 16])), None, &mut None).expect("scope proof")
        } else {
            issuer().issue("root", &capabilities, None, None, &mut None).expect("scope proof")
        };
        Command {
            operation_id,
            idempotency_key: IdempotencyKey(format!("key-{operation_id}")),
            expected_revision,
            scope,
            causal_parent: None,
            action,
        }
    }
mod native;
mod model;
mod extensions;
mod authority;
mod conversation;
mod effects;
mod fork;
