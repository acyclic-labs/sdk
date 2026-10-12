    use super::*;
    use crate::core::{AggregateKind, AuthorityIssuer, ExtensionForkPolicy};
    use serde_json::json;

    fn fixture() -> (Reducer, AuthorityIssuer) {
        let authority = Authority {
            kind: AggregateKind::Agent,
            id: "checkpoint-agent".into(),
        };
        let issuer = AuthorityIssuer::new("checkpoint-owner", [23; 32], authority.clone());
        (
            Reducer::new(authority, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
            issuer,
        )
    }

    #[test]
    fn recomputing_public_digest_cannot_authenticate_forged_projection() -> Result<()> {
        let (reducer, issuer) = fixture();
        let original = reducer.snapshot(usize::MAX, &mut None)?.0;
        let mut forged = original.copy(&mut None)?;
        forged.projection.lifecycle = LifecycleState::Completed;
        forged.state_digest = forged.digest_with_work(&mut None)?;
        assert!(matches!(
            Reducer::restore(forged, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        let mut forged_cache = original;
        forged_cache.revision = 50;
        forged_cache.state_digest = forged_cache.digest_with_work(&mut None)?;
        assert!(matches!(
            Reducer::restore(forged_cache, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
        let (reducer, issuer) = fixture();
        let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
        for other in [
            AuthorityIssuer::new("checkpoint-owner", [24; 32], reducer.authority.clone()),
            AuthorityIssuer::new("other-owner", [23; 32], reducer.authority),
            AuthorityIssuer::new(
                "checkpoint-owner",
                [23; 32],
                Authority {
                    kind: AggregateKind::Agent,
                    id: "other-agent".into(),
                },
            ),
        ] {
            assert!(matches!(
                Reducer::restore(snapshot.copy(&mut None)?, other.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
                Err(Error::Unauthorized(_))
            ));
        }
        let mut old = snapshot;
        old.format_version = 2;
        assert!(matches!(
            Reducer::restore(old, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

    #[test]
    fn restore_pins_complete_installed_bindings_and_allows_new_namespaces() -> Result<()> {
        let (mut reducer, issuer) = fixture();
        reducer.schemas.register(
            "example.pinned",
            1,
            json!({"type":"object"}),
            [8; 32],
            ExtensionForkPolicy::Inherit,
            [],
            Some(json!({"type":"object"})),
        )?;
        let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
        assert!(matches!(
            Reducer::restore(snapshot.copy(&mut None)?, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()),
            Err(Error::Unsupported(_))
        ));
        for edit in 0..5 {
            let mut changed = reducer.schemas.clone();
            let binding = changed
                .schemas
                .as_mut().map(Arc::make_mut).expect("fixture registry")
                .get_mut(&("example.pinned".into(), 1))
                .ok_or_else(|| Error::Storage("fixture binding missing".into()))?;
            match edit {
                0 => binding.schema = json!({"type":"string"}),
                1 => binding.implementation_digest = [9; 32],
                2 => binding.fork_policy = ExtensionForkPolicy::Reset,
                3 => {
                    binding.dependencies.insert(ExtensionDependency {
                        name: "example.dependency".into(),
                        version: 1,
                    });
                }
                _ => binding.configuration_schema = Some(json!({"type":"string"})),
            }
            assert!(matches!(
                Reducer::restore(snapshot.copy(&mut None)?, issuer.verifier(&mut None).expect("verifier copy"), changed),
                Err(Error::Conflict(_))
            ));
        }
        let mut expanded = reducer.schemas.clone();
        expanded.register(
            "example.additional",
            1,
            json!({"type":"string"}),
            [9; 32],
            ExtensionForkPolicy::Reset, [], None,
        )?;
        let restored = Reducer::restore(snapshot, issuer.verifier(&mut None).expect("verifier copy"), expanded.clone())?;
        reducer.schemas = expanded;
        assert_eq!(restored, reducer);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 4096,
            peak_allocation_bytes: 4_194_304,
            bytes_copied: 4_194_304,
            bytes_encoded: 4_194_304,
            bytes_hashed: 4_194_304,
            source_bytes_read: 4_194_304,
            items_examined: 4_194_304,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn populated_fixture() -> Result<(Reducer, AuthorityIssuer)> {
        let (mut reducer, issuer) = fixture();
        for (index, to) in [LifecycleState::Active, LifecycleState::Completed].into_iter().enumerate() {
            reducer.apply(super::super::Command {
                operation_id: crate::OperationId::from_bytes([index as u8 + 1; 16]),
                idempotency_key: crate::IdempotencyKey::new(format!("snapshot-source-{index}"))?,
                expected_revision: index as u64,
                scope: issuer.issue("snapshot-source", &crate::Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof"),
                causal_parent: None,
                action: super::super::Action::TransitionLifecycle {
                    to, reason: Some(format!("{index}:{}", "owned canonical lifecycle reason".repeat(128))),
                },
            })?;
        }
        Ok((reducer, issuer))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_original_cold_decoder_preserves_digest_and_issuer_mac_rejection() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let (reducer, issuer) = populated_fixture()?;
        let original = reducer.snapshot(usize::MAX, &mut None)?.0;
        for forged_digest in [false, true] {
            let mut forged = original.copy(&mut None)?;
            if forged_digest {
                forged.projection.lifecycle = LifecycleState::Pending;
                forged.state_digest = forged.digest_with_work(&mut None)?;
            } else {
                forged.attestation[0] ^= 1;
            }
            let bytes = crate::contract::canonical_json_bytes(&forged)?;
            let token = CancellationToken::new();
            let source = NativeMaterializationWork::new(native_budget(), &token).expect("cold decoder ledger");
            let mut io = crate::store::history::native_work_io(&source).expect("cold decoder observer");
            let decoded = Snapshot::decode(&bytes, 4_194_304, &mut Some(&mut io))?;
            drop(io);
            assert!(matches!(
                Reducer::restore_native(decoded, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), &source),
                Err(Error::Unauthorized(_))
            ));
            source.admit_retained(native_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let mut forged = original;
        forged.projection.lifecycle = LifecycleState::Pending;
        let bytes = crate::contract::canonical_json_bytes(&forged)?;
        let token = CancellationToken::new();
        let source = NativeMaterializationWork::new(native_budget(), &token).expect("digest rejection ledger");
        let mut io = crate::store::history::native_work_io(&source).expect("original decoder observer");
        let decoded = Snapshot::decode(&bytes, 4_194_304, &mut Some(&mut io))?;
        drop(io);
        assert!(matches!(Reducer::restore_native(decoded, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), &source),
            Err(Error::Invalid(_))));
        source.admit_retained(native_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

