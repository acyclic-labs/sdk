use super::*;

#[tokio::test]
async fn measured_cold_retirement_preserves_original_atomic_indexes() -> Result<()> {
    use crate::core::EffectGuarantee;
    use crate::{EffectAttemptId, EffectId};
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let request = content()?;
    let schema_bytes = br#"{"type":"object"}"#;
    let schema = FileRef::new(
        request.volume().clone(),
        "effects/schema.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(schema_bytes, "application/schema+json", &mut None)?,
        "schema.json".into(),
        &mut None,
    )?;
    let content_verifier = Arc::new(MigrationContentStore(Arc::new(std::sync::Mutex::new(
        BTreeMap::from([
            (
                MigrationContentStore::key(&request)?,
                EXTENSION_BYTES.to_vec(),
            ),
            (MigrationContentStore::key(&schema)?, schema_bytes.to_vec()),
        ]),
    ))));
    let mut writer = with_content(&provider)
        .await?
        .with_content_verifier(content_verifier);
    let effect_id = EffectId::from_bytes([41; 16]);
    let attempt_id = EffectAttemptId::from_bytes([42; 16]);
    let scope = issuer()
        .issue(
            "effects",
            &Capabilities::new(["effect:run", "effect:plan", "effect:provider:test.effects"]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let fresh = |identity: u8, revision, action| Command {
        operation_id: OperationId::from_bytes([identity; 16]),
        idempotency_key: IdempotencyKey::new(format!("measured-effect-{identity}"))
            .expect("finite key"),
        expected_revision: revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    };
    writer
        .execute(
            fresh(
                41,
                0,
                Action::PlanEffect {
                    effect_id,
                    provider: "test.effects".into(),
                    guarantee: EffectGuarantee::IdempotentRetry,
                    effect_kind: "test.write".into(),
                    request,
                    result_schema: schema,
                },
            ),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    writer
        .execute(
            fresh(
                42,
                1,
                Action::MarkEffectDispatched {
                    effect_id,
                    attempt_id,
                },
            ),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let observation = issuer().attest_effect(
        effect_id,
        writer
            .reducer()
            .effect(effect_id)
            .expect("dispatched effect"),
        attempt_id,
        EffectStatus::Failed {
            message: "known failure".into(),
        },
        &mut None,
    )?;
    writer
        .execute(
            fresh(43, 2, Action::ResolveEffect { observation }),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let expected = writer.reducer().snapshot(usize::MAX, &mut None)?.0;
    provider.forbid_writes.store(true, Ordering::SeqCst);
    provider.reset_io();
    let cancellation = CancellationToken::new();
    let restored = open_history(
        &provider,
        authority(),
        None,
        default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    assert_eq!(
        restored.value.reducer().snapshot(usize::MAX, &mut None)?.0,
        expected
    );
    assert_eq!(
        restored.work.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    let verifier = issuer().verifier(&mut None)?;
    let reader = HistoryReader::new(&provider, &authority(), &verifier, &mut None)?;
    let cursor = HistoryCursor {
        authority: authority(),
        after_revision: 0,
        through_revision: 3,
    };
    for cut in [1, 3] {
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
            budget(),
            &cancellation,
        )?);
        let (events, _) = effect_events(
            &provider,
            &verifier,
            &HistoryCursor {
                through_revision: cut,
                ..cursor.clone()
            },
            effect_id,
            default_projection_read_limits(),
            &mut Some(&mut io),
        )
        .await?;
        assert_eq!(
            events,
            expected
                .events
                .iter()
                .take(usize::try_from(cut).map_err(|error| Error::Invalid(error.to_string()))?)
                .cloned()
                .collect::<Vec<_>>()
        );
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let native = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
            .expect("finite reader budget");
        let expected_state = reader
            .effect(&cursor, effect_id, default_projection_read_limits(), None)
            .await?
            .map(|(state, _)| state);
        provider.reset_io();
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let state = reader
            .effect(
                &cursor,
                effect_id,
                default_projection_read_limits(),
                Some(&mut io),
            )
            .await?
            .map(|(state, _)| state);
        assert_eq!(state, expected_state);
        io.return_staged_output()?;
        let custody = io.into_custody()?;
        assert!(native.shares_ledger(custody.work()));
        assert_eq!(
            native.receipt().authority_bytes_read,
            provider.io_snapshot().read_bytes
        );
        drop(state);
        drop(custody);
    }
    assert!(
        reader
            .effect(
                &cursor,
                EffectId::from_bytes([99; 16]),
                default_projection_read_limits(),
                None
            )
            .await?
            .is_none()
    );
    let mut denied = HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        WorkBudget {
            backend_read_operations: 0,
            ..budget()
        },
        &cancellation,
    )?);
    provider.reset_io();
    assert!(
        reader
            .effect(
                &cursor,
                effect_id,
                default_projection_read_limits(),
                Some(&mut denied)
            )
            .await
            .is_err()
    );
    assert_eq!(provider.io_snapshot().reads, 0);
    assert_eq!(provider.io_snapshot().tails, 0);
    for (canonical_missing, transition_missing) in [(true, 0), (false, 3)] {
        provider
            .hide_aggregate_read
            .store(canonical_missing, Ordering::SeqCst);
        provider
            .hide_effect_transition_position
            .store(transition_missing, Ordering::SeqCst);
        provider.reset_io();
        let failure = open_history(
            &provider,
            authority(),
            None,
            default_projection_read_limits(),
            budget(),
            &cancellation,
        )
        .await
        .err()
        .expect("incomplete retirement proof");
        assert_eq!(
            failure.work.authority_bytes_read,
            provider.io_snapshot().read_bytes
        );
        assert_eq!(failure.work.output_bytes, 0);
    }
    provider
        .hide_effect_transition_position
        .store(0, Ordering::SeqCst);
    assert_eq!(
        open_history(
            &provider,
            authority(),
            None,
            default_projection_read_limits(),
            budget(),
            &cancellation
        )
        .await
        .map_err(|failure| failure.error)?
        .value
        .reducer()
        .snapshot(usize::MAX, &mut None)?
        .0,
        expected
    );
    Ok(())
}
