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
    let mut writer = StreamAggregate::open(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
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
    let receipt = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .map_err(|failure| failure.error)?;
    assert_eq!(
        receipt.value.reducer().snapshot(usize::MAX, &mut None)?.0,
        expected
    );
    assert_eq!(
        receipt.work.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );

    assert_eq!(receipt.work.items_returned, 3);
    let reader = HistoryReader::new(
        &provider,
        &authority(),
        &(issuer().verifier(&mut None).expect("verifier copy")),
        &mut None,
    )?;
    for cut in [1, 3] {
        provider.reset_io();
        let mut work = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_budget(budget(), &cancellation)?,
        );
        work.preflight()?;
        let result = effect_events(
            &provider,
            &issuer().verifier(&mut None).expect("verifier copy"),
            &HistoryCursor {
                authority: authority(),
                after_revision: 0,
                through_revision: cut,
            },
            effect_id,
            default_projection_read_limits(),
            &mut Some(&mut work),
        )
        .await
        .map(|(events, _)| events);
        let archived = work.finish(result).map_err(|failure| failure.error)?;
        assert_eq!(
            archived.value,
            expected
                .events
                .iter()
                .take(cut as usize)
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_eq!(archived.work.items_returned, cut);
        assert_eq!(
            archived.work.authority_bytes_read,
            provider.io_snapshot().read_bytes
        );

        assert_eq!(provider.io_snapshot().writes, 0);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let native = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original effect reader ledger");
        provider.reset_io();
        let cursor = HistoryCursor {
            authority: authority(),
            after_revision: 0,
            through_revision: 3,
        };
        let expected_state = reader
            .effect(&cursor, effect_id, default_projection_read_limits(), None)
            .await
            .map(|state| state.map(|(state, _)| state))?;
        let prefix = native.receipt();
        provider.reset_io();
        let (state, state_custody) = async {
            let mut _history_io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&native)?,
            );
            let _history_value = reader
                .effect(
                    &cursor,
                    effect_id,
                    default_projection_read_limits(),
                    Some(&mut _history_io),
                )
                .await?
                .map(|(state, _)| state);
            _history_io.return_staged_output()?;
            Ok::<_, crate::Error>((_history_value, _history_io.into_custody()?))
        }
        .await?;
        assert_eq!(state, expected_state);
        assert!(native.shares_ledger(state_custody.work()));
        assert_eq!(
            native.receipt().authority_bytes_read - prefix.authority_bytes_read,
            provider.io_snapshot().read_bytes
        );
        drop(state);
        drop(state_custody);
    }
    provider.reset_io();
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget(),
        &cancellation,
    )?);
    work.preflight()?;
    let result = effect_events(
        &provider,
        &issuer().verifier(&mut None).expect("verifier copy"),
        &HistoryCursor {
            authority: authority(),
            after_revision: 0,
            through_revision: 3,
        },
        EffectId::from_bytes([99; 16]),
        default_projection_read_limits(),
        &mut Some(&mut work),
    )
    .await
    .map(|(events, _)| events);
    let absent = work.finish(result).map_err(|failure| failure.error)?;
    assert!(absent.value.is_empty());
    assert_eq!(absent.work.items_returned, 0);
    assert_eq!(absent.work.output_bytes, 0);
    assert!(absent.work.authority_records_read > 0);
    provider.reset_io();
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        WorkBudget {
            backend_read_operations: 0,
            ..budget()
        },
        &cancellation,
    )?);
    work.preflight()?;
    let result = effect_events(
        &provider,
        &issuer().verifier(&mut None).expect("verifier copy"),
        &HistoryCursor {
            authority: authority(),
            after_revision: 0,
            through_revision: 3,
        },
        effect_id,
        default_projection_read_limits(),
        &mut Some(&mut work),
    )
    .await
    .map(|(events, _)| events);
    let denied = work
        .finish(result)
        .expect_err("SDK admission must precede even the original tail request");
    assert_eq!(denied.work.backend_read_operations, 0);
    assert_eq!(provider.io_snapshot().reads, 0);
    assert_eq!(provider.io_snapshot().tails, 0);
    let terminal = expected.events.last().expect("original resolved event");
    provider.reset_io();
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        budget(),
        &cancellation,
    )?);
    work.preflight()?;
    let result = crate::store::effects::retirement_proof_for_event_with_work(
        &provider,
        &authority(),
        terminal,
        &mut Some(&mut work),
    )
    .await;
    let proof = work.finish(result).map_err(|failure| failure.error)?;
    assert!(proof.value);
    assert_eq!(
        proof.work.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert!(proof.work.bytes_encoded > 0);
    provider.reset_io();
    let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
        WorkBudget {
            bytes_encoded: 0,
            ..budget()
        },
        &cancellation,
    )?);
    work.preflight()?;
    let result = crate::store::effects::retirement_proof_for_event_with_work(
        &provider,
        &authority(),
        terminal,
        &mut Some(&mut work),
    )
    .await;
    let failure = work
        .finish(result)
        .err()
        .expect("original canonical re-encode must admit before owned codec growth");
    assert_eq!(
        failure.work.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert_eq!(failure.work.bytes_encoded, 0);
    assert_eq!(failure.work.output_bytes, 0);
    provider.hide_aggregate_read.store(true, Ordering::SeqCst);
    provider.reset_io();
    let failure = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .err()
    .expect("unavailable original operation index must preserve spent canonical suffix work");
    assert_eq!(
        failure.work.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert_eq!(failure.work.output_bytes, 0);
    assert_eq!(failure.work.items_returned, 0);
    provider.hide_aggregate_read.store(false, Ordering::SeqCst);
    provider
        .hide_effect_transition_position
        .store(3, Ordering::SeqCst);
    provider.reset_io();
    let failure = open_history(
        &provider,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        None,
        crate::store::default_projection_read_limits(),
        budget(),
        &cancellation,
    )
    .await
    .err()
    .expect("missing original terminal effect index cannot retire");
    assert_eq!(
        failure.work.authority_bytes_read,
        provider.io_snapshot().read_bytes
    );
    assert_eq!(failure.work.output_bytes, 0);
    assert_eq!(failure.work.items_returned, 0);
    provider
        .hide_effect_transition_position
        .store(0, Ordering::SeqCst);
    assert_eq!(
        open_history(
            &provider,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            None,
            crate::store::default_projection_read_limits(),
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
