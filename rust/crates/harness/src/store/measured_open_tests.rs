#[cfg(feature = "filesystem")]
mod measured_open_tests {
    use super::*;
    use acyclic_fs::{CancellationToken, WorkBudget, WorkCounters};
    use std::sync::atomic::Ordering;

    type Provider = crate::test_stream::LostSessionAck<MemoryStream>;

    fn budget() -> WorkBudget {
        let bytes = 64 * acyclic_stream::MAX_COMMAND_BYTES as u64;
        WorkBudget {
            authority_records_read: 128,
            authority_bytes_read: bytes,
            backend_read_operations: 128,
            source_bytes_read: bytes,
            bytes_copied: bytes,
            bytes_encoded: bytes,
            bytes_hashed: bytes,
            output_bytes: bytes,
            items_examined: 1_000_000,
            items_returned: 128,
            allocation_operations: 1_000_000,
            peak_allocation_bytes: bytes,
            ..WorkBudget::default()
        }
    }

    async fn fixture() -> Result<(Arc<Provider>, StreamClient<Provider>)> {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?);
        for revision in 0..69 {
            writer.execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.reset_io();
        Ok((provider, client))
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_reader_constructor_rejects_before_copy_and_transfers_original_custody() -> Result<()> {
        use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let provider = Arc::new(Provider::default());
        let client = StreamClient::new(provider.clone());
        let cancellation = CancellationToken::new();
        let mut denied = budget();
        denied.bytes_copied = 0;
        let native = NativeMaterializationWork::new(denied, &cancellation)
            .expect("original accounting state");
        let prefix = native.receipt();
        let mut io = history::native_work_io(&native).expect("empty custody");
        assert!(matches!(HistoryReader::new_with_work(
            &client, &authority(), &issuer().verifier(), Some(&mut io)),
            Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                    counter: "bytes_copied", ..
                }), custody: None,
            })));
        assert_eq!(native.receipt(), prefix);
        drop(io);
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original accounting state");
        let mut io = history::native_work_io(&native).expect("empty custody");
        let reader = HistoryReader::new_with_work(
            &client, &authority(), &issuer().verifier(), Some(&mut io))?;
        let custody = io.into_custody();
        assert!(custody.bytes() >= authority().stream_path()?.len() as u64);
        assert!(native.receipt().bytes_copied >= authority().stream_path()?.len() as u64);
        let sdk = provider.io_snapshot();
        assert_eq!(sdk.reads + sdk.tails as u64 + sdk.writes + sdk.inspections
            + sdk.commit_reads + sdk.other_calls, 0);
        drop(reader);
        drop(custody);
        Ok(())
    }

    #[tokio::test]
    async fn prepared_publication_preserves_atomic_indexes_and_preallocation_denial() -> Result<()> {
        let provider = Arc::new(Provider::default());
        let client = StreamClient::new(provider.clone());
        let writer = with_content(StreamAggregate::open(
            &client, authority(), issuer().verifier(), schemas()).await?);
        let ApplyResult::Applied { event } = writer.reducer().plan(&command(1)?)? else {
            return Err(Error::Invalid("fresh original command must apply".into()));
        };
        let publication = operations::IndexedPublication::new(
            &authority(), &event, encode_event(&authority(), &event)?, &mut None)?;
        let cancellation = CancellationToken::new();
        let mut denied = budget();
        denied.allocation_operations = 0;
        provider.reset_io();
        let mut work = history::HistoryWork::new(denied, &cancellation);
        assert!(crate::distributed::digest_key(&[67; 32], &mut Some(&mut work)).is_err());
        let failed = work.finish(Ok(())).map_err(|failure| failure.error)?;
        assert_eq!(failed.work.allocation_operations + failed.work.bytes_copied, 0);
        assert_eq!(provider.io_snapshot().writes, 0);
        let mut allowed = budget();
        allowed.backend_write_operations = 1;
        allowed.authority_bytes_written = allowed.authority_bytes_read;
        allowed.authority_records_appended = allowed.authority_records_read;
        let mut work = history::HistoryWork::new(allowed, &cancellation);
        let mut observer: Option<&mut dyn history::HistoryIo> = Some(&mut work);
        let (key, held) = crate::distributed::digest_key(&[68; 32], &mut observer)?;
        let mut prepared = crate::distributed::commit_request(key, held, &mut observer);
        crate::distributed::add_condition(&mut prepared, acyclic_stream::CommitCondition::Absent {
            path: publication.path.clone(),
        })?;
        crate::distributed::add_record(&mut prepared, publication.path.clone(), publication.bytes.clone())?;
        publication.add_index_with_work(&mut prepared)?;
        let (outcome, retained) = crate::distributed::commit(prepared, &client, Some(event.operation_id), None, None, None).await?;
        let acyclic_stream::CommitOutcome::Committed(envelope) = outcome else {
            return Err(Error::Conflict("original indexed publication unexpectedly conflicted".into()));
        };
        publication.verify(&envelope)?;
        let mutations = envelope.mutations.len() as u64;
        drop(envelope);
        observer.as_deref_mut().expect("original observer").release_owned(retained)?;
        drop(observer);
        let receipt = work.finish(Ok(())).map_err(|failure| failure.error)?;
        assert_eq!(receipt.work.authority_records_appended, mutations);
        let reader = writer.history_reader()?;
        assert_eq!(reader.operation_event(event.operation_id).await?, Some(event));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_history_pages_preserve_original_provider_prefixes_and_custody() -> Result<()> {
        use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError};
        let (provider, client) = fixture().await?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite ledger");
        let mut io = history::native_work_io(&native).expect("original reader custody");
        let reader = HistoryReader::new_with_work(
            &client, &authority(), &issuer().verifier(), Some(&mut io))?;
        let reader_custody = io.into_custody();
        provider.reset_io();
        let (cursor, cursor_custody) = reader.pin_with_native_work(64, &native).await?;
        assert_eq!(cursor.through_revision, 69);
        assert!(cursor_custody.bytes() >= cursor.authority.id.len() as u64);
        assert!(native.shares_ledger(cursor_custody.work()));
        let prefix = native.receipt();
        provider.reset_io();
        let (page, page_custody) = reader.read_page_with_native_work(
            &cursor, default_projection_read_limits(), &native).await?;
        let receipt = native.receipt();
        let sdk = provider.io_snapshot();
        assert_eq!(page.events.len(), 5);
        assert_eq!(page.cursor.after_revision, 69);
        assert!(native.shares_ledger(page_custody.work()));
        assert!(page_custody.bytes() >= page.cursor.authority.id.len() as u64);
        assert_eq!(receipt.backend_read_operations - prefix.backend_read_operations,
            sdk.reads + sdk.tails as u64);
        assert_eq!(receipt.authority_bytes_read - prefix.authority_bytes_read, sdk.read_bytes);
        assert_eq!(receipt.output_bytes - prefix.output_bytes, sdk.read_bytes);
        assert_eq!(receipt.items_returned - prefix.items_returned, 5);
        assert_eq!(sdk.writes + sdk.inspections + sdk.commit_reads + sdk.other_calls, 0);
        let prefix = native.receipt();
        provider.reset_io();
        let (archived, archived_custody) = reader.operation_event_bounded_with_native_work(
            page.events[0].operation_id, budget().authority_bytes_read, &native).await?;
        assert_eq!(archived.as_ref(), Some(&page.events[0]));
        assert!(native.shares_ledger(archived_custody.work()));
        assert_eq!(native.receipt().items_returned - prefix.items_returned, 1);
        assert_eq!(native.receipt().authority_bytes_read - prefix.authority_bytes_read,
            provider.io_snapshot().read_bytes);
        drop(archived);
        drop(archived_custody);
        drop(page);
        drop(page_custody);

        for narrow in [
            WorkBudget { output_bytes: 0, ..budget() },
            WorkBudget { authority_bytes_read: 0, ..budget() },
        ] {
            let denied = NativeMaterializationWork::new(narrow, &cancellation)
                .expect("original finite denied ledger");
            provider.reset_io();
            assert!(matches!(reader.read_page_with_native_work(
                &cursor, default_projection_read_limits(), &denied).await,
                Err(Error::NativeWork { custody: None, .. })));
            let receipt = denied.receipt();
            assert_eq!(receipt.authority_bytes_read, provider.io_snapshot().read_bytes);
            assert!(receipt.authority_bytes_read > 0);
            assert_eq!(receipt.output_bytes, 0);
            assert_eq!(receipt.items_returned, 0);
        }

        let fault_work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original failed source ledger");
        provider.reset_io();
        provider.history_read_fault.store(4, Ordering::SeqCst);
        assert!(reader.read_page_with_native_work(
            &cursor, default_projection_read_limits(), &fault_work).await.is_err());
        assert_eq!(fault_work.receipt().authority_bytes_read, 7);
        assert_eq!(fault_work.receipt().authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(fault_work.receipt().output_bytes, 0);
        assert_eq!(fault_work.receipt().items_returned, 0);

        let wrong_issuer = AuthorityIssuer::new("test", [8; 32], authority());
        let auth_work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original authentication ledger");
        let mut io = history::native_work_io(&auth_work).expect("original reader custody");
        let wrong_reader = HistoryReader::new_with_work(
            &client, &authority(), &wrong_issuer.verifier(), Some(&mut io))?;
        let wrong_custody = io.into_custody();
        provider.reset_io();
        assert!(wrong_reader.read_page_with_native_work(
            &cursor, default_projection_read_limits(), &auth_work).await.is_err());
        assert_eq!(auth_work.receipt().authority_bytes_read, provider.io_snapshot().read_bytes);
        assert!(auth_work.receipt().authority_bytes_read > 0);
        assert_eq!(auth_work.receipt().output_bytes, 0);
        drop(wrong_reader);
        drop(wrong_custody);

        let cancelled = CancellationToken::new();
        let cancelled_work = NativeMaterializationWork::new(budget(), &cancelled)
            .expect("original cancellation ledger");
        cancelled.cancel();
        provider.reset_io();
        assert!(matches!(reader.pin_with_native_work(64, &cancelled_work).await,
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, custody: None })));
        assert!(matches!(reader.read_page_with_native_work(
            &cursor, default_projection_read_limits(), &cancelled_work).await,
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, custody: None })));
        let sdk = provider.io_snapshot();
        assert_eq!(sdk.reads + sdk.tails as u64, 0);
        assert_eq!(cancelled_work.receipt().output_bytes, 0);
        drop(cursor);
        drop(cursor_custody);
        drop(reader);
        drop(reader_custody);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_checkpoint_open_preserves_actual_output_and_failed_source_prefixes() -> Result<()> {
        use acyclic_fs::{NativeMaterializationWork, NativeTreeMaterializationError};
        let (provider, client) = fixture().await?;
        let cancellation = CancellationToken::new();
        let expected = StreamAggregate::open(
            &client, authority(), issuer().verifier(), schemas()).await?.reducer().snapshot()?;
        let measured = StreamAggregate::open_measured(
            &client, authority(), issuer().verifier(), schemas(), budget(), &cancellation)
            .await.map_err(|failure| failure.error)?;
        let expected_output = measured.work.output_bytes;
        let expected_items = measured.work.items_returned;
        drop(measured);
        let work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original finite open ledger");
        let accounting_bytes = work.receipt().peak_allocation_bytes;
        provider.reset_io();
        let (aggregate, custody) = StreamAggregate::open_with_native_work(
            &client, authority(), issuer().verifier(), schemas(), &work).await?;
        assert_eq!(aggregate.reducer().snapshot()?, expected);
        assert!(work.shares_ledger(custody.work()));
        let receipt = work.receipt();
        let sdk = provider.io_snapshot();
        assert_eq!(receipt.backend_read_operations, sdk.reads + sdk.tails as u64);
        assert_eq!(receipt.authority_bytes_read, sdk.read_bytes);
        assert_eq!(receipt.output_bytes, expected_output);
        assert_eq!(receipt.items_returned, expected_items);
        assert_eq!(sdk.writes + sdk.inspections + sdk.commit_reads + sdk.other_calls, 0);
        drop(custody);
        let all_payload_allowance = budget().peak_allocation_bytes - accounting_bytes;
        assert!(work.admit_retained(all_payload_allowance).is_err());
        drop(aggregate);
        assert!(work.admit_retained(all_payload_allowance).is_ok());
        let exact_work = NativeMaterializationWork::new(receipt, &cancellation)
            .expect("actual native high-water allowance includes accounting state");
        provider.reset_io();
        let (repeated, repeated_custody) = StreamAggregate::open_with_native_work(
            &client, authority(), issuer().verifier(), schemas(), &exact_work).await?;
        assert_eq!(repeated.reducer().snapshot()?, expected);
        assert_eq!(exact_work.receipt(), receipt);
        drop(repeated);
        drop(repeated_custody);
        let denied_copy = NativeMaterializationWork::new(
            WorkBudget { bytes_copied: 0, ..budget() }, &cancellation)
            .expect("original clone-denied ledger");
        provider.reset_io();
        assert!(matches!(StreamAggregate::open_with_native_work(
            &client, authority(), issuer().verifier(), schemas(), &denied_copy).await,
            Err(Error::NativeWork { custody: None, .. })));
        let sdk = provider.io_snapshot();
        assert_eq!(sdk.reads + sdk.tails as u64, 0);
        assert_eq!(denied_copy.receipt().bytes_copied, 0);
        for narrow in [
            WorkBudget { output_bytes: 0, ..budget() },
            WorkBudget { authority_bytes_read: 0, ..budget() },
        ] {
            let denied = NativeMaterializationWork::new(narrow, &cancellation)
                .expect("original denied open ledger");
            provider.reset_io();
            assert!(StreamAggregate::open_with_native_work(
                &client, authority(), issuer().verifier(), schemas(), &denied).await.is_err());
            assert_eq!(denied.receipt().authority_bytes_read, provider.io_snapshot().read_bytes);
            assert!(denied.receipt().authority_bytes_read > 0);
            assert_eq!(denied.receipt().output_bytes, 0);
            assert_eq!(denied.receipt().items_returned, 0);
        }
        let wrong_issuer = AuthorityIssuer::new("test", [8; 32], authority());
        let auth_work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("original authentication ledger");
        provider.reset_io();
        assert!(StreamAggregate::open_with_native_work(
            &client, authority(), wrong_issuer.verifier(), schemas(), &auth_work).await.is_err());
        assert_eq!(auth_work.receipt().authority_bytes_read, provider.io_snapshot().read_bytes);
        assert!(auth_work.receipt().authority_bytes_read > 0);
        assert_eq!(auth_work.receipt().output_bytes, 0);
        let cancelled = CancellationToken::new();
        let cancelled_work = NativeMaterializationWork::new(budget(), &cancelled)
            .expect("original cancelled open ledger");
        cancelled.cancel();
        provider.reset_io();
        assert!(matches!(StreamAggregate::open_with_native_work(
            &client, authority(), issuer().verifier(), schemas(), &cancelled_work).await,
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, custody: None })));
        let sdk = provider.io_snapshot();
        assert_eq!(sdk.reads + sdk.tails as u64, 0);
        assert_eq!(cancelled_work.receipt().output_bytes, 0);
        Ok(())
    }
    #[tokio::test]
    async fn measured_checkpoint_faults_and_issuer_tamper_keep_failed_prefixes() -> Result<()> {
        let (provider, client) = fixture().await?;
        let canonical_path = authority().stream_path()?;
        let path = acyclic_stream::StreamPath::new(format!("harness/v3/projection-checkpoints/{}", canonical_path.strip_prefix("harness/v2/").expect("canonical authority")))?;
        let head_bytes = operations::one_record(&client, &path, 0, &mut None).await?.expect("checkpoint head").value.len() as u64;
        let cancellation = CancellationToken::new();
        for (fault, records, raw_bytes) in [
            (1, 1, head_bytes),
            (2, 2, 2 * head_bytes),
            (3, 0, 0),
            (4, 1, 7),
            (5, 1, acyclic_stream::MAX_RECORD_BYTES as u64 + 1),
            (6, 1, head_bytes),
        ] {
            provider.reset_io();
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            let failure = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.err().expect("mutated durable checkpoint read must reject");
            assert_eq!(failure.work.authority_records_read, records);
            assert_eq!(failure.work.authority_bytes_read, raw_bytes);
            assert_eq!(failure.work.source_bytes_read, raw_bytes);
            assert_eq!(failure.work.output_bytes, 0);
            assert_eq!(failure.work.items_returned, 0);
            assert_eq!(provider.io_snapshot().writes, 0);
            assert_eq!(StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.map_err(|failure| failure.error)?.value.reducer().revision(), 69);
        }
        provider.reset_io();
        let wrong_issuer = AuthorityIssuer::new("test", [8; 32], authority());
        let failure = StreamAggregate::open_measured(&client, authority(), wrong_issuer.verifier(), schemas(), budget(), &cancellation).await.err().expect("checkpoint canonical anchor must retain original issuer authentication");
        assert_eq!(failure.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(failure.work.items_returned, 0);
        provider.reset_io();
        let failure = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), SchemaRegistry::new(), budget(), &cancellation).await.err().expect("decoded checkpoint must still authenticate its schema registry");
        assert_eq!(failure.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(failure.work.items_returned, 0);
        assert_eq!(provider.io_snapshot().writes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn measured_checkpoint_cancellation_stops_before_next_sdk_call() -> Result<()> {
        let (provider, client) = fixture().await?;
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let uncancelled = CancellationToken::new();
        for (work_budget, token) in [(budget(), &cancelled), (WorkBudget::UNBOUNDED, &uncancelled)] {
            provider.reset_io();
            let failure = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), work_budget, token).await.err().expect("invalid preflight must not touch provider");
            assert_eq!(*failure.work, WorkCounters::default());
            assert_eq!(provider.io_snapshot().reads + provider.io_snapshot().tails as u64, 0);
        }
        let cancellation = CancellationToken::new();
        *provider.history_cancel_after_tail.lock().map_err(|_| Error::Storage("cancellation control poisoned".into()))? = Some(cancellation.clone());
        provider.reset_io();
        let failure = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.err().expect("cancellation after cut must precede checkpoint read");
        assert_eq!(failure.work.authority_records_read, 0);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(provider.io_snapshot().reads, 0);
        provider.reset_io();
        let other = Authority { id: "other".into(), ..authority() };
        let failure = StreamAggregate::open_measured(&client, other, issuer().verifier(), schemas(), budget(), &CancellationToken::new()).await.err().expect("wrong audience must fail before provider IO");
        assert!(matches!(failure.error, Error::Unauthorized(_)));
        assert_eq!(*failure.work, WorkCounters::default());
        let empty_provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let empty = StreamClient::new(empty_provider.clone());
        let receipt = StreamAggregate::open_measured(&empty, authority(), issuer().verifier(), schemas(), budget(), &CancellationToken::new()).await.map_err(|failure| failure.error)?;
        assert_eq!(receipt.value.reducer().revision(), 0);
        assert_eq!(receipt.work.authority_bytes_read, 0);
        assert_eq!(receipt.work.items_returned, 0);
        assert_eq!(empty_provider.io_snapshot().writes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn measured_cold_retirement_preserves_original_atomic_indexes() -> Result<()> {
        use crate::core::EffectGuarantee;
        use crate::{EffectAttemptId, EffectId};
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let request = content()?;
        let schema_bytes = br#"{"type":"object"}"#;
        let schema = FileRef::new(request.volume().clone(), "effects/schema.json", "generation-1", FileDescriptor::from_bytes(schema_bytes, "application/schema+json")?, "schema.json")?;
        let content_verifier = Arc::new(InteractionContent(BTreeMap::from([
            (request.path().to_owned(), EXTENSION_BYTES.to_vec()),
            (schema.path().to_owned(), schema_bytes.to_vec()),
        ])));
        let mut writer = StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?.with_content_verifier(content_verifier);
        let effect_id = EffectId::from_bytes([41; 16]);
        let attempt_id = EffectAttemptId::from_bytes([42; 16]);
        let scope = issuer().root("effects", Capabilities::new(["effect:run", "effect:plan", "effect:provider:test.effects"]));
        let fresh = |identity: u8, revision, action| Command {
            operation_id: OperationId::from_bytes([identity; 16]),
            idempotency_key: IdempotencyKey::new(format!("measured-effect-{identity}")).expect("finite key"),
            expected_revision: revision,
            scope: scope.clone(),
            causal_parent: None,
            action,
        };
        writer.execute(fresh(41, 0, Action::PlanEffect { effect_id, provider: "test.effects".into(), guarantee: EffectGuarantee::IdempotentRetry, effect_kind: "test.write".into(), request, result_schema: schema }), #[cfg(feature = "filesystem")] None, &mut None).await?;
        writer.execute(fresh(42, 1, Action::MarkEffectDispatched { effect_id, attempt_id }), #[cfg(feature = "filesystem")] None, &mut None).await?;
        let observation = issuer().attest_effect(effect_id, writer.reducer().effect(effect_id).expect("dispatched effect"), attempt_id, EffectStatus::Failed { message: "known failure".into() })?;
        writer.execute(fresh(43, 2, Action::ResolveEffect { observation }), #[cfg(feature = "filesystem")] None, &mut None).await?;
        let expected = writer.reducer().snapshot()?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.reset_io();
        let cancellation = CancellationToken::new();
        let receipt = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.map_err(|failure| failure.error)?;
        assert_eq!(receipt.value.reducer().snapshot()?, expected);
        assert_eq!(receipt.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(receipt.work.backend_read_operations, provider.io_snapshot().reads + provider.io_snapshot().tails as u64);
        assert_eq!(receipt.work.items_returned, 3);
        let reader = HistoryReader::new(&client, &authority(), issuer().verifier())?;
        let read_work = reader.read_page_measured(
            &HistoryCursor { authority: authority(), after_revision: 0, through_revision: 3 },
            default_projection_read_limits(), budget(), &cancellation,
        ).await.map_err(|failure| failure.error)?.work;
        assert!(receipt.work.bytes_encoded > read_work.bytes_encoded);
        assert!(receipt.work.bytes_hashed > read_work.bytes_hashed);
        assert!(receipt.work.peak_allocation_bytes > 0);
        for cut in [1, 3] {
            provider.reset_io();
            let mut work = history::HistoryWork::new(budget(), &cancellation);
            work.preflight()?;
            let result = effect_events(&client, &issuer().verifier(),
                &HistoryCursor { authority: authority(), after_revision: 0, through_revision: cut },
                effect_id, default_projection_read_limits(), &mut Some(&mut work),
            ).await.map(|(events, _)| events);
            let archived = work.finish(result).map_err(|failure| failure.error)?;
            assert_eq!(archived.value, expected.events.iter().take(cut as usize).cloned().collect::<Vec<_>>());
            assert_eq!(archived.work.items_returned, cut);
            assert_eq!(archived.work.authority_bytes_read, provider.io_snapshot().read_bytes);
            assert_eq!(archived.work.backend_read_operations, provider.io_snapshot().reads + provider.io_snapshot().tails as u64);
            assert_eq!(provider.io_snapshot().writes, 0);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let native = acyclic_fs::NativeMaterializationWork::new(budget(), &cancellation)
                .expect("original effect reader ledger");
            provider.reset_io();
            let mut io = history::native_work_io(&native)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let (events, _) = effect_events(&client, &issuer().verifier(),
                &HistoryCursor { authority: authority(), after_revision: 0, through_revision: 3 },
                effect_id, default_projection_read_limits(), &mut Some(&mut io)).await?;
            io.return_staged_output()?;
            let custody = io.into_custody();
            assert_eq!(events, expected.events);
            assert!(native.shares_ledger(custody.work()));
            assert_eq!(native.receipt().items_returned, 3);
            assert_eq!(native.receipt().authority_bytes_read, provider.io_snapshot().read_bytes);
            assert!(native.receipt().output_bytes > 0);
            drop(events);
            drop(custody);
            let cursor = HistoryCursor {
                authority: authority(), after_revision: 0, through_revision: 3,
            };
            let expected_state = reader.effect(
                &cursor, effect_id, default_projection_read_limits()).await?;
            let prefix = native.receipt();
            provider.reset_io();
            let (state, state_custody) = reader.effect_with_native_work(
                &cursor, effect_id, default_projection_read_limits(), &native).await?;
            assert_eq!(state, expected_state);
            assert!(native.shares_ledger(state_custody.work()));
            assert_eq!(native.receipt().authority_bytes_read - prefix.authority_bytes_read,
                provider.io_snapshot().read_bytes);
            drop(state);
            drop(state_custody);
        }
        provider.reset_io();
        let mut work = history::HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        let result = effect_events(&client, &issuer().verifier(),
            &HistoryCursor { authority: authority(), after_revision: 0, through_revision: 3 },
            EffectId::from_bytes([99; 16]), default_projection_read_limits(), &mut Some(&mut work),
        ).await.map(|(events, _)| events);
        let absent = work.finish(result).map_err(|failure| failure.error)?;
        assert!(absent.value.is_empty());
        assert_eq!(absent.work.items_returned, 0);
        assert_eq!(absent.work.output_bytes, 0);
        assert!(absent.work.authority_records_read > 0);
        provider.reset_io();
        let mut work = history::HistoryWork::new(
            WorkBudget { backend_read_operations: 0, ..budget() }, &cancellation);
        work.preflight()?;
        let result = effect_events(&client, &issuer().verifier(),
            &HistoryCursor { authority: authority(), after_revision: 0, through_revision: 3 },
            effect_id, default_projection_read_limits(), &mut Some(&mut work),
        ).await.map(|(events, _)| events);
        let denied = work.finish(result).expect_err("SDK admission must precede even the original tail request");
        assert_eq!(denied.work.backend_read_operations, 0);
        assert_eq!(provider.io_snapshot().reads, 0);
        assert_eq!(provider.io_snapshot().tails, 0);
        let terminal = expected.events.last().expect("original resolved event");
        provider.reset_io();
        let mut work = history::HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        let result = effects::retirement_proof_for_event_with_work(&client, &authority(), terminal, &mut Some(&mut work)).await;
        let proof = work.finish(result).map_err(|failure| failure.error)?;
        assert!(proof.value);
        assert_eq!(proof.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert!(proof.work.bytes_encoded > 0);
        provider.reset_io();
        let mut work = history::HistoryWork::new(WorkBudget { bytes_encoded: 0, ..budget() }, &cancellation);
        work.preflight()?;
        let result = effects::retirement_proof_for_event_with_work(&client, &authority(), terminal, &mut Some(&mut work)).await;
        let failure = work.finish(result).err().expect("original canonical re-encode must admit before owned codec growth");
        assert_eq!(failure.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(failure.work.bytes_encoded, 0);
        assert_eq!(failure.work.output_bytes, 0);
        provider.hide_aggregate_read.store(true, Ordering::SeqCst);
        provider.reset_io();
        let failure = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.err().expect("unavailable original operation index must preserve spent canonical suffix work");
        assert_eq!(failure.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(failure.work.items_returned, 0);
        provider.hide_effect_transition_position.store(3, Ordering::SeqCst);
        provider.reset_io();
        let failure = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.err().expect("missing original terminal effect index cannot retire");
        assert_eq!(failure.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(failure.work.items_returned, 0);
        provider.hide_effect_transition_position.store(0, Ordering::SeqCst);
        provider.reset_io();
        let failure = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), WorkBudget { backend_read_operations: 4, ..budget() }, &cancellation).await.err().expect("retirement proof shares the original work allowance");
        assert_eq!(failure.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.map_err(|failure| failure.error)?.value.reducer().snapshot()?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn measured_snapshot_authentication_observes_original_encoder_and_mac_failures() -> Result<()> {
        let (provider, client) = fixture().await?;
        let snapshot = checkpoints::load_with_work(&client, &authority(), &issuer().verifier(), default_projection_read_limits(), None, &mut None).await?.snapshot.expect("durable checkpoint");
        let expected = Reducer::restore(snapshot.clone_with_work(&mut None)?, issuer().verifier(), schemas())?;
        let cancellation = CancellationToken::new();
        let mut work = history::HistoryWork::new(budget(), &cancellation);
        work.preflight()?;
        provider.reset_io();
        let result = Reducer::restore_with_work(snapshot.clone_with_work(&mut None)?, issuer().verifier(), schemas(), &mut Some(&mut work));
        let receipt = work.finish(result).map_err(|failure| failure.error)?;
        assert_eq!(receipt.value, expected);
        assert!(receipt.work.bytes_encoded > 0);
        assert!(receipt.work.bytes_hashed > 0);
        assert!(receipt.work.peak_allocation_bytes > 0);
        assert_eq!(receipt.work.authority_bytes_read, 0);
        assert_eq!(receipt.work.backend_read_operations, 0);
        assert_eq!(receipt.work.source_bytes_read, 0);
        for narrow in [
            WorkBudget { bytes_encoded: 0, ..budget() },
            WorkBudget { bytes_hashed: 0, ..budget() },
            WorkBudget { peak_allocation_bytes: 0, ..budget() },
        ] {
            let mut work = history::HistoryWork::new(narrow, &cancellation);
            work.preflight()?;
            let result = Reducer::restore_with_work(snapshot.clone_with_work(&mut None)?, issuer().verifier(), schemas(), &mut Some(&mut work));
            let failure = work.finish(result).err().expect("original snapshot crypto shares the cumulative work allowance");
            if narrow.bytes_encoded == 0 { assert_eq!(failure.work.bytes_encoded, 0); }
            if narrow.peak_allocation_bytes == 0 { assert_eq!(failure.work.peak_allocation_bytes, 0); }
            if narrow.bytes_hashed == 0 {
                assert!(failure.work.bytes_encoded > 0);
                assert!(failure.work.peak_allocation_bytes > 0);
                assert_eq!(failure.work.bytes_hashed, 0);
            }
            assert_eq!(failure.work.authority_bytes_read, 0);
            assert_eq!(failure.work.backend_read_operations, 0);
            assert_eq!(failure.work.output_bytes, 0);
        }
        let mut forged = snapshot.clone_with_work(&mut None)?;
        forged.attestation[0] ^= 1;
        assert!(matches!(Reducer::restore(forged.clone_with_work(&mut None)?, issuer().verifier(), schemas()), Err(Error::Unauthorized(_))));
        let mut work = history::HistoryWork::new(budget(), &cancellation);
        let result = Reducer::restore_with_work(forged, issuer().verifier(), schemas(), &mut Some(&mut work));
        let failure = work.finish(result).err().expect("original snapshot MAC must reject even after its state digest verifies");
        assert!(matches!(failure.error, Error::Unauthorized(_)));
        assert!(failure.work.bytes_encoded > 0);
        assert!(failure.work.bytes_hashed > 0);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(failure.work.items_returned, 0);
        let mut forged = snapshot;
        forged.state_digest[0] ^= 1;
        let mut work = history::HistoryWork::new(budget(), &cancellation);
        let result = Reducer::restore_with_work(forged, issuer().verifier(), schemas(), &mut Some(&mut work));
        let failure = work.finish(result).err().expect("original complete projection digest must reject");
        assert!(matches!(failure.error, Error::Invalid(_)));
        assert!(failure.work.bytes_encoded > 0);
        assert!(failure.work.bytes_hashed > 0);
        assert_eq!(failure.work.output_bytes, 0);
        assert_eq!(provider.io_snapshot().reads + provider.io_snapshot().tails as u64, 0);
        Ok(())
    }

    #[tokio::test]
    async fn exact_historical_open_keeps_admitted_origin_after_later_parent_appends() -> Result<()> {
        let (provider, client) = fixture().await?;
        let limits = default_projection_read_limits();
        let original = StreamAggregate::open_at_revision_with_read_limits(&client, authority(), issuer().verifier(), schemas(), 65, limits).await?.reducer().snapshot()?;
        provider.forbid_writes.store(false, Ordering::SeqCst);
        let mut writer = with_content(StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?);
        for revision in 69..134 {
            writer.execute(projection_command(u128::from(revision + 1), revision)?, #[cfg(feature = "filesystem")] None, &mut None).await?;
        }
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.stale_projection_head_tail.store(2, Ordering::SeqCst);
        let cancellation = CancellationToken::new();
        provider.reset_io();
        let admitted = StreamAggregate::open_at_revision_measured(&client, authority(), issuer().verifier(), schemas(), 65, limits, budget(), &cancellation).await.map_err(|failure| failure.error)?;
        assert_eq!(admitted.value.reducer().snapshot()?, original);
        assert_eq!(admitted.value.reducer().revision(), 65);
        assert_eq!(admitted.work.items_returned, 2);
        assert_eq!(admitted.work.authority_bytes_read, provider.io_snapshot().read_bytes);
        assert_eq!(provider.io_snapshot().writes, 0);
        assert_eq!(provider.stale_projection_head_tail.load(Ordering::SeqCst), 2);
        provider.stale_projection_head_tail.store(0, Ordering::SeqCst);
        for cut in [0_u64, 63, 64, 127, 128, 134] {
            provider.reset_io();
            let historical = StreamAggregate::open_at_revision_measured(&client, authority(), issuer().verifier(), schemas(), cut, limits, budget(), &cancellation).await.map_err(|failure| failure.error)?;
            assert_eq!(historical.value.reducer().revision(), cut);
            let cached = if cut < 64 { cut as usize } else { (cut % 64 + 1) as usize };
            assert_eq!(historical.value.reducer().snapshot()?.events.len(), cached);
            let io = provider.io_snapshot();
            assert_eq!(historical.work.backend_read_operations, io.reads + io.tails as u64);
            assert_eq!(historical.work.authority_bytes_read, io.read_bytes);
            assert_eq!(io.writes + io.inspections + io.commit_reads + io.other_calls, 0);
        }
        provider.reset_io();
        let future = StreamAggregate::open_at_revision_measured(&client, authority(), issuer().verifier(), schemas(), 135, limits, budget(), &cancellation).await.err().expect("uncommitted original revision cannot use latest projection");
        assert_eq!(future.work.authority_records_read, 0);
        assert_eq!(future.work.output_bytes, 0);
        assert_eq!(provider.io_snapshot().reads, 0);
        provider.reset_io();
        let narrow = StreamAggregate::open_at_revision_measured(&client, authority(), issuer().verifier(), schemas(), 65, HistoryReadLimits { maximum_events: 1, ..limits }, budget(), &cancellation).await.err().expect("exact historical suffix must share original finite event allowance");
        assert_eq!(narrow.work.output_bytes, 0);
        for fault in [3, 4] {
            provider.reset_io();
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            let failed = StreamAggregate::open_at_revision_measured(&client, authority(), issuer().verifier(), schemas(), 65, limits, budget(), &cancellation).await.err().expect("unavailable/tampered original checkpoint cannot fall forward to latest checkpoint");
            assert_eq!(failed.work.output_bytes, 0);
            assert_eq!(failed.work.items_returned, 0);
            assert_eq!(StreamAggregate::open_at_revision_with_read_limits(&client, authority(), issuer().verifier(), schemas(), 65, limits).await?.reducer().snapshot()?, original);
        }
        let exact = StreamAggregate::open_at_revision_measured(&client, authority(), issuer().verifier(), schemas(), 134, limits, budget(), &cancellation).await.map_err(|failure| failure.error)?;
        let latest = StreamAggregate::open_measured(&client, authority(), issuer().verifier(), schemas(), budget(), &cancellation).await.map_err(|failure| failure.error)?;
        assert_eq!(exact.value.reducer().snapshot()?, latest.value.reducer().snapshot()?);
        assert_eq!(exact.work, latest.work);
        Ok(())
    }
}
