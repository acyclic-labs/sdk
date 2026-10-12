use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn keyed_append_native_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        allocation_operations: 4096,
        peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
            + 1_048_576,
        backend_write_operations: 8,
        backend_read_operations: 8,
        authority_records_appended: 8,
        authority_bytes_written: 1024,
        authority_records_read: 8,
        authority_bytes_read: 1024,
        items_examined: 4096,
        source_bytes_read: 1_048_576,
        output_bytes: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_hashed: 1_048_576,
        bytes_encoded: 1_048_576,
        ..acyclic_fs::WorkBudget::default()
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn keyed_append_native_keeps_exact_original_sdk_mismatch_source() {
    let client = Arc::new(MemoryStream::default());
    let stream = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        "keyed-append-native-mismatch",
    )
    .expect("source stream");
    let key = StreamIdempotencyKey::new(OwnedBytes::from_static(b"mismatch-key")).expect("key");
    let operation = OperationId::from_bytes([15; 16]);
    append(
        (&stream, &client),
        [OwnedBytes::from_static(b"original")],
        0,
        Some(0),
        key.clone(),
        operation,
        &mut None,
    )
    .await
    .expect("original append");
    let cancellation = acyclic_fs::CancellationToken::new();
    let work =
        acyclic_fs::NativeMaterializationWork::new(keyed_append_native_budget(), &cancellation)
            .expect("original ledger");
    let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
    let error = append(
        (&stream, &client),
        [OwnedBytes::from_static(b"changed")],
        0,
        Some(0),
        key.clone(),
        operation,
        &mut Some(&mut observer),
    )
    .await
    .expect_err("SDK mismatch");
    assert!(matches!(&error, Error::NativeStream {
        code: wire::ErrorCode::Conflict, source: StreamError::IdempotencyMismatch,
        custody: Some(custody),
    } if custody.bytes() == 0));
    assert_eq!(observer.into_custody().bytes(), 0);
    assert!(matches!(
        append(
            (&stream, &client),
            [OwnedBytes::from_static(b"changed")],
            0,
            Some(0),
            key,
            operation,
            &mut None
        )
        .await,
        Err(Error::Conflict(_))
    ));
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_refresh_keeps_original_serde_error_and_raw_record_prefix() {
    let client = Arc::new(MemoryStream::default());
    let mut coordinator = open(&client).await.expect("original empty coordinator");
    let raw = OwnedBytes::from(
        encode(
            1,
            OperationId::from_bytes([16; 16]),
            "native-invalid-schema",
            *blake3::hash(b"{}").as_bytes(),
            b"{}".to_vec(),
            1,
            &mut None,
        )
        .expect("original envelope producer"),
    );
    let raw_length = raw.len() as u64;
    coordinator
        .stream
        .0
        .append_batch(vec![raw], Some(0), None)
        .await
        .expect("original physical envelope");
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = keyed_append_native_budget();
    budget.peak_allocation_bytes =
        acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 524_288;
    budget.authority_bytes_read = raw_length;
    budget.allocation_operations = 128;
    budget.items_examined = 128;
    budget.bytes_copied = 4096;
    budget.bytes_encoded = 4096;
    budget.bytes_hashed = 2;
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
    let error = coordinator
        .refresh(&mut Some(&mut observer))
        .await
        .expect_err("original typed scheduler Deserialize failure");
    assert!(matches!(&error, Error::NativeDecode {
        code: wire::ErrorCode::Storage, source, custody: Some(guard),
    } if source.is_data() && guard.bytes() > 0));
    assert_eq!(coordinator.revision(), 0);
    assert_eq!(
        observer.into_custody().bytes(),
        0,
        "record and envelope die before observer release"
    );
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_publication_phase_survives_receiver_denial_and_original_ack_recovery() {
    use std::sync::atomic::Ordering;
    for (lost_ack, denied, expected_phase) in [
        (true, "authority_records_read", CommitPhase::Committed),
        (true, "backend_read_operations", CommitPhase::Unknown),
        (true, "none", CommitPhase::Committed),
        (false, "backend_write_operations", CommitPhase::Unknown),
    ] {
        let client = Arc::new(LostSessionAck::<MemoryStream>::default());
        let path = acyclic_stream::StreamPath::new("native-publication-disposition")
            .expect("original publication path");
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = keyed_append_native_budget();
        let expected_tail = u64::from(denied != "backend_write_operations");
        match denied {
            "authority_records_read" => budget.authority_records_read = 0,
            "backend_read_operations" => budget.backend_read_operations = 0,
            "backend_write_operations" => budget.backend_write_operations = 0,
            _ => {}
        }
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("original operation ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        let mut work = Some(&mut io as &mut dyn HistoryIo);
        let (key, retained) = stream_key(
            "native-phase-key".len(),
            format_args!("native-phase-key"),
            &mut work,
        )
        .expect("original key");
        let mut prepared = commit_request(key, retained, &mut work);
        add_condition(
            &mut prepared,
            acyclic_stream::CommitCondition::Tail {
                path: path.clone(),
                expected: 0,
            },
        )
        .expect("original guarded condition");
        add_record(
            &mut prepared,
            path.clone(),
            OwnedBytes::from_static(b"original"),
        )
        .expect("original publication payload");
        client.native_only.store(true, Ordering::SeqCst);
        client.lose_ack.store(lost_ack, Ordering::SeqCst);
        let mut phase = CommitPhase::Uncommitted;
        let result = commit(
            prepared,
            &client,
            Some(OperationId::from_bytes([21; 16])),
            Some(&mut phase),
        )
        .await;
        assert_eq!(phase, expected_phase);
        if denied == "none" {
            let (outcome, bytes) = result.expect("original immutable observation proves commit");
            assert!(matches!(
                &outcome,
                acyclic_stream::CommitOutcome::Committed(_)
            ));
            let quota = CodecQuota::from_retained(&mut work, bytes);
            drop(outcome);
            drop(quota);
        } else {
            assert!(
                matches!(result, Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded { counter: actual, .. },
                ), ..
            }) if actual == denied),
                "preserve the original typed first cause"
            );
        }
        drop(work);
        assert_eq!(
            io.into_custody().bytes(),
            0,
            "payloads retire before their exact claims"
        );
        client.native_only.store(false, Ordering::SeqCst);
        let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), path.as_str())
            .expect("original canonical stream");
        assert_eq!(
            stream.tail().await.expect("actual immutable tail"),
            expected_tail
        );
    }
}
