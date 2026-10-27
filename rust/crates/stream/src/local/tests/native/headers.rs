use super::*;

fn body() -> Result<crate::AppendRecord, StreamError> {
    Ok(crate::AppendRecord {
        value: OwnedBytes::from_static(b"completed"),
        headers: crate::RecordHeaders::new(
            (0..=u8::MAX)
                .take(crate::MAX_RECORD_KEYS)
                .map(|key| OwnedBytes::from(vec![key]))
                .collect(),
            (0..crate::MAX_RECORD_ATTRIBUTES)
                .map(|index| crate::Attribute {
                    name: format!("k{index:02}"),
                    value: OwnedBytes::from_static(b"v"),
                })
                .collect(),
        )?,
    })
}

#[tokio::test]
async fn prepared_memory_and_local_commits_cover_every_late_header_field()
-> Result<(), Box<dyn std::error::Error>> {
    for local in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider: Arc<dyn StreamProvider> = if local {
            Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            Arc::new(MemoryStream::new(MemoryLimits::default()))
        };
        let request = prepared_fixture(provider.as_ref()).await?;
        let body = body()?;
        let meter = Arc::new(Meter::default());
        let packet = provider
            .native_prepare_commit(request, body.payload_bytes(), meter.original())
            .await??;
        let (mut packet, guard) = packet.into_parts();
        let recovery = packet.take_recovery_future().ok_or("missing recovery")?;
        // No new storage or codec admission is possible after issuance.
        let allocations = meter.allocations.load(Ordering::SeqCst);
        meter.deny_allocation.store(true, Ordering::SeqCst);
        meter.deny_codec.store(true, Ordering::SeqCst);
        meter.cancelled.store(true, Ordering::SeqCst);
        let result = packet
            .finish(body.clone())
            .await
            .map_err(|error| format!("prepared header completion failed: {error}"))?
            .map_err(|error| format!("prepared header completion failed: {error}"))?;
        let CommitOutcome::Committed(receipt) = result.value() else {
            return Err("prepared header completion was not committed".into());
        };
        assert_eq!(
            meter.allocations.load(Ordering::SeqCst),
            allocations,
            "prepared header completion allocated after issuance"
        );
        // Inspect immutable facts separately from the compact completion output.
        let envelope = provider.read_commit(receipt.commit_id).await?;
        assert!(receipt.matches_envelope(&envelope));
        let [crate::CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
            return Err("prepared header completion changed family".into());
        };
        let [record] = append.records.as_slice() else {
            return Err("prepared header completion changed record count".into());
        };
        assert_eq!(record.value, body.value);
        assert_eq!(record.headers, body.headers);
        let recovered = recovery.await??;
        let observed = recovered.value().as_ref().ok_or("missing completion")?;
        assert_eq!(
            observed.outcome,
            crate::IdempotencyOutcome::Commit(result.value().clone())
        );
        drop(result);
        drop(recovered);
        drop(guard);
        drop(provider);
        meter.assert_refunded();
    }
    Ok(())
}
