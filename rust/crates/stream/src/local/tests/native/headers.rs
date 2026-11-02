use super::*;
use futures::TryStreamExt as _;

fn body() -> Result<crate::AppendRecord, StreamError> {
    Ok(crate::AppendRecord {
        value: OwnedBytes::from_static(b"completed"),
        headers: crate::RecordHeaders::new(
            (0..=u8::MAX)
                .take(129)
                .map(|key| {
                    OwnedBytes::from(match key {
                        0 => vec![0; crate::MAX_RECORD_KEY_BYTES - 1],
                        1 => vec![0; crate::MAX_RECORD_KEY_BYTES],
                        _ => vec![key],
                    })
                })
                .collect(),
            (0..65)
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
        let provider: Arc<dyn crate::NativeStreamProvider> = if local {
            Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
        } else {
            Arc::new(MemoryStream::new(MemoryLimits::default()))
        };
        let request = prepared_fixture(provider.as_ref()).await?;
        let body = body()?;
        let meter = Arc::new(Meter::default());
        let packet = provider
            .native_prepare_commit(
                request,
                crate::PreparedRecordCapacity {
                    payload_bytes: body.payload_bytes(),
                    keys: body.headers.keys().len(),
                    attributes: body.headers.attributes().len(),
                },
                meter.original(),
            )
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
        for key in &body.headers.keys()[..2] {
            let selected: Vec<_> = provider
                .read(ReadRequest {
                    path: append.path.clone(),
                    from: append.start,
                    limit: 1,
                    key: Some(crate::ReadKeySeek {
                        key: key.clone(),
                        before: append.end,
                    }),
                    at_least: Default::default(),
                })
                .await?
                .try_collect()
                .await?;
            assert_eq!(selected.as_slice(), std::slice::from_ref(record));
        }
        let recovered = recovery.await??;
        let observed = recovered.value().as_ref().ok_or("missing completion")?;
        assert_eq!(
            observed.outcome,
            crate::IdempotencyOutcome::Commit(envelope)
        );
        drop(result);
        drop(recovered);
        drop(guard);
        drop(provider);
        meter.assert_refunded();
    }
    Ok(())
}

#[tokio::test]
async fn prepared_recovery_refuses_unreserved_header_counts_before_codec_work()
-> Result<(), Box<dyn std::error::Error>> {
    for local in [false, true] {
        for attributes in [false, true] {
            let directory = tempfile::tempdir()?;
            let provider: Arc<dyn crate::NativeStreamProvider> = if local {
                Arc::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?)
            } else {
                Arc::new(MemoryStream::new(MemoryLimits::default()))
            };
            let mut request = prepared_fixture(provider.as_ref()).await?;
            let meter = Arc::new(Meter::default());
            let packet = provider
                .native_prepare_commit(
                    request.clone(),
                    crate::PreparedRecordCapacity {
                        payload_bytes: 64,
                        keys: 0,
                        attributes: 0,
                    },
                    meter.original(),
                )
                .await??;
            let (mut packet, guard) = packet.into_parts();
            let recovery = packet.take_recovery_future().ok_or("missing recovery")?;
            let keys = if attributes {
                vec![]
            } else {
                (0..16).map(|key| OwnedBytes::from(vec![key])).collect()
            };
            let fields = if attributes {
                (0..16)
                    .map(|index| crate::Attribute {
                        name: format!("k{index:02}"),
                        value: OwnedBytes::from_static(b""),
                    })
                    .collect()
            } else {
                vec![]
            };
            let [crate::CommitMutation::Append { records, .. }] = request.mutations.as_mut_slice()
            else {
                return Err("fixture changed request family".into());
            };
            *records = vec![crate::AppendRecord {
                value: OwnedBytes::from_static(b"v"),
                headers: crate::RecordHeaders::new(keys, fields)?,
            }];
            let unreserved = records.first().ok_or("missing unreserved body")?.clone();
            assert!(matches!(
                provider.commit(request).await?,
                CommitOutcome::Committed(_)
            ));
            let hashed = meter.hashed.load(Ordering::SeqCst);
            let allocations = meter.allocations.load(Ordering::SeqCst);
            meter.deny_allocation.store(true, Ordering::SeqCst);
            meter.deny_codec.store(true, Ordering::SeqCst);
            meter.cancelled.store(true, Ordering::SeqCst);
            assert!(matches!(recovery.await?, Err(StreamError::LimitExceeded)));
            assert_eq!(meter.hashed.load(Ordering::SeqCst), hashed);
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            // Await the original completion as well: Local's accepted writer
            // owns its reservations until the closed ingress has drained.
            assert!(matches!(
                packet.finish(unreserved).await?,
                Err(StreamError::LimitExceeded)
            ));
            assert_eq!(meter.hashed.load(Ordering::SeqCst), hashed);
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            drop(guard);
            drop(provider);
            meter.assert_refunded();
        }
    }
    Ok(())
}
