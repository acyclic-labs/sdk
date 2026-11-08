use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn binary_replay_identity_and_work_follow_the_request_through_forks_and_recovery()
-> TestResult<()> {
    let provider = MemoryStream::default();
    let mut keys = vec![vec![0, 0, 0], vec![0], vec![0, 0], vec![255], vec![255, 0]];
    // Every data coordinate plus a complete zero key. The absent short [7]
    // must stop at its own terminator, before this longer subtree's branches.
    for bit in 0..=256 {
        let mut key = vec![0; 33];
        key[0] = 7;
        if bit < 256 {
            key[1 + bit / 8] = 0x80 >> (bit % 8);
        }
        keys.push(key);
    }
    for length in 4..=64 {
        keys.push(vec![0; length]);
    }
    keys.push(vec![0; crate::MAX_IDEMPOTENCY_KEY_BYTES]);
    let mut expected = Vec::new();
    let paths = [
        path("replay/root")?,
        path("replay/child")?,
        path("replay/child/grandchild")?,
    ];
    for (index, bytes) in keys.iter().enumerate() {
        let key = IdempotencyKey::new(Bytes::from(bytes.clone()))?;
        let target = &paths[index % paths.len()];
        let AppendOutcome::Committed(receipt) = provider
            .append(AppendRequest {
                path: target.clone(),
                records: vec![Bytes::from_static(b"value").into()],
                if_tail: None,
                idempotency_key: key.clone(),
            })
            .await?
        else {
            return Err(StreamError::Unavailable.into());
        };
        expected.push((key.clone(), receipt));
        if index == 0 {
            let meter = Arc::new(Meter::default());
            provider
                .native_inspect_idempotency(key, meter.original())
                .await??;
            assert_eq!(
                meter.examined.load(Ordering::SeqCst),
                bytes.len() as u64 + 3,
                "binary replay lookup omitted original comparison admission"
            );
            // One physical node fits, but the complete key comparison does not.
            let denied = Arc::new(Meter::default());
            denied.examination_limit.store(2, Ordering::SeqCst);
            assert!(
                provider
                    .native_inspect_idempotency(expected[0].0.clone(), denied.original())
                    .await
                    .is_err(),
                "binary replay lookup bypassed original comparison refusal"
            );
            denied.assert_refunded();
            // Construct a real fork tree before continuing concurrent histories.
            for (fork, pair) in paths.windows(2).enumerate() {
                provider
                    .fork(ForkRequest {
                        source: pair[0].clone(),
                        destination: pair[1].clone(),
                        at_tail: Some(1),
                        exclude: Vec::new(),
                        idempotency_key: IdempotencyKey::new(Bytes::from(format!("fork/{fork}")))?,
                    })
                    .await?;
            }
        }
    }
    assert!(IdempotencyKey::new(Bytes::new()).is_err());
    let snapshot = provider.encode_state().await;
    let restored = MemoryStream::default();
    restored.install_state(&snapshot).await?;
    assert_eq!(restored.encode_state().await, snapshot);
    // Exact output and request-width cost use the shipped native read boundary,
    // both before and after the real persisted snapshot's index rebuild.
    for stream in [&provider, &restored] {
        for (key, receipt) in &expected {
            let meter = Arc::new(Meter::default());
            let output = stream
                .native_inspect_idempotency(key.clone(), meter.original())
                .await??;
            let observed = output.value().as_ref();
            assert!(
                observed.is_some(),
                "binary replay identity lost its original outcome"
            );
            let observed = observed.ok_or(StreamError::NotFound)?;
            assert_eq!(observed.idempotency_key, *key);
            let IdempotencyOutcome::Append(AppendOutcome::Committed(actual)) = &observed.outcome
            else {
                panic!("binary replay changed its original outcome family")
            };
            assert_eq!(
                (
                    actual.start,
                    actual.end,
                    actual.tail,
                    actual.commit_id,
                    actual.revision.index
                ),
                (
                    receipt.start,
                    receipt.end,
                    receipt.tail,
                    receipt.commit_id,
                    receipt.revision.index
                )
            );
            assert!(
                meter.examined.load(Ordering::SeqCst)
                    <= BinaryKey(key.as_bytes())
                        .lookup_examinations()
                        .ok_or(StreamError::Capacity)?
                        + 1,
                "binary replay lookup escaped the requested key width"
            );
            drop(output);
            meter.assert_refunded();
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        }
        for bytes in [vec![7], vec![7, 0], vec![255, 0, 0], vec![0; 65]] {
            let key = IdempotencyKey::new(Bytes::from(bytes))?;
            let meter = Arc::new(Meter::default());
            let output = stream
                .native_inspect_idempotency(key.clone(), meter.original())
                .await??;
            assert!(
                output.value().is_none(),
                "binary replay aliased an absent prefix or trailing zero"
            );
            assert!(
                meter.examined.load(Ordering::SeqCst)
                    <= BinaryKey(key.as_bytes())
                        .lookup_examinations()
                        .ok_or(StreamError::Capacity)?
                        + 1,
                "binary replay lookup escaped the requested key width"
            );
        }
    }
    Ok(())
}
