use super::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn historical_commit_lookup_and_snapshot_rebuild_do_not_scan_retained_history()
-> TestResult<()> {
    let provider = MemoryStream::default();
    let path = path("index/commits")?;
    let mut oldest = None;
    let mut maximum = 0;
    for step in 1..=1000 {
        let AppendOutcome::Committed(receipt) = provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"value").into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await?
        else {
            return Err(StreamError::Unavailable.into());
        };
        let oldest = *oldest.get_or_insert(receipt.commit_id);
        if [1, 10, 100, 1000].contains(&step) {
            COMMIT_VISITS.with(|count| count.set(0));
            let started = std::time::Instant::now();
            for _ in 0..100 {
                let envelope = provider.read_commit(oldest).await?;
                assert_eq!(envelope.commit_id, oldest);
            }
            let elapsed = started.elapsed().as_nanos() / 100;
            let examined = COMMIT_VISITS.with(std::cell::Cell::get) / 100;
            maximum = maximum.max(examined);
            eprintln!("historical commit step={step} examined={examined} elapsed_ns={elapsed}");
        }
    }
    // Original reverse scan, independent of the indexed lookup.
    let state = provider.state.read().await;
    let oldest = oldest.ok_or(StreamError::Unavailable)?;
    let mut linear = 0;
    let previous = state
        .retained
        .iter()
        .rev()
        .find_map(|entry| {
            linear += 1;
            entry
                .envelope()
                .filter(|envelope| envelope.commit_id == oldest)
        })
        .ok_or(StreamError::NotFound)?;
    assert_eq!(previous.commit_id, oldest);
    assert_eq!(linear, 1000);
    drop(state);
    let snapshot = provider.encode_state().await;
    let restored = MemoryStream::default();
    COMMIT_VISITS.with(|count| count.set(0));
    let started = std::time::Instant::now();
    restored.install_state(&snapshot).await?;
    let rebuild = COMMIT_VISITS.with(std::cell::Cell::get);
    eprintln!(
        "snapshot commits=1000 examined={rebuild} elapsed_us={}",
        started.elapsed().as_micros()
    );
    assert_eq!(restored.encode_state().await, snapshot);
    assert_eq!(restored.read_commit(oldest).await?.commit_id, oldest);
    assert!(
        maximum <= 257,
        "historical lookup scanned {maximum} retained entries"
    );
    assert!(
        rebuild <= 257 * 1000,
        "snapshot duplicate checks scanned {rebuild} retained entries"
    );
    assert!(linear > 257, "linear negative control was not detected");
    Ok(())
}

#[test]
fn every_commit_key_bit_fits_the_fixed_lookup_and_insertion_reservations() -> TestResult<()> {
    let mut state = State::default();
    let mut keys = Vec::new();
    for bit in 0..=256 {
        let mut key = [0; 32];
        if bit < 256 {
            *key.get_mut(bit / 8).ok_or(StreamError::Unavailable)? = 0x80 >> (bit % 8);
        }
        let commit_id = CommitId::from_bytes(key);
        let envelope = CommittedEnvelope {
            commit_id,
            committed_at_micros: 0,
            mutations: Vec::new(),
        };
        let mut probes = 0;
        retain(
            &mut state,
            &mut || probes += 1,
            None,
            None,
            [0; 32],
            IdempotencyOutcome::Commit(envelope),
            Default::default(),
        );
        assert!(probes <= COMMIT_INDEX_PROBES);
        keys.push(commit_id);
    }
    let mut maximum = 0;
    for key in keys {
        let mut probes = 0;
        let found = state
            .find_commit(&key, || {
                probes += 1;
                Ok::<_, StreamError>(())
            })?
            .ok_or(StreamError::NotFound)?;
        assert_eq!(found.commit_id, key);
        maximum = maximum.max(probes);
        assert!(probes <= 257);
    }
    assert_eq!(
        maximum, 257,
        "the adversarial keys did not span all 256 bits"
    );
    assert!(
        state
            .find_commit(
                &CommitId::from_bytes([255; 32]),
                || Ok::<_, StreamError>(())
            )?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn commit_index_work_is_admitted_before_append_or_coordinated_publication() -> TestResult<()>
{
    use crate::test_meter::Meter;
    for mode in 0..3 {
        let coordinated = mode != 0;
        let provider = MemoryStream::default();
        let destination = path("index/fork")?;
        let path = path("index/admission")?;
        let request = AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"value").into()],
            if_tail: None,
            idempotency_key: None,
        };
        provider.append(request.clone()).await?;
        let snapshot = provider.encode_state().await;
        let denied = Arc::new(Meter::default());
        denied
            .examination_limit
            .store(100, std::sync::atomic::Ordering::SeqCst);
        let commit = indexed_commit(&request, &destination, mode == 2)?;
        let refused = if coordinated {
            provider
                .native_commit(commit.clone(), None, denied.original())
                .await
                .is_err()
        } else {
            provider
                .native_append(request.clone(), denied.original())
                .await
                .is_err()
        };
        assert!(refused, "index publication had no prior work admission");
        assert_eq!(provider.encode_state().await, snapshot);
        assert_eq!(provider.tail(path.clone()).await?, 1);
        assert!(matches!(
            provider.tail(destination.clone()).await,
            Err(StreamError::NotFound)
        ));
        denied.assert_refunded();
        assert_eq!(denied.held.load(std::sync::atomic::Ordering::SeqCst), 0);
        let funded = Arc::new(Meter::default());
        let identity = if coordinated {
            let output = provider
                .native_commit(commit, None, funded.original())
                .await??;
            let CommitOutcome::Committed(envelope) = output.value() else {
                return Err(StreamError::Unavailable.into());
            };
            envelope.commit_id
        } else {
            let output = provider.native_append(request, funded.original()).await??;
            let AppendOutcome::Committed(receipt) = output.value() else {
                return Err(StreamError::Unavailable.into());
            };
            receipt.commit_id
        };
        assert_eq!(provider.read_commit(identity).await?.commit_id, identity);
        let changed = if mode == 2 { destination } else { path };
        assert_eq!(provider.tail(changed).await?, 2);
        drop(provider);
        funded.assert_refunded();
        assert_eq!(funded.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    Ok(())
}

fn indexed_commit(
    request: &AppendRequest,
    destination: &StreamPath,
    fork: bool,
) -> TestResult<CommitRequest> {
    let mutation = if fork {
        CommitMutation::Fork {
            exclude: Vec::new(),
            source: request.path.clone(),
            destination: destination.clone(),
            at_tail: 1,
            records: request.records.clone(),
        }
    } else {
        CommitMutation::Append {
            path: request.path.clone(),
            records: request.records.clone(),
        }
    };
    let mut conditions = vec![CommitCondition::Tail {
        path: request.path.clone(),
        expected: 1,
    }];
    if fork {
        conditions.push(CommitCondition::Absent {
            path: destination.clone(),
        });
    }
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions,
        mutations: vec![mutation],
        idempotency_key: IdempotencyKey::new(Bytes::from_static(b"indexed-commit"))?,
    })
}
