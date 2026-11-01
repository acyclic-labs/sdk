use super::*;
use crate::test_meter::Meter;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

async fn append_batch(
    provider: &MemoryStream,
    path: &StreamPath,
    values: Vec<Bytes>,
) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: values.into_iter().map(Into::into).collect(),
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    Ok(())
}

async fn point_read(
    provider: &MemoryStream,
    path: &StreamPath,
    sequence: u64,
) -> TestResult<usize> {
    HISTORY_VISITS.with(|count| count.set(0));
    let records = provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: sequence,
            limit: 1,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 1);
    let expected = records.first().ok_or(StreamError::Unavailable)?;
    assert_eq!(expected.sequence, sequence);
    assert_eq!(expected.value.as_ref(), b"value");
    let visits = HISTORY_VISITS.with(std::cell::Cell::get);
    let meter = Arc::new(Meter::default());
    let mut output = provider
        .native_read(
            ReadRequest {
                key: None,
                path: path.clone(),
                from: sequence,
                limit: 1,
                at_least: Default::default(),
            },
            meter.original(),
        )
        .await??;
    let record = output
        .value_mut()
        .next()
        .await
        .ok_or(StreamError::Unavailable)???;
    assert_eq!(record.value(), expected);
    assert!(output.value_mut().next().await.is_none());
    // The position index charges key-byte comparisons as well as node visits;
    // the original namespace/open/output add four examinations.
    assert!(meter.examined.load(Ordering::SeqCst) <= 4 + 65 * 58);
    drop(record);
    drop(output);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(visits)
}

// Independent negative control: the original parent walk visits every newer
// append before reaching an old point. Do not use HistoryWalk here.
async fn linear_visits(provider: &MemoryStream, path: &StreamPath, ceiling: u64) -> usize {
    let state = provider.state.read().await;
    let mut cursor = state.paths.get(path).and_then(|path| path.history.as_ref());
    let mut visited = 0;
    while let Some(history) = cursor {
        visited += 1;
        if history.boundary() < ceiling {
            break;
        }
        cursor = history.parent();
    }
    visited
}

#[tokio::test]
async fn old_point_reads_have_bounded_work_from_one_to_a_thousand_appends() -> TestResult<()> {
    let provider = MemoryStream::default();
    let path = path("indexed/history")?;
    let mut maximum = 0;
    for step in 0..1000 {
        append_batch(&provider, &path, vec![Bytes::from_static(b"value")]).await?;
        // Positions zero and one have a separate prefix cache. Position two
        // exercises history traversal after the initial three appends.
        let sequence = step.min(2);
        maximum = maximum.max(point_read(&provider, &path, sequence).await?);
        assert!(maximum <= 64, "history nodes: {maximum} at {step}");
    }
    assert_eq!(linear_visits(&provider, &path, 3).await, 998);
    assert!(linear_visits(&provider, &path, 3).await > 64);
    let restored = MemoryStream::default();
    let snapshot = provider.encode_state().await;
    restored.install_state(&snapshot).await?;
    assert_eq!(restored.encode_state().await, snapshot);
    assert!(point_read(&restored, &path, 2).await? <= 64);
    eprintln!("old-point history visits: maximum={maximum}, linear=998");
    Ok(())
}

async fn assert_records(
    provider: &MemoryStream,
    path: &StreamPath,
    expected: &[Bytes],
) -> TestResult<()> {
    for from in 0..expected.len() {
        let actual = provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from: from as u64,
                limit: 3,
                at_least: Default::default(),
            })
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let end = expected.len().min(from + 3);
        assert_eq!(
            actual
                .iter()
                .map(|record| &record.value)
                .collect::<Vec<_>>(),
            expected
                .get(from..end)
                .ok_or(StreamError::Unavailable)?
                .iter()
                .collect::<Vec<_>>()
        );
        let meter = Arc::new(Meter::default());
        let mut output = provider
            .native_read(
                ReadRequest {
                    key: None,
                    path: path.clone(),
                    from: from as u64,
                    limit: 3,
                    at_least: Default::default(),
                },
                meter.original(),
            )
            .await??;
        for expected in actual {
            let record = output
                .value_mut()
                .next()
                .await
                .ok_or(StreamError::Unavailable)???;
            assert_eq!(record.value(), &expected);
        }
        assert!(output.value_mut().next().await.is_none());
        drop(output);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn indexed_ranges_preserve_nested_fork_cuts_inside_batches_and_recovery() -> TestResult<()> {
    let provider = MemoryStream::default();
    let mut source = path("indexed/source")?;
    let mut expected = (0..23)
        .map(|index| Bytes::from(format!("original-{index}")))
        .collect::<Vec<_>>();
    append_batch(&provider, &source, expected.clone()).await?;
    let mut paths = vec![(source.clone(), expected.clone())];
    for depth in 0..32 {
        let destination = path(&format!("indexed/fork-{depth}"))?;
        let cut = (depth * 7 + 3) % expected.len();
        provider
            .fork(ForkRequest {
                exclude: Vec::new(),
                source,
                destination: destination.clone(),
                at_tail: Some(cut as u64),
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
        expected.truncate(cut);
        let suffix = (0..17)
            .map(|index| Bytes::from(format!("{depth}-{index}")))
            .collect::<Vec<_>>();
        expected.extend(suffix.iter().cloned());
        append_batch(&provider, &destination, suffix).await?;
        source = destination;
        paths.push((source.clone(), expected.clone()));
    }
    let restored = MemoryStream::default();
    restored
        .install_state(&provider.encode_state().await)
        .await?;
    for (path, expected) in paths {
        assert_records(&provider, &path, &expected).await?;
        assert_records(&restored, &path, &expected).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pinned_ranges_preserve_cold_bounds_cuts_and_original_admission() -> TestResult<()> {
    let provider = MemoryStream::default();
    let original = path("indexed/admission")?;
    let values: Vec<_> = (0..23)
        .map(|i| Bytes::from(format!("original-{i}")))
        .collect();
    append_batch(&provider, &original, values.clone()).await?;
    let mut selected = original.clone();
    let mut costs = Vec::new();
    for depth in 1..=256 {
        let destination = path(&format!("indexed/cut-{depth}"))?;
        provider
            .fork(ForkRequest {
                source: selected,
                destination: destination.clone(),
                at_tail: Some(3),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        selected = destination;
        if [1, 32, 256].contains(&depth) {
            let captured = provider
                .state
                .read()
                .await
                .paths
                .get(&selected)
                .cloned()
                .unwrap();
            let mut pinned = PinnedHistory::new(&captured, captured.tail);
            let mut items = 0;
            let mut window = None;
            let allocation = acyclic_test_support::measure(|| {
                window = pinned
                    .page(2, 64, |count| {
                        items += count;
                        Ok::<_, StreamError>(())
                    })
                    .unwrap();
            });
            let window = window.unwrap();
            assert_eq!(window.end - window.start, 1);
            assert_eq!(window.records.records[window.start].value, values[2]);
            assert_eq!(window.records.records[window.start].origin, original);
            assert_eq!(allocation.count_total, 0);
            assert!(items <= 1 + 65 * 58);
            costs.push(items);
            assert_eq!(linear_visits(&provider, &selected, 3).await, depth + 1);
        }
    }
    assert!(costs.windows(2).all(|pair| pair[0] == pair[1]));
    let captured = provider
        .state
        .read()
        .await
        .paths
        .get(&selected)
        .cloned()
        .unwrap();
    let mut pinned = PinnedHistory::new(&captured, captured.tail);
    HISTORY_METADATA_READS.with(|count| count.set(0));
    let refused = pinned.page(2, 64, |_| Err(StreamError::Capacity));
    assert!(matches!(refused, Err(StreamError::Capacity)));
    assert_eq!(HISTORY_METADATA_READS.with(std::cell::Cell::get), 0);

    // The fork cut falls inside the original batch. Growing the selected path
    // cannot reveal that batch's discarded suffix through the inherited leaf.
    let suffix: Vec<_> = (0..17)
        .map(|i| Bytes::from(format!("selected-{i}")))
        .collect();
    append_batch(&provider, &selected, suffix.clone()).await?;
    append_batch(&provider, &selected, vec![Bytes::from_static(b"later")]).await?;
    let mut expected = values[..3].to_vec();
    expected.extend(suffix);
    expected.push(Bytes::from_static(b"later"));
    assert_records(&provider, &selected, &expected).await?;
    let old = pinned.page(0, 64, |_| Ok::<_, StreamError>(()))?.unwrap();
    assert_eq!(old.end - old.start, 3);
    assert!(pinned.page(3, 64, |_| Ok::<_, StreamError>(()))?.is_none());

    let state = provider.state.read().await;
    let selected_state = state.paths.get(&selected).unwrap();
    let History::Batch { records, .. } = selected_state.history.as_deref().unwrap() else {
        unreachable!()
    };
    // A cut before (or exactly at) the current batch needs only the original
    // Prefix owner. Its unreadable batch must not consume index funding or
    // make a valid fork fail when that additional capacity is unavailable.
    for cut in [0, 3, 20] {
        let mutations = [CommitMutation::Fork {
            source: selected.clone(),
            destination: path("indexed/funded-prefix")?,
            at_tail: cut,
            exclude: Vec::new(),
            records: Vec::new(),
        }];
        let (facts, _facts) = provider.facts(&state, &mutations, CommitId::default(), 0)?;
        let meter = Arc::new(Meter::default());
        let admitted = provider.with_context(Work::new(meter.original())?);
        let allocations = meter.allocations.load(Ordering::SeqCst);
        let held = meter.held.load(Ordering::SeqCst);
        meter.limit.store(allocations + 1, Ordering::SeqCst);
        let prepared = admitted.history(&state, &facts[0])?;
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations + 1);
        drop(prepared);
        assert_eq!(meter.held.load(Ordering::SeqCst), held);
        drop(admitted);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    for allowance in 0..=3 {
        let meter = Arc::new(Meter::default());
        let work = Work::new(meter.original())?;
        let admitted = provider.with_context(work);
        let held = meter.held.load(Ordering::SeqCst);
        meter.limit.store(
            meter.allocations.load(Ordering::SeqCst) + allowance,
            Ordering::SeqCst,
        );
        let next = selected_state.ranges.insert(
            20_u64.to_be_bytes(),
            Arc::clone(records),
            &mut HistoryIndexWork(&admitted),
        );
        if allowance == 0 {
            assert!(next.is_err());
        }
        drop(next);
        assert_eq!(meter.held.load(Ordering::SeqCst), held);
        drop(admitted);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }

    // The same production preparation still binds the current batch uniquely.
    let mutations = [CommitMutation::Append {
        path: selected.clone(),
        records: vec![Bytes::from_static(b"prepared").into()],
    }];
    let (facts, _facts) = provider.facts(&state, &mutations, CommitId::default(), 0)?;
    let (mut histories, _histories) = provider.histories(&state, &facts)?;
    bind_histories(&mut histories, CommitId::from_bytes([7; 32]), 42)?;
    let History::Batch { records, .. } = histories[0].history.as_ref() else {
        unreachable!()
    };
    assert_eq!(Arc::strong_count(records), 1);
    assert_eq!(records.records[0].committed_at_micros, 42);
    drop(state);
    let restored = MemoryStream::default();
    restored
        .install_state(&provider.encode_state().await)
        .await?;
    assert_records(&restored, &selected, &expected).await?;
    println!(
        "cold inherited position depth=[1,32,256] comparison_work={costs:?}; ancestry visits=[2,33,257]; page allocations=0"
    );
    Ok(())
}
