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
            path: path.clone(),
            from: sequence,
            limit: 1,
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
                path: path.clone(),
                from: sequence,
                limit: 1,
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
    assert!(meter.examined.load(Ordering::SeqCst) <= 64);
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
                path: path.clone(),
                from: from as u64,
                limit: 3,
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
                    path: path.clone(),
                    from: from as u64,
                    limit: 3,
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
async fn jumps_preserve_nested_fork_cuts_inside_batches_and_recovery() -> TestResult<()> {
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
async fn deriving_a_jump_admits_each_ancestor_before_examining_it() -> TestResult<()> {
    let provider = MemoryStream::default();
    let path = path("indexed/admission")?;
    for _ in 0..16 {
        append_batch(&provider, &path, vec![Bytes::from_static(b"value")]).await?;
    }
    let state = provider.state.read().await;
    let root = state
        .paths
        .get(&path)
        .and_then(|path| path.history.as_ref())
        .ok_or(StreamError::NotFound)?;
    let references = Arc::strong_count(root);
    let mut visits = 0;
    HISTORY_METADATA_READS.with(|count| count.set(0));
    let jump = HistoryJump::new(Some(root), 16, || {
        let reads = HISTORY_METADATA_READS.with(|count| count.replace(0));
        // The first admission must precede every metadata read. Afterwards
        // only the node admitted on the preceding call may be inspected:
        // its ordinal once, plus the initial parent ordinal on the first hop.
        let allowed = match visits {
            0 => 0,
            1 => 2,
            _ => 1,
        };
        assert!(
            reads <= allowed,
            "unadmitted history metadata: {reads} > {allowed}"
        );
        visits += 1;
        Ok(())
    })?;
    assert!(visits > 1);
    assert_eq!(jump.ordinal, 16);
    assert_eq!(
        jump.target.as_ref().map(|target| target.jump().ordinal),
        Some(0)
    );
    drop(jump);
    for admitted in 0..visits {
        let mut remaining = admitted;
        HISTORY_METADATA_READS.with(|count| count.set(0));
        let refused = HistoryJump::new(Some(root), 16, || {
            if remaining == 0 {
                return Err(StreamError::Capacity);
            }
            remaining -= 1;
            Ok(())
        });
        assert!(matches!(refused, Err(StreamError::Capacity)));
        if admitted == 0 {
            assert_eq!(HISTORY_METADATA_READS.with(std::cell::Cell::get), 0);
        }
        assert_eq!(Arc::strong_count(root), references);
    }
    drop(state);
    assert_eq!(provider.tail(path).await?, 16);
    Ok(())
}
