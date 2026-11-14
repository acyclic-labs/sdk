//! Stateful simulation over the real journal, snapshot and reader paths.
//!
//! After every transition: acknowledged records survive recovery, histories have
//! contiguous sequences and exact payloads, forks retain their selected prefix,
//! conditional competitors have one winner, follow has no gaps, and bounded
//! discovery enumerates every child exactly once. A failed physical write may
//! leave its whole record or nothing, never a partial logical record.
use super::super::*;
use acyclic_native_runtime::testing::{DurableFault, Schedule, fill_seeded, seeded_runtime};
use std::collections::BTreeMap;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type History = BTreeMap<String, Vec<Vec<u8>>>;

fn append(path: &str, tail: usize, value: Vec<u8>) -> Result<AppendRequest> {
    Ok(AppendRequest {
        path: StreamPath::new(path)?,
        records: vec![value.into()],
        if_tail: Some(tail.try_into()?),
        idempotency_key: IdempotencyKey::unique(),
    })
}

async fn records(provider: &LocalStream, path: &str) -> Result<Vec<Vec<u8>>> {
    let mut stream = provider
        .read(ReadRequest {
            key: None,
            path: StreamPath::new(path)?,
            from: 0,
            limit: u32::MAX,
            at_least: Default::default(),
        })
        .await?;
    let mut values = Vec::new();
    while let Some(record) = stream.next().await {
        let record = record?;
        assert_eq!(
            record.sequence,
            values.len() as u64,
            "noncontiguous history: {path}"
        );
        values.push(record.value.as_slice().to_vec());
    }
    Ok(values)
}

async fn invariant(provider: &LocalStream, expected: &History) -> Result {
    for (path, values) in expected {
        assert_eq!(
            provider.tail(StreamPath::new(path)?).await?,
            values.len() as u64
        );
        assert_eq!(
            &records(provider, path).await?,
            values,
            "history changed: {path}"
        );
    }
    let mut after = None;
    let mut fence = None;
    let mut found = Vec::new();
    loop {
        let request = crate::ChildrenPageRequest {
            parent: Some(StreamPath::new("tree")?),
            tail_stream: None,
            after,
            hierarchy_version: fence,
            limit: 2,
        };
        let page = provider.children_page(request.clone()).await?;
        page.validate_for(&request)?;
        fence = Some(page.hierarchy_version);
        found.extend(
            page.children
                .iter()
                .map(|child| child.path.as_str().to_owned()),
        );
        match page.next_after {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(found, expected.keys().cloned().collect::<Vec<_>>());
    Ok(())
}

async fn world(seed: u64, steps: usize) -> Result {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        compaction_operations: 4,
        ..Default::default()
    };
    let mut provider = LocalStream::open(directory.path(), limits).await?;
    let mut expected = History::new();
    // Draw the operation tape from the shared core, not a subsystem RNG. Keep
    // it independent of how much entropy the real implementation consumes.
    let (plan, _) = Schedule::seeded(seed);
    let mut tape = vec![0; steps * 3];
    plan.scope(async {
        assert!(fill_seeded(&mut tape));
    })
    .await;
    for (step, choice) in tape.chunks_exact(3).enumerate() {
        let (schedule, mut events) = Schedule::seeded(seed.wrapping_add(step as u64));
        let path = if expected.is_empty() {
            "tree/root".to_owned()
        } else {
            expected
                .keys()
                .nth(choice[1] as usize % expected.len())
                .unwrap()
                .clone()
        };
        let tail = expected.get(&path).map_or(0, Vec::len);
        let value = (step as u64).to_le_bytes().to_vec();
        let operation = if tail == 0 { 0 } else { choice[0] % 5 };
        let result: Result = super::journal_lifecycle(&schedule, &mut events, async {
            match operation {
                1 if tail > 0 => {
                    let destination = format!("tree/fork{step}");
                    let cut = choice[2] as usize % (tail + 1);
                    provider
                        .fork(ForkRequest {
                            source: StreamPath::new(&path)?,
                            destination: StreamPath::new(&destination)?,
                            at_tail: Some(cut as u64),
                            exclude: Vec::new(),
                            idempotency_key: IdempotencyKey::unique(),
                        })
                        .await?;
                    expected.insert(destination, expected[&path][..cut].to_vec());
                }
                2 => {
                    let first = append(&path, tail, value.clone())?;
                    let mut other = value.clone();
                    other.push(1);
                    let second = append(&path, tail, other.clone())?;
                    let (a, b) = tokio::join!(provider.append(first), provider.append(second));
                    let a = matches!(a?, AppendOutcome::Committed(_));
                    let b = matches!(b?, AppendOutcome::Committed(_));
                    assert_ne!(a, b, "conditional competitors must have exactly one winner");
                    expected.entry(path.clone()).or_default().push(if a {
                        value.clone()
                    } else {
                        other
                    });
                }
                3 if tail > 0 => {
                    use futures::TryFutureExt as _;
                    let mut follow = provider
                        .follow(crate::FollowRequest::new(
                            StreamPath::new(&path)?,
                            tail as u64,
                        ))
                        .map_ok(crate::records_only)
                        .await?;
                    let request = append(&path, tail, value.clone())?;
                    let (written, observed) = tokio::join!(provider.append(request), follow.next());
                    assert!(matches!(written?, AppendOutcome::Committed(_)));
                    let observed = observed.ok_or("follow ended")??;
                    assert_eq!(observed.sequence, tail as u64);
                    assert_eq!(observed.value.as_slice(), value.as_slice());
                    expected.get_mut(&path).unwrap().push(value.clone());
                }
                4 => {
                    schedule.cut_next(match choice[2] % 4 {
                        0 => DurableFault::Before,
                        1 => DurableFault::After,
                        2 => DurableFault::Torn(7),
                        _ => DurableFault::DiskFull,
                    });
                    let outcome = provider.append(append(&path, tail, value.clone())?).await;
                    if matches!(outcome, Ok(AppendOutcome::Committed(_))) {
                        expected
                            .entry(path.clone())
                            .or_default()
                            .push(value.clone());
                    }
                }
                _ => {
                    let request = append(&path, tail, value.clone())?;
                    let first = provider.append(request.clone()).await?;
                    assert!(matches!(first, AppendOutcome::Committed(_)));
                    assert_eq!(
                        provider.append(request).await?,
                        first,
                        "replay changed its receipt"
                    );
                    expected
                        .entry(path.clone())
                        .or_default()
                        .push(value.clone());
                }
            }
            Ok(())
        })
        .await?;
        result?;
        // Discard the crashed scope only after its original work has drained.
        // Recovery uses the same real directory, including torn journal bytes.
        drop(provider);
        provider = LocalStream::open(directory.path(), limits).await?;
        if operation == 4 {
            let actual = records(&provider, &path).await?;
            let prefix = expected.get(&path).cloned().unwrap_or_default();
            if actual != prefix {
                let mut permitted = prefix;
                permitted.push(value);
                assert_eq!(
                    actual, permitted,
                    "failed write changed more than its own record"
                );
                expected.insert(path, actual);
            }
        }
        invariant(&provider, &expected).await?;
    }
    Ok(())
}

fn run(seed: u64, steps: usize) {
    let runtime = seeded_runtime(seed).unwrap();
    runtime.block_on(async {
        tokio::time::pause();
        world(seed, steps)
            .await
            .unwrap_or_else(|error| panic!("seed={seed} steps={steps}: {error}"));
    });
}

#[test]
fn seeded_journal_snapshot_follow_and_fork() {
    for seed in [0, 1, 7, 71] {
        run(seed, 32);
    }
}

#[test]
#[ignore = "deep seed sweep; run on an idle-priority validation worker"]
fn seeded_journal_deep_sweep() {
    for seed in 0..4096 {
        run(seed, 256);
    }
}
