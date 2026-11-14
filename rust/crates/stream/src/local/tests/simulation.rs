//! Stateful simulation over the real journal, snapshot and reader paths.
//!
//! After every transition: acknowledged records survive recovery, histories have
//! contiguous sequences and exact payloads, forks retain their selected prefix,
//! conditional competitors have one winner, follow has no gaps, and bounded
//! discovery enumerates every child exactly once. A failed physical write may
//! leave its whole record or nothing, never a partial logical record.
use super::super::*;
use acyclic_native_runtime::testing::sweep;
use acyclic_native_runtime::testing::{DurableFault, Schedule, fill_seeded, seeded_runtime};
use std::collections::BTreeMap;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type History = BTreeMap<String, Vec<Vec<u8>>>;

#[derive(Clone, Copy, Debug)]
struct Choice {
    step: usize,
    bytes: [u8; 3],
}

fn append(path: &str, tail: usize, value: Vec<u8>) -> Result<AppendRequest> {
    Ok(AppendRequest {
        path: StreamPath::new(path)?,
        records: vec![value.into()],
        if_tail: Some(tail.try_into()?),
        idempotency_key: IdempotencyKey::unique(),
    })
}

async fn records(provider: &LocalStream, path: &str) -> Result<Vec<Vec<u8>>> {
    let mut values = Vec::new();
    let limit = u32::try_from(crate::MAX_ITEMS)?;
    loop {
        let from = values.len() as u64;
        let mut stream = provider
            .read(ReadRequest {
                key: None,
                path: StreamPath::new(path)?,
                from,
                limit,
                at_least: Default::default(),
            })
            .await?;
        while let Some(record) = stream.next().await {
            let record = record?;
            assert_eq!(
                record.sequence,
                values.len() as u64,
                "noncontiguous history: {path}"
            );
            values.push(record.value.as_slice().to_vec());
        }
        if values.len() as u64 - from < u64::from(limit) {
            break;
        }
    }
    Ok(values)
}

async fn invariant(provider: &LocalStream, expected: &History) -> Result {
    for (path, values) in expected {
        let marks = provider.watermarks(StreamPath::new(path)?).await?;
        assert_eq!(
            marks.accepted,
            values.len() as u64,
            "accepted watermark differs from history"
        );
        assert_eq!(
            marks.durable, marks.accepted,
            "acknowledged history is not durable"
        );
        assert_eq!(
            provider.tail(StreamPath::new(path)?).await?,
            values.len() as u64,
            "tail differs from history"
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
    assert_eq!(
        found,
        expected.keys().cloned().collect::<Vec<_>>(),
        "children differ from hierarchy"
    );
    Ok(())
}

async fn world(seed: u64, choices: &[Choice]) -> Result<(u64, usize)> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits {
        compaction_operations: 4,
        ..Default::default()
    };
    let (opening, mut events) = Schedule::seeded(seed);
    let mut provider = super::journal_lifecycle(
        &opening,
        &mut events,
        LocalStream::open(directory.path(), limits),
    )
    .await??;
    let mut expected = History::new();
    let mut retries = Vec::new();
    let mut epoch = 0;
    let mut faults = 0;
    for Choice {
        step,
        bytes: choice,
    } in choices.iter().copied()
    {
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
        let mut uncertain = false;
        eprintln!("seed={seed} step={step} operation={operation} path={path} tail={tail}");
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
                    assert_eq!(
                        observed.sequence, tail as u64,
                        "follow skipped the appended sequence"
                    );
                    assert_eq!(
                        observed.value.as_slice(),
                        value.as_slice(),
                        "follow changed the appended value"
                    );
                    expected.get_mut(&path).unwrap().push(value.clone());
                }
                4 => {
                    // The first cut proves injection is live. Later cuts also
                    // reach flush, snapshot publication and journal retirement;
                    // an ordinal beyond this operation simply leaves it intact.
                    schedule.cut_at(
                        if faults == 0 {
                            0
                        } else {
                            choice[1] as usize % 8
                        },
                        match choice[2] % 4 {
                            0 => DurableFault::Before,
                            1 => DurableFault::After,
                            2 => DurableFault::Torn(7),
                            _ => DurableFault::DiskFull,
                        },
                    );
                    let outcome = provider.append(append(&path, tail, value.clone())?).await;
                    match outcome {
                        Ok(AppendOutcome::Committed(_)) => expected
                            .entry(path.clone())
                            .or_default()
                            .push(value.clone()),
                        Err(_) => uncertain = true,
                        Ok(AppendOutcome::TailConflict { .. }) => {
                            panic!("fault caused a conditional conflict")
                        }
                    }
                }
                _ => {
                    let request = append(&path, tail, value.clone())?;
                    let first = provider.append(request.clone()).await?;
                    assert!(matches!(first, AppendOutcome::Committed(_)));
                    assert_eq!(
                        provider.append(request.clone()).await?,
                        first,
                        "replay changed its receipt"
                    );
                    retries.push((request, first));
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
        faults += usize::from(schedule.storage_fault().is_some());
        if !uncertain {
            invariant(&provider, &expected).await?;
        }
        // Discard the crashed scope only after its original work has drained.
        // Recovery uses the same real directory, including torn journal bytes.
        drop(provider);
        let (recovery, mut events) = Schedule::seeded(seed ^ (step as u64).rotate_left(32));
        provider = super::journal_lifecycle(
            &recovery,
            &mut events,
            LocalStream::open(directory.path(), limits),
        )
        .await??;
        if uncertain {
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
        if !retries.is_empty() {
            let (request, receipt) = &retries[choice[2] as usize % retries.len()];
            // A replay may finish snapshot maintenance made due by an append
            // whose acknowledgement was lost. It must not append another frame.
            let appended = provider.inner.journal.lock().unwrap().appended;
            let (replay, mut events) = Schedule::seeded(seed ^ step as u64);
            let actual =
                super::journal_lifecycle(&replay, &mut events, provider.append(request.clone()))
                    .await??;
            assert_eq!(&actual, receipt, "recovery changed the original receipt");
            assert_eq!(
                provider.inner.journal.lock().unwrap().appended,
                appended,
                "replay wrote another journal command"
            );
            invariant(&provider, &expected).await?;
        }
        match read_snapshot(directory.path(), limits)? {
            Some(snapshot) => {
                assert!(snapshot.epoch >= epoch, "snapshot epoch moved backwards");
                epoch = snapshot.epoch;
            }
            None => assert_eq!(epoch, 0, "installed snapshot disappeared"),
        }
    }
    Ok((epoch, faults))
}

fn evaluate(seed: u64, choices: &[Choice]) -> std::result::Result<(u64, usize), String> {
    let mut coverage = None;
    // The shared runner owns panic capture too. Coverage stays outside the
    // shrinking predicate: removing work must not manufacture a new failure.
    sweep::sweep("streams replay", 0..1, |_| {
        let runtime = seeded_runtime(seed).map_err(|error| error.to_string())?;
        coverage = Some(
            runtime
                .block_on(async {
                    tokio::time::pause();
                    world(seed, choices).await
                })
                .map_err(|error| error.to_string())?,
        );
        Ok(())
    })
    .map_err(|failure| failure.report)?;
    Ok(coverage.expect("one replay completed"))
}

fn run(seed: u64, steps: usize) -> std::result::Result<(), String> {
    let runtime = seeded_runtime(seed).map_err(|error| error.to_string())?;
    // Draw the operation tape from the shared core independently of how much
    // entropy the implementation consumes. Retain original step identities
    // when the shared minimizer removes choices.
    let choices: Vec<Choice> = runtime.block_on(async {
        let (plan, _) = Schedule::seeded(seed);
        let mut tape = vec![0; steps * 3];
        plan.scope(async {
            assert!(fill_seeded(&mut tape));
        })
        .await;
        tape.chunks_exact(3)
            .enumerate()
            .map(|(step, bytes)| Choice {
                step,
                bytes: bytes.try_into().expect("three-byte choice"),
            })
            .collect()
    });
    drop(runtime);
    match evaluate(seed, &choices) {
        Ok((epoch, faults)) => {
            if epoch == 0 || faults == 0 {
                return Err(format!(
                    "insufficient transition coverage: epoch={epoch} storage_events={faults}"
                ));
            }
            Ok(())
        }
        Err(report) => {
            let signature = report.lines().next().unwrap_or(&report);
            let minimal = sweep::minimize(choices, |candidate| {
                evaluate(seed, candidate)
                    .is_err_and(|error| error.lines().next().unwrap_or(&error) == signature)
            });
            Err(format!("{report}\nminimal choices: {minimal:?}"))
        }
    }
}

#[test]
fn seeded_journal_snapshot_follow_and_fork() {
    sweep::sweep("streams", sweep::seeds(0..4), |seed| run(seed, 32)).unwrap();
}

#[test]
#[ignore = "deep seed sweep; run on an idle-priority validation worker"]
fn seeded_journal_deep_sweep() {
    sweep::sweep("streams deep", sweep::seeds(0..4096), |seed| run(seed, 256)).unwrap();
}
