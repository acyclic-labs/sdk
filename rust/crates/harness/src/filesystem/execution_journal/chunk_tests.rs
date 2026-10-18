//! Real task-owned journal crash cuts and bounded chunk projection work.
use super::*;
use crate::runtime::DurableTaskHost;
use crate::test_stream::{ExecutionFaultMode, LostSessionAck};
use crate::test_support::accounted;
use crate::{
    durable_host::tests::fixture::Fixture,
    task_volume::{Query, TaskFiles},
};
use acyclic_stream::MemoryStream;
use acyclic_test_support::differential;
use std::sync::atomic::Ordering::SeqCst;

type Provider = LostSessionAck<MemoryStream>;
type Journal = FilesystemExecutionJournal<
    Provider,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;
const OP: OperationId = OperationId::from_bytes([2; 16]);
struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1234
    }
}

async fn fixture() -> Result<(
    Fixture<Provider>,
    Journal,
    Arc<TaskFiles<Provider>>,
    ExecutionEvent,
)> {
    let f = Fixture::new(
        Arc::new(Provider {
            inner: MemoryStream::new_with_clock(Default::default(), Arc::new(Clock)),
            ..Default::default()
        }),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        Some(crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 2,
        }),
        &[],
    )
    .await?;
    let owner = f.running(1).await?;
    let (task, fence) = owner.task_binding();
    let request = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            model: crate::model::Model::new(
                "synthetic".into(),
                "journal".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            messages: vec![crate::model::ModelMessage {
                role: crate::model::ModelRole::User,
                content: ModelContent::Text("hello".into()),
            }],
            tools: vec![],
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )?;
    f.host
        .claim_model_dispatch(task, OP, 0, request.request_digest(), fence.clone())
        .await?;
    // Fix initial entropy independently on each side: native B-tree lookup
    // examinations depend on authenticated commit identities, including headers.
    f.stream
        .append(acyclic_stream::AppendRequest {
            path: acyclic_stream::StreamPath::new(format!(
                "harness/task-volumes/{}",
                f.storage.storage_name(&mut None)?
            ))?,
            records: vec![
                serde_json::to_vec(&serde_json::json!({
                    "volume":"06060606-0606-0606-0606-060606060606",
                    "epoch":"07070707-0707-0707-0707-070707070707",
                    "schema":crate::task_volume::SCHEMA,
                }))
                .unwrap()
                .into(),
            ],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let files = f.task_files().await?;
    let journal = f
        .execution(f.owner(1).await?, OP)?
        .with_task_files(Some(files.clone()));
    let reference = journal
        .stage(OP, "request", request.bytes(), "application/json", None)?
        .await?;
    journal
        .append(
            OP,
            "started",
            &ExecutionEvent::Started {
                request_digest: [9; 32],
            },
            None,
        )?
        .await?;
    journal
        .append(
            OP,
            "model",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: crate::executor::ModelPurpose::Response,
                request_digest: request.request_digest(),
                request: reference,
            },
            None,
        )?
        .await?;
    let reference = crate::executor::stage_json(
        &journal,
        OP,
        "delta",
        &ModelEvent::Content {
            delta: "working".into(),
        },
        None,
    )
    .await?;
    let event = ExecutionEvent::Model {
        step: 0,
        purpose: crate::executor::ModelPurpose::Response,
        event: reference,
    };
    Ok((f, journal, files, event))
}

async fn crash_cut(cut: u8, native: bool, broken: bool) -> Result<bool> {
    let (f, mut journal, files, event) = fixture().await?;
    if broken {
        journal.task_files = None;
    } // Former two-commit path, killed before publication.
    f.stream.arm_execution(
        2,
        match cut {
            0 => ExecutionFaultMode::Before,
            1 => ExecutionFaultMode::AfterVisible,
            _ => ExecutionFaultMode::AfterHidden,
        },
    );
    // append_validated is the exact old crash cut: no caller continuation runs.
    let result = if native {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 1 << 28),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let journal = f
            .native_execution(1, OP, &io)
            .await?
            .with_task_files(if broken { None } else { Some(files.clone()) });
        f.stream.native_only.store(true, SeqCst);
        let result = journal
            .append_validated(OP, None, "event:100", &event, Some(&io))
            .await;
        f.stream.native_only.store(false, SeqCst);
        result
    } else {
        journal
            .append_validated(OP, None, "event:100", &event, None)
            .await
    };
    if let Err(error) = result {
        assert!(matches!(error, Error::Indeterminate(_)), "{error:?}");
    }
    drop(journal);
    f.stream.hide_receipt.store(false, SeqCst);
    let reopened = f
        .execution(f.owner(1).await?, OP)?
        .with_task_files(Some(files.clone()));
    let before = files.inner.snapshot(&Query::Root, 16).await?;
    let records = reopened.replay(OP, 0, 8, None)?.await?;
    let committed = records.len() == 3;
    let complete = committed == (before.files.len() == 1);
    if !broken {
        assert_eq!(committed, cut != 0);
        if committed {
            assert_eq!(before.files[0].bytes.as_ref(), b"working");
            let path = reopened.path(OP)?;
            let mut raw = path.read(2, 1).await?;
            use futures::TryStreamExt as _;
            let canonical = raw.try_next().await?.unwrap();
            let envelope = f.stream.inner.read_commit(canonical.commit_id).await?;
            assert_eq!(
                envelope.mutations.len(),
                2,
                "event and chunk must share the commit"
            );
        }
        reopened.append(OP, "event:100", &event, None)?.await?;
        let after = files.inner.snapshot(&Query::Root, 16).await?;
        assert_eq!(after.files.len(), 1);
        let tail = after.generation;
        reopened.append(OP, "event:100", &event, None)?.await?;
        assert_eq!(
            files.inner.head().await?,
            tail,
            "exact retry republished its chunk"
        );
        assert!(
            !reopened
                .append_if_tail(OP, 0, "rejected:101", &event, None)?
                .await?
        );
        let mut invalid = event.clone();
        if let ExecutionEvent::Model { step, .. } = &mut invalid {
            *step = 1;
        }
        assert!(
            reopened
                .append(OP, "invalid:102", &invalid, None)?
                .await
                .is_err()
        );
        assert_eq!(
            files.inner.head().await?,
            tail,
            "rejected append published a chunk"
        );
    }
    Ok(complete)
}

#[test]
fn chunk_transaction_survives_each_crash_cut() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for cut in 0..3 {
        for native in [false, true] {
            assert!(runtime.block_on(crash_cut(cut, native, false)).unwrap());
        }
    }
}

#[tokio::test]
async fn negative_control_split_commit_loses_the_chunk() {
    assert!(!crash_cut(2, false, true).await.unwrap());
}
async fn warm() -> Result<(
    Fixture<Provider>,
    Journal,
    Arc<TaskFiles<Provider>>,
    ExecutionEvent,
)> {
    let (f, mut journal, files, event) = fixture().await?;
    journal.task_files = None;
    for ordinal in 0..8 {
        journal
            .append(OP, &format!("warm:{ordinal}"), &event, None)?
            .await?;
    }
    journal.task_files = Some(files.clone());
    // Both bounded prefix slots belong to setup, so every measured anchor
    // uses the same production suffix cursor (including its exact allocation).
    journal.append(OP, "event:99", &event, None)?.await?;
    journal.append(OP, "event:100", &event, None)?.await?;
    Ok((f, journal, files, event))
}

async fn deltas(
    state: (
        Fixture<Provider>,
        Journal,
        Arc<TaskFiles<Provider>>,
        ExecutionEvent,
    ),
    count: usize,
    io: Option<&PreparationIo>,
) -> Result<(Vec<Vec<u8>>, Vec<crate::task_volume::FileValue>, String)> {
    let (f, journal, files, event) = state;
    let journal = match io {
        Some(io) => f
            .native_execution(1, OP, io)
            .await?
            .with_task_files(Some(files.clone())),
        None => journal,
    };
    f.stream.reset_io();
    // The deliberately old snapshot scan uses ordinary I/O. The candidate
    // and payload-copy control must use only original native provider calls.
    f.stream
        .native_only
        .store(io.is_some() && !accounted::chunk_scan_enabled(), SeqCst);
    for index in 101..101 + count {
        let key = format!("event:{index}");
        accounted::observe_append(async { journal.append(OP, &key, &event, io)?.await }, io)
            .await?;
        differential::record(("commit.accept", f.stream.io_snapshot().writes));
        accounted::observe_append(async { journal.append(OP, &key, &event, io)?.await }, io)
            .await?; // Exact retries do no projection work.
        differential::record(("commit.retry", f.stream.io_snapshot().writes));
        assert!(
            !accounted::observe_append(
                async {
                    journal
                        .append_if_tail(OP, 0, "rejected:999", &event, io)?
                        .await
                },
                io
            )
            .await?
        );
        differential::record(("commit.reject", f.stream.io_snapshot().writes));
    }
    let mut invalid = event.clone();
    if let ExecutionEvent::Model { step, .. } = &mut invalid {
        *step = 1;
    }
    let error = accounted::observe_append(
        async { journal.append(OP, "invalid:999", &invalid, io)?.await },
        io,
    )
    .await
    .unwrap_err();
    differential::record(("commit.invalid", f.stream.io_snapshot().writes));
    f.stream.native_only.store(false, SeqCst);
    let writes = f.stream.io_snapshot();
    assert_eq!(writes.writes, count as u64);
    assert_eq!(
        writes.records,
        2 * count as u64,
        "each delta needs exactly one atomic pair"
    );
    let records = journal.replay(OP, 2, 64, None)?.await?;
    let mut bytes = Vec::new();
    for record in records {
        let ExecutionEvent::Model { event, .. } = record.event else {
            unreachable!()
        };
        bytes.push(journal.load(&event, None)?.await?.as_ref().to_vec());
    }
    use futures::TryStreamExt as _;
    let mut canonical = journal.path(OP)?.read(0, 64).await?;
    while let Some(record) = canonical.try_next().await? {
        let mut authenticated = record.commit_id.as_bytes().to_vec();
        authenticated.extend_from_slice(&record.value);
        bytes.push(authenticated);
    }
    let snapshot = files.inner.snapshot(&Query::Root, 64).await?;
    assert_eq!(snapshot.files.len(), count + 2);
    for file in &snapshot.files {
        assert_eq!(file.bytes.as_ref(), b"working");
    }
    Ok((bytes, snapshot.files, error.to_string()))
}

fn semantic_trace(trace: &[(&'static str, u64)]) -> Vec<(&'static str, u64)> {
    trace
        .iter()
        .copied()
        .filter(|(operation, _)| matches!(*operation, "read" | "verify" | "retain"))
        .collect()
}

fn totals(trace: &[(&'static str, u64)]) -> [u64; 7] {
    [
        "chunk.items",
        "chunk.allocations",
        "chunk.allocated-bytes",
        "chunk.native-allocations",
        "chunk.native-copy",
        "chunk.native-encoding",
        "chunk.native-hash",
    ]
    .map(|name| {
        trace
            .iter()
            .filter(|(operation, _)| *operation == name)
            .map(|(_, value)| value)
            .sum()
    })
}

#[test]
fn chunk_projection_has_exact_linear_work_and_allocations() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut modes = Vec::new();
    for native in [false, true] {
        let mut measured = Vec::new();
        for count in [8, 16] {
            let [baseline, candidate] = differential::pair(
                || runtime.block_on(warm()).unwrap(),
                |state, old| {
                    accounted::with_chunk_scan(old, || {
                        if native {
                            accounted::accounted(true, 1 << 28, false, |io| {
                                runtime.block_on(deltas(state, count, Some(io))).unwrap()
                            })
                            .unwrap()
                            .0
                        } else {
                            runtime.block_on(deltas(state, count, None)).unwrap()
                        }
                    })
                },
            );
            assert_eq!(
                baseline.0, candidate.0,
                "prefix scan changed bytes or exact errors"
            );
            assert_eq!(semantic_trace(&baseline.1), semantic_trace(&candidate.1));
            let metric = totals(&candidate.1);
            let broken = totals(&baseline.1);
            eprintln!("chunk native={native} N={count} exact={metric:?}");
            assert_ne!(
                broken, metric,
                "quadratic publication passed the work/allocation guarantee"
            );
            measured.push((metric, broken));
        }
        modes.push(measured);
    }
    // Exact production allocations include all three chunk scopes and their
    // actual node splits and shared StreamPath future frames. Neither temporary
    // payload copies nor prefix scans fit. Naming plus fenced recheck adds one
    // anchor read and task-tail lookup; rebuilding the tail adds one index step:
    // exactly 3 items, 6/7 ordinary/native allocations per accepted delta.
    // Main #821 changed the shared native bounded encoder and its charges;
    // exact native pins include that codec, while the unmetered path is unchanged.
    let expected = [
        [[136, 219, 27944, 0, 0, 0, 0], [272, 445, 66672, 0, 0, 0, 0]],
        [
            [136, 315, 34152, 208, 7464, 1384, 13440],
            [272, 637, 79088, 416, 14944, 2776, 26888],
        ],
    ];
    for (mode, measured) in modes.into_iter().enumerate() {
        for (size, (metric, _)) in measured.iter().enumerate() {
            assert_eq!(*metric, expected[mode][size]);
        }
        for item in [0, 1] {
            let curvature = |values: [[u64; 7]; 2]| {
                i128::from(values[1][item]) - 2 * i128::from(values[0][item])
            };
            assert_ne!(
                curvature([measured[0].1, measured[1].1]),
                curvature(expected[mode]),
                "quadratic work escaped exact N/2N assertion"
            );
        }
    }
}

#[test]
fn projection_payload_copy_negative_control_has_exact_extra_allocations() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        let [baseline, candidate] = differential::pair(
            || runtime.block_on(warm()).unwrap(),
            |state, copied| {
                accounted::with_chunk_copy(copied, || {
                    if native {
                        accounted::accounted(true, 1 << 28, false, |io| {
                            runtime.block_on(deltas(state, 8, Some(io))).unwrap()
                        })
                        .unwrap()
                        .0
                    } else {
                        runtime.block_on(deltas(state, 8, None)).unwrap()
                    }
                })
            },
        );
        assert_eq!(baseline.0, candidate.0);
        assert_eq!(semantic_trace(&baseline.1), semantic_trace(&candidate.1));
        let old = totals(&baseline.1);
        let new = totals(&candidate.1);
        eprintln!("copy negative native={native} old={old:?} candidate={new:?}");
        assert_eq!(old[0], new[0]);
        assert_eq!(old[1] - new[1], if native { 16 } else { 8 });
        assert_eq!(new[1], if native { 315 } else { 219 });
        if native {
            assert_eq!(old[3] - new[3], 16);
            assert_eq!(old[4] - new[4], 8 * b"working".len() as u64);
            assert_eq!(old[5], new[5]);
            assert_eq!(old[6], new[6]);
        }
        assert!(old[2] > new[2]);
    }
}

#[test]
fn chunk_projection_preserves_the_bound_on_its_authorized_task_files() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for maximum in [6, 7] {
        for native in [false, true] {
            let (f, mut journal, files, event) = runtime.block_on(fixture()).unwrap();
            let limited = runtime
                .block_on(TaskFiles::open(
                    f.stream.clone(),
                    files.volume(),
                    &journal.verifier,
                    &journal.scope,
                    maximum,
                ))
                .unwrap();
            let limited = Arc::new(limited);
            journal.task_files = Some(limited.clone());
            let before = runtime.block_on(files.inner.head()).unwrap();
            let appended = if native {
                accounted::accounted(true, 1 << 28, false, |io| {
                    runtime.block_on(async {
                        let journal = f
                            .native_execution(1, OP, io)
                            .await
                            .unwrap()
                            .with_task_files(Some(limited.clone()));
                        journal
                            .append(OP, "bounded:100", &event, Some(io))
                            .unwrap()
                            .await
                    })
                })
                .unwrap()
                .0
            } else {
                runtime.block_on(journal.append(OP, "bounded:100", &event, None).unwrap())
            };
            assert_eq!(appended.is_ok(), maximum == 7, "{appended:?}");
            let after = runtime
                .block_on(files.inner.snapshot(&Query::Root, 4))
                .unwrap();
            assert_eq!(after.files.len(), usize::from(maximum == 7));
            let records = runtime
                .block_on(journal.replay(OP, 0, 8, None).unwrap())
                .unwrap();
            assert_eq!(records.len(), if maximum == 7 { 3 } else { 2 });
            if maximum == 6 {
                assert_eq!(after.generation, before);
            }
        }
    }
}

#[test]
fn entire_append_has_exact_linear_work_with_prefix_replay_negative() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let names = [
        "append.calls",
        "append.items",
        "append.allocations",
        "append.allocated-bytes",
        "append.native-allocations",
        "append.native-copy",
        "append.native-encoding",
        "append.native-hash",
        "append.native-items",
    ];
    // Whole-append physical and original-ledger receipts. N/2N includes
    // fixed initial replay and the real index/map splits; no projection gap.
    // Shared StreamPath clones remove 5*N+2 allocations from the old path owner.
    // Native pins include the main #821 single-pass bounded encoder and charges.
    let expected = [
        [
            [25, 136, 10542, 3407159, 0, 0, 0, 0, 0],
            [49, 272, 20498, 6669719, 0, 0, 0, 0, 0],
        ],
        [
            [
                25, 136, 14578, 6274200, 13348, 493688, 178747, 290950, 27751,
            ],
            [
                49, 272, 25222, 11595576, 23351, 880886, 315735, 523560, 54145,
            ],
        ],
    ];
    let mut modes = Vec::new();
    for native in [false, true] {
        let mut measured = Vec::new();
        let mut negatives = Vec::new();
        for count in [8, 16] {
            let [baseline, candidate] = differential::pair(
                || runtime.block_on(warm()).unwrap(),
                |state, old| {
                    accounted::with_append_scan(old, || {
                        if native {
                            accounted::accounted(true, 1 << 28, false, |io| {
                                runtime.block_on(deltas(state, count, Some(io))).unwrap()
                            })
                            .unwrap()
                            .0
                        } else {
                            runtime.block_on(deltas(state, count, None)).unwrap()
                        }
                    })
                },
            );
            assert_eq!(
                baseline.0, candidate.0,
                "full bytes, task files and exact errors changed"
            );
            let commits = |trace: &Vec<(&'static str, u64)>| {
                trace
                    .iter()
                    .copied()
                    .filter(|(name, _)| name.starts_with("commit."))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                commits(&baseline.1),
                commits(&candidate.1),
                "committed operation trace changed"
            );
            let sum = |trace: &Vec<(&'static str, u64)>| {
                names.map(|name| {
                    trace
                        .iter()
                        .filter(|(key, _)| *key == name)
                        .map(|(_, n)| *n)
                        .sum::<u64>()
                })
            };
            let actual = sum(&candidate.1);
            let broken = sum(&baseline.1);
            eprintln!("append native={native} N={count} actual={actual:?} negative={broken:?}");
            eprintln!(
                "append projection native={native} N={count} exact={:?}",
                totals(&candidate.1)
            );
            assert_eq!(actual[0], 3 * count as u64 + 1);
            assert_ne!(
                actual, broken,
                "full prefix replay passed whole-append work detector"
            );
            negatives.push(broken);
            measured.push(actual);
        }
        modes.push((measured, negatives));
    }
    for (mode, (measured, negatives)) in modes.into_iter().enumerate() {
        let native = mode == 1;
        let expected = expected[mode];
        assert_eq!(measured.as_slice(), &expected);
        for metric in 0..9 {
            assert_eq!(
                i128::from(measured[1][metric]) - 2 * i128::from(measured[0][metric]),
                i128::from(expected[1][metric]) - 2 * i128::from(expected[0][metric]),
            );
        }
        // The same N/2N allocation assertion rejects the separately initialized
        // full-prefix production-path control, even with identical outputs.
        assert_ne!(
            i128::from(negatives[1][2]) - 2 * i128::from(negatives[0][2]),
            i128::from(expected[1][2]) - 2 * i128::from(expected[0][2]),
        );
        if native {
            assert_ne!(
                i128::from(negatives[1][8]) - 2 * i128::from(negatives[0][8]),
                i128::from(expected[1][8]) - 2 * i128::from(expected[0][8]),
            );
        }
    }
}

mod ordering;
