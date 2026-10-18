use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use acyclic_test_support::differential;

async fn fixture(count: u64, case: u8) -> (TaskVolume<MemoryStream>, Query, FileKey) {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("scopes").unwrap(),
    )
    .await
    .unwrap();
    let task = Component::new("task").unwrap();
    let execution = crate::OperationId::from_bytes([1; 16]);
    let key = FileKey::Task {
        task: task.clone(),
        file: TaskFile::Chunk {
            turn: 0,
            execution,
            sequence: 0,
        },
    };
    let mut cut = volume.head().await.unwrap();
    for ordinal in 0..count {
        let mut value = FileValue::new(
            if ordinal == 0 {
                key.clone()
            } else {
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Chunk {
                        turn: if matches!(case, 1 | 2) { ordinal } else { 0 },
                        execution: if case == 0 {
                            crate::OperationId::from_bytes([2; 16])
                        } else {
                            execution
                        },
                        sequence: ordinal,
                    },
                }
            },
            if ordinal == 0 {
                b"needle".to_vec()
            } else {
                b"other".to_vec()
            },
        );
        value.channel = Some(if ordinal > 0 && case == 3 {
            Channel::Reasoning
        } else {
            Channel::Text
        });
        cut = volume.append(cut, &value).await.unwrap();
    }
    let query = match case {
        0 => Query::ExecutionChunks {
            task,
            turn: 0,
            execution,
        },
        1 => Query::Chunks { task, turn: 0 },
        2 => Query::Turn { task, turn: 0 },
        _ => Query::File(FileKey::Task {
            task,
            file: TaskFile::TextHead {
                turn: 0,
                execution,
                channel: Channel::Text,
            },
        }),
    };
    (volume, query, key)
}

#[test]
fn scoped_queries_have_exact_work_with_unrelated_history_and_scan_negative() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut measurements = Vec::new();
    for case in 0..4 {
        for count in [64, 128] {
            let [old, candidate] = differential::pair::<_, _, (FileKey, Vec<u8>)>(
                || runtime.block_on(fixture(count, case)),
                |(volume, query, _), scan| {
                    let mut output = None;
                    let (allocations, work) = crate::test_support::measure_task_volume_work(|| {
                        acyclic_test_support::measure(|| {
                            output = Some(runtime.block_on(async {
                                if scan {
                                    volume
                                        .snapshot_index::<false>(&query, 1)
                                        .await
                                        .unwrap()
                                        .files
                                } else {
                                    volume.snapshot(&query, 1).await.unwrap().files
                                }
                            }));
                        })
                    });
                    let output = output.unwrap();
                    for file in &output {
                        differential::record((file.key.clone(), file.bytes.to_vec()));
                    }
                    (output, work, allocations.count_total)
                },
            );
            assert_eq!(old.0.0, candidate.0.0, "full files changed");
            assert_eq!(old.1, candidate.1, "observer trace changed");
            assert_eq!(candidate.0.0.len(), 1);
            assert_eq!(candidate.0.0[0].bytes.as_ref(), b"needle");
            assert_eq!(
                candidate.0.1, 3,
                "one anchor, one scoped position, one file lookup"
            );
            assert_eq!(
                old.0.1,
                count + 1,
                "full-scan negative escaped the exact candidate bound"
            );
            eprintln!(
                "scoped case={case} N={count} allocations={} baseline={}",
                candidate.0.2, old.0.2
            );
            measurements.push(candidate.0.2);
            let (volume, _, key) = runtime.block_on(fixture(count, case));
            let (_, work) = crate::test_support::measure_task_volume_work(|| {
                runtime
                    .block_on(volume.snapshot(&Query::File(key), 1))
                    .unwrap()
            });
            assert_eq!(work, 2, "exact lookup must remain one anchor and one file");
        }
    }
    // Pin concrete production allocations after collecting every scoped case.
    assert_eq!(measurements, vec![13, 13, 13, 13, 14, 14, 14, 14]);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn scoped_position_index_matches_full_scan(count in 2u64..16, case in 0u8..4) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let (volume, query, _) = runtime.block_on(fixture(count, case));
        let expected = runtime.block_on(volume.snapshot_index::<false>(&query, 1)).unwrap();
        let (actual, work) = crate::test_support::measure_task_volume_work(|| runtime.block_on(volume.snapshot(&query, 1)).unwrap());
        proptest::prop_assert_eq!(actual.files, expected.files);
        proptest::prop_assert_eq!(actual.cursor, expected.cursor);
        proptest::prop_assert_eq!(work, 3);
    }
}
