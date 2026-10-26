use super::*;
use acyclic_stream::MemoryStream;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn only_an_unchanged_history_prefix_can_be_retried(mutation in 0u8..6, text in "[a-z]{0,32}") {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("retry-property").unwrap()).await.unwrap();
            let mut expected = volume.head().await.unwrap();
            let goal = Write::new(FileKey::Goal, b"client".to_vec());
            volume.append(expected, &goal).await.unwrap();
            match mutation {
                1 => expected.volume[0] ^= 1,
                2 => { let mut epoch = *expected.epoch.as_bytes(); epoch[0] ^= 1; expected.epoch = Uuid::from_bytes(epoch); },
                3 => expected.commit[0] ^= 1,
                4 => expected.position = u64::MAX,
                5 => expected.position = 0,
                _ => {},
            }
            let write = Write::new(FileKey::Task { task: Component::new("task").unwrap(),
                file: TaskFile::Tool { turn: 0, call: Component::new("bash").unwrap(), file: ToolFile::Stdout(0) } }, text.into_bytes());
            let result = publish(&volume, expected, &write, || std::future::ready(Ok::<_, Error>((Vec::new(), None)))).await;
            prop_assert_eq!(result.is_ok(), mutation == 0);
            let snapshot = volume.snapshot(&Query::Root, 8).await.unwrap();
            prop_assert_eq!(snapshot.files.contains(&write), mutation == 0);
            prop_assert!(snapshot.files.contains(&goal));
            Ok(())
        })?;
    }
}

fn request() -> Write {
    Write::new(
        FileKey::Task {
            task: Component::new("task").unwrap(),
            file: TaskFile::Tool {
                turn: 0,
                call: Component::new("bash").unwrap(),
                file: ToolFile::Request,
            },
        },
        b"request".to_vec(),
    )
}

async fn coordinator_event(provider: &MemoryStream, path: &StreamPath) {
    let (record, _) = segment::encode(&Write::new(FileKey::Goal, b"event".to_vec()), None).unwrap();
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![record.into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .unwrap();
}

/// After a coordinator event fails the owner condition, publication is
/// retried only under a fresh authorization: it lands while the owner holds
/// its fence, stops when the owner lost it, and never repeats a condition
/// that already failed.
#[test]
fn a_fence_conflict_publishes_only_under_a_fresh_authorization() {
    for outcome in ["owner", "lost", "stale"] {
        futures::executor::block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let coordinator = StreamPath::new("coordinator").unwrap();
            coordinator_event(&provider, &coordinator).await;
            let volume = TaskVolume::open(provider.clone(), StreamPath::new("fenced").unwrap())
                .await
                .unwrap();
            let write = request();
            let mut calls = 0;
            let result = publish(&volume, volume.head().await.unwrap(), &write, || {
                calls += 1;
                let call = calls;
                let provider = provider.clone();
                let coordinator = coordinator.clone();
                async move {
                    let expected = provider.tail(coordinator.clone()).await?;
                    match (call, outcome) {
                        // A sibling's event lands between authorization and commit.
                        (1, _) => coordinator_event(&provider, &coordinator).await,
                        (_, "lost") => return Err(Error::Unauthorized),
                        // The condition that already failed.
                        (_, "stale") => {
                            return Ok((
                                vec![CommitCondition::Tail {
                                    path: coordinator,
                                    expected: expected - 1,
                                }],
                                None,
                            ));
                        }
                        _ => {}
                    }
                    Ok::<_, Error>((
                        vec![CommitCondition::Tail {
                            path: coordinator,
                            expected,
                        }],
                        None,
                    ))
                }
            })
            .await;
            assert_eq!(calls, 2, "{outcome}");
            let snapshot = volume.snapshot(&Query::Root, 8).await.unwrap();
            match outcome {
                "owner" => {
                    assert!(result.is_ok(), "{result:?}");
                    assert_eq!(snapshot.files, [write]);
                }
                "lost" => {
                    assert!(matches!(result, Err(Error::Unauthorized)), "{result:?}");
                    assert!(snapshot.files.is_empty());
                }
                _ => {
                    assert!(matches!(result, Err(Error::FenceConflict)), "{result:?}");
                    assert!(snapshot.files.is_empty());
                }
            }
        });
    }
}
