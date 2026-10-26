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

/// Forwards everything to a local stream except its durability barrier, so a
/// receipt can come only from a watermark that already covers the file.
struct NoBarrier(acyclic_stream::LocalStream);

#[async_trait::async_trait]
impl StreamProvider for NoBarrier {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.0.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.0.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> std::result::Result<u64, StreamError> {
        self.0.tail(path).await
    }
    async fn bounds(
        &self,
        path: StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        self.0.bounds(path).await
    }
    async fn watermarks(
        &self,
        path: StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamWatermarks, StreamError> {
        self.0.watermarks(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, StreamError> {
        self.0.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
        self.0.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.0.read(request).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: acyclic_stream::FollowFilter,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.0.follow(path, from, filter).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, StreamError> {
        self.0.children_page(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<CommitOutcome, StreamError> {
        self.0.commit(request).await
    }
    async fn read_commit(
        &self,
        commit_id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.0.read_commit(commit_id).await
    }
}

fn approval() -> Write {
    Write::new(
        FileKey::Task {
            task: Component::new("task").unwrap(),
            file: TaskFile::ApprovalDecision {
                request: Component::new("request").unwrap(),
                principal: Component::new("principal").unwrap(),
            },
        },
        b"approve".to_vec(),
    )
}

/// Publishes the approval file entirely inside a deferred-durability scope.
async fn publish_deferred<P: StreamProvider>(provider: Arc<P>) -> Result<DurableFile> {
    let volume = TaskVolume::open(provider, StreamPath::new("deferred-approval").unwrap()).await?;
    acyclic_stream::deferring_durability(publish_durable(&volume, &approval(), None)).await
}

/// Inside `deferring_durability` every commit acknowledgement precedes the
/// device sync. The proof still comes only from the provider's barrier, and
/// once issued the stream's durable watermark covers the file.
#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_publication_is_proven_only_through_the_barrier() {
    let directory = tempfile::tempdir().unwrap();
    let local = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .unwrap();
    let proof = publish_deferred(Arc::new(local.clone())).await.unwrap();
    assert_eq!(proof.key(), &approval().key);
    let path = StreamPath::new("deferred-approval").unwrap();
    let watermarks = local.watermarks(path).await.unwrap();
    assert_eq!(watermarks.durable, watermarks.accepted);
}

/// Negative control: the same deferred, acknowledged publication on a provider
/// without the barrier yields no proof, although the file is accepted.
#[tokio::test(flavor = "multi_thread")]
async fn negative_control_a_deferred_acknowledgement_yields_no_proof() {
    let directory = tempfile::tempdir().unwrap();
    let local = acyclic_stream::LocalStream::open(directory.path(), Default::default())
        .await
        .unwrap();
    let error = publish_deferred(Arc::new(NoBarrier(local.clone())))
        .await
        .expect_err("a deferred acknowledgement produced a durability proof");
    assert!(matches!(error, Error::AwaitingDurability), "{error:?}");
    let path = StreamPath::new("deferred-approval").unwrap();
    let watermarks = local.watermarks(path).await.unwrap();
    assert!(watermarks.durable < watermarks.accepted);
}
