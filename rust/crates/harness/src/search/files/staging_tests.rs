use super::*;
use crate::sync::Mutex;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitMutation,
    CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, MemoryStream, ReadRequest, Record, RecordStream, StreamBounds,
    StreamError, StreamPath, StreamWatermarks,
};

type Storage<T> = std::result::Result<T, StreamError>;

struct Interleaved {
    inner: Arc<MemoryStream>,
    parent: StreamPath,
    // Counter locks are released before any volume/provider call.
    staged: Mutex<usize>,
    published: Mutex<usize>,
    fail_open: Mutex<bool>,
}

#[async_trait::async_trait]
impl StreamProvider for Interleaved {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Storage<Option<IdempotencyObservation>> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Storage<u64> {
        self.inner.tail(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Storage<StreamWatermarks> {
        self.inner.watermarks(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Storage<StreamBounds> {
        self.inner.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> Storage<AppendOutcome> {
        if request.path.as_str().contains("/files/") {
            let fail = std::mem::take(&mut *self.fail_open.lock().await);
            if fail {
                return Err(StreamError::Unavailable);
            }
        }
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Storage<ForkReceipt> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Storage<RecordStream> {
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Storage<RecordStream> {
        self.inner.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Storage<RecordStream> {
        self.inner.follow_checked(path, from, preceding).await
    }
    async fn children(&self, request: ChildrenRequest) -> Storage<ChildStream> {
        self.inner.children(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Storage<CommittedEnvelope> {
        self.inner.read_commit(id).await
    }
    async fn commit(&self, request: CommitRequest) -> Storage<CommitOutcome> {
        let write = request.mutations.iter().find_map(|mutation| {
            let CommitMutation::Append { records, .. } = mutation else {
                return None;
            };
            let bytes = records.first()?;
            FileValue::from_record(&Record {
                sequence: 1,
                value: bytes.clone(),
                commit_id: CommitId::from_bytes([0; 32]),
                committed_at_micros: 0,
            })
            .ok()
            .flatten()
        });
        let counter = match write.as_ref().map(|write| &write.key) {
            Some(FileKey::Task {
                file: TaskFile::RequestReady(_),
                ..
            }) => Some(&self.staged),
            Some(FileKey::Task {
                file: TaskFile::RequestResult(_),
                ..
            }) => Some(&self.published),
            _ => None,
        };
        if let Some(counter) = counter {
            // Advance until this append is stale, or all four writes have landed.
            // A request-scoped staging append has no condition on the parent tail.
            loop {
                let next = {
                    let mut count = counter.lock().await;
                    if *count == 4 {
                        break;
                    }
                    *count += 1;
                    *count
                };
                let peer = TaskVolume::open(self.inner.clone(), self.parent.clone())
                    .await
                    .unwrap();
                let generation = peer
                    .snapshot(&Query::File(FileKey::Goal), 1)
                    .await
                    .unwrap()
                    .generation;
                let published = *self.published.lock().await;
                peer.append(
                    generation,
                    &FileValue::new(FileKey::Goal, format!("{next}-{published}").into_bytes()),
                )
                .await
                .unwrap();
                if request.conditions.iter().any(|condition| {
                    matches!(condition,
                    CommitCondition::Head { path, .. } | CommitCondition::Tail { path, .. }
                        if path == &self.parent)
                }) {
                    break;
                }
            }
        }
        self.inner.commit(request).await
    }
}

pub(super) async fn schedule(provider: Arc<dyn SearchProvider>, broken: bool) -> Result<()> {
    let stream = Arc::new(Interleaved {
        inner: Arc::new(MemoryStream::default()),
        parent: StreamPath::new("staging-conflicts").unwrap(),
        staged: Mutex::new(0),
        published: Mutex::new(0),
        fail_open: Mutex::new(false),
    });
    let volume = TaskVolume::open(stream.clone(), stream.parent.clone())
        .await
        .map_err(storage)?;
    let task = Component::new("staging").unwrap();
    let id = Component::new("search-one").unwrap();
    let fence = LeaseFence {
        reservation_id: "owner".into(),
        placement: "test".into(),
    };
    let write = FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(id.clone()),
        },
        br#"{"query":"model"}"#.as_slice(),
    );
    let generation = volume
        .snapshot(&Query::File(write.key.clone()), 1)
        .await
        .map_err(storage)?
        .generation;
    volume.append(generation, &write).await.map_err(storage)?;
    let completion = volume
        .file_volume(&ready(&task, &id))
        .await
        .map_err(storage)?;
    let bytes = encode(
        &SearchOutcome::Succeeded {
            response: SearchResponse { results: vec![] },
        },
        None,
    )?;
    let authorize = |_| std::future::ready(Ok(vec![]));
    let result = if broken {
        // Restore staging on the shared parent generation: four conflicts lose it.
        retain_response(&volume, ready(&task, &id), bytes.clone(), None)
            .await
            .map(|_| ())
    } else {
        settle(
            &volume,
            &task,
            &id,
            &fence,
            &mut || Ok(Some(provider.clone())),
            &authorize,
            None,
        )
        .await
    };
    assert!(
        matches!(result, Err(Error::SearchPublicationDeferred)),
        "completed response was not retained: {result:?}"
    );
    assert_eq!(*stream.staged.lock().await, 4);
    let saved = completion
        .snapshot(&Query::File(ready(&task, &id)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(saved.files.len(), 1);
    assert_eq!(saved.files[0].bytes, bytes);
    assert_eq!(*stream.published.lock().await, 4);
    let reopened = TaskVolume::open(stream.clone(), stream.parent.clone())
        .await
        .map_err(storage)?;
    settle(
        &reopened,
        &task,
        &id,
        &fence,
        &mut || panic!("resume redispatched the provider"),
        &authorize,
        None,
    )
    .await?;
    let result = reopened
        .snapshot(&Query::File(file(&task, &id, true)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(result.files.len(), 1);
    assert_eq!(result.files[0].bytes, bytes);
    Ok(())
}

#[tokio::test]
async fn four_interleaved_volume_writes_preserve_completion_and_resume() -> Result<()> {
    use crate::test_support::{ScriptedSearch, ScriptedSearchProvider};
    let provider = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "model".into(),
            max_results: 5,
        },
        response: Some(SearchResponse { results: vec![] }),
        error: None,
    }])?);
    schedule(provider.clone(), false).await?;
    assert_eq!(provider.remaining(), 0);
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "completed response was not retained")]
async fn negative_control_detects_staging_on_the_shared_generation() {
    schedule(
        Arc::new(crate::test_support::ScriptedSearchProvider::new([]).unwrap()),
        true,
    )
    .await
    .unwrap();
}

pub(super) async fn failed_open_retry(
    provider: Arc<dyn SearchProvider>,
    broken: bool,
) -> Result<()> {
    let stream = Arc::new(Interleaved {
        inner: Arc::new(MemoryStream::default()),
        parent: StreamPath::new("failed-completion-open").unwrap(),
        staged: Mutex::new(4),
        published: Mutex::new(4),
        fail_open: Mutex::new(true),
    });
    let volume = TaskVolume::open(stream.clone(), stream.parent.clone())
        .await
        .map_err(storage)?;
    let task = Component::new("task").unwrap();
    let id = Component::new("search-open").unwrap();
    let fence = LeaseFence {
        reservation_id: "owner".into(),
        placement: "test".into(),
    };
    let generation = volume
        .snapshot(&Query::Root, 0)
        .await
        .map_err(storage)?
        .generation;
    let generation = volume
        .append(
            generation,
            &FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Request(id.clone()),
                },
                br#"{"query":"model"}"#.as_slice(),
            ),
        )
        .await
        .map_err(storage)?;
    if broken {
        // Restore the rejected ordering: make the claim irreversible before open.
        volume
            .append(
                generation,
                &FileValue::new(file(&task, &id, false), encode(&fence, None)?),
            )
            .await
            .map_err(storage)?;
    }
    let authorize = |_| std::future::ready(Ok(vec![]));
    let first = settle(
        &volume,
        &task,
        &id,
        &fence,
        &mut || panic!("failed open resolved credentials"),
        &authorize,
        None,
    )
    .await;
    assert!(
        matches!(
            first,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                ..
            })
        ),
        "{first:?}"
    );
    let claim = volume
        .snapshot(&Query::File(file(&task, &id, false)), 1)
        .await
        .map_err(storage)?;
    assert!(claim.files.is_empty(), "failed open committed a claim");
    let reopened = TaskVolume::open(stream.clone(), stream.parent.clone())
        .await
        .map_err(storage)?;
    settle(
        &reopened,
        &task,
        &id,
        &fence,
        &mut || Ok(Some(provider.clone())),
        &authorize,
        None,
    )
    .await?;
    let terminal = reopened
        .snapshot(&Query::File(file(&task, &id, true)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(terminal.files.len(), 1);
    assert_eq!(
        serde_json::from_slice::<SearchOutcome>(&terminal.files[0].bytes).unwrap(),
        SearchOutcome::Succeeded {
            response: SearchResponse { results: vec![] }
        }
    );
    Ok(())
}

#[tokio::test]
async fn failed_completion_open_leaves_request_retryable() -> Result<()> {
    use crate::test_support::{ScriptedSearch, ScriptedSearchProvider};
    let provider = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "model".into(),
            max_results: 5,
        },
        response: Some(SearchResponse { results: vec![] }),
        error: None,
    }])?);
    failed_open_retry(provider.clone(), false).await?;
    assert_eq!(provider.remaining(), 0);
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "failed open committed a claim")]
async fn negative_control_detects_claiming_before_open() {
    failed_open_retry(
        Arc::new(crate::test_support::ScriptedSearchProvider::new([]).unwrap()),
        true,
    )
    .await
    .unwrap();
}
