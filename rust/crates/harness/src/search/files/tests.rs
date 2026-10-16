use super::*;
use crate::{
    durable_host::tests::fixture::Fixture,
    test_support::{ScriptedSearch, ScriptedSearchProvider},
};
use acyclic_stream::{MemoryStream, StreamPath};
use serde_json::json;
use std::cell::Cell;

async fn shared_coordinator_tail_advance(broken: bool) -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let owner = fixture.running(1).await?;
    let volume = TaskVolume::open(stream, StreamPath::new("tail-advance").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new(hex::encode(owner.task_id().into_bytes())).unwrap();
    let id = Component::new("search-tail").unwrap();
    let initial = volume.snapshot(&Query::Root, 0).await.map_err(storage)?;
    volume
        .append(
            initial.generation,
            &FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Request(id.clone()),
                },
                br#"{"query":"tail"}"#.as_slice(),
            ),
        )
        .await
        .map_err(storage)?;
    let provider = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "tail".into(),
            max_results: 5,
        },
        response: Some(SearchResponse { results: vec![] }),
        error: None,
    }])?);
    let advanced = Cell::new(false);
    let state = (&fixture, &owner, &advanced);
    let authorize = |settlement| async move {
        let (fixture, owner, advanced) = state;
        if broken && settlement && advanced.get() {
            return Err(storage(crate::task_volume::Error::FenceConflict));
        }
        let authorized = condition(owner, None, settlement).await?;
        if settlement && !advanced.replace(true) {
            fixture.running(2).await?;
        }
        Ok(authorized)
    };
    let result = settle(
        &volume,
        &task,
        &id,
        owner.task_binding().1,
        &mut || Ok(Some(provider.clone() as Arc<dyn SearchProvider>)),
        &authorize,
        None,
    )
    .await;
    assert!(
        result.is_ok(),
        "completed search response was lost: {result:?}"
    );
    assert!(advanced.get());
    let result = volume
        .snapshot(&Query::File(file(&task, &id, true)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(result.files.len(), 1);
    assert_eq!(
        serde_json::from_slice::<SearchOutcome>(&result.files[0].bytes).unwrap(),
        SearchOutcome::Succeeded {
            response: SearchResponse { results: vec![] }
        }
    );
    assert_eq!(provider.remaining(), 0);
    settle(
        &volume,
        &task,
        &id,
        owner.task_binding().1,
        &mut || panic!("completed search was redispatched"),
        &authorize,
        None,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn completed_response_survives_another_task_advancing_the_coordinator_tail() -> Result<()> {
    shared_coordinator_tail_advance(false).await
}

#[tokio::test]
#[should_panic(expected = "completed search response was lost")]
async fn negative_control_detects_discarding_a_completed_response() {
    shared_coordinator_tail_advance(true).await.unwrap();
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    request: serde_json::Value,
    submit: bool,
    configured: bool,
    authorized: bool,
    steps: Vec<ScriptedSearch>,
    expected: SearchOutcome,
}

async fn published(client: &mut crate::task_volume::Observation) -> Result<FileValue> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let change = client
                .changes
                .next()
                .await
                .expect("live query ended")
                .map_err(storage)?;
            if let Some(file) = change.file {
                return Ok(file);
            }
        }
    })
    .await
    .expect("live query did not publish the result")
}

#[tokio::test]
async fn search_file_scenarios() -> Result<()> {
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    assert_eq!(cases.len(), 8);
    for case in cases {
        let backend = Arc::new(ScriptedSearchProvider::new(case.steps)?);
        let stream = Arc::new(MemoryStream::default());
        let volume = TaskVolume::open(stream, StreamPath::new("file-scenarios").unwrap())
            .await
            .map_err(storage)?;
        let task = Component::new("scenario").unwrap();
        let input = FileKey::parse(".tasks/scenario/requests/search-one.json").unwrap();
        let result = FileKey::parse(".tasks/scenario/requests/search-one/result.json").unwrap();
        let initial = volume
            .snapshot(&Query::Task(task.clone()), 4)
            .await
            .map_err(storage)?;
        let mut client = volume
            .observe(Query::File(result.clone()), None, 1)
            .await
            .map_err(storage)?;
        assert!(client.snapshot.take().unwrap().files.is_empty());
        if case.submit {
            volume
                .append(
                    initial.generation,
                    &FileValue::new(input, serde_json::to_vec(&case.request).unwrap()),
                )
                .await
                .map_err(storage)?;
        }
        let fence = LeaseFence {
            reservation_id: "first".into(),
            placement: "scenario".into(),
        };
        let conditions = if case.authorized {
            vec![]
        } else {
            vec![CommitCondition::Tail {
                path: StreamPath::new("file-scenarios").unwrap(),
                expected: u64::MAX,
            }]
        };
        let resolves = Cell::new(0);
        let first = drive_for_test(
            &volume,
            &task,
            &fence,
            || {
                resolves.set(resolves.get() + 1);
                Ok(case
                    .configured
                    .then(|| backend.clone() as Arc<dyn SearchProvider>))
            },
            conditions,
            None,
            3,
        )
        .await;
        let after = volume
            .snapshot(&Query::Task(task.clone()), 4)
            .await
            .map_err(storage)?;
        if !case.authorized {
            assert!(first.is_err(), "{}", case.name);
            assert_eq!(after.files.len(), 1);
            assert_eq!(resolves.get(), 0);
        } else {
            first?;
            assert_eq!(
                after.files.len(),
                if case.submit { 3 } else { 0 },
                "{}",
                case.name
            );
            if case.submit {
                let change = published(&mut client).await?;
                assert_eq!(change.key, result);
                let outcome: SearchOutcome = serde_json::from_slice(&change.bytes).unwrap();
                assert_eq!(outcome, case.expected, "{}", case.name);
                assert!(
                    !String::from_utf8_lossy(&change.bytes).contains("scripted search unavailable")
                );
                drive_for_test(
                    &volume,
                    &task,
                    &fence,
                    || panic!("replay resolved search credentials"),
                    vec![],
                    None,
                    3,
                )
                .await?;
                let replay = volume
                    .snapshot(&Query::Task(task.clone()), 4)
                    .await
                    .map_err(storage)?;
                assert_eq!(replay.generation, after.generation);
                assert_eq!(replay.files, after.files);
            } else {
                assert_eq!(resolves.get(), 0);
            }
        }
        assert_eq!(backend.remaining(), 0, "{}", case.name);
    }
    Ok(())
}

#[tokio::test]
async fn host_worker_follows_requests_and_clients_follow_results() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let owner = fixture.running(1).await?;
    let volume = TaskVolume::open(stream, StreamPath::new("host-search-files").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new(hex::encode(owner.task_id().into_bytes())).unwrap();
    let request = SearchRequest {
        query: "live query".into(),
        max_results: 5,
    };
    let response = SearchResponse { results: vec![] };
    let backend = Arc::new(ScriptedSearchProvider::new((0..2).map(|_| {
        ScriptedSearch {
            request: request.clone(),
            response: Some(response.clone()),
            error: None,
        }
    }))?);
    let resolves = Cell::new(0);
    let worker = follow(
        &volume,
        &owner,
        || {
            resolves.set(resolves.get() + 1);
            Ok(Some(backend.clone() as Arc<dyn SearchProvider>))
        },
        None,
        6,
    );
    let client = async {
        for id in ["search-snapshot", "search-follow"] {
            let id = Component::new(id).unwrap();
            let result = file(&task, &id, true);
            let mut client = volume
                .observe(Query::File(result.clone()), None, 1)
                .await
                .map_err(storage)?;
            assert!(client.snapshot.take().unwrap().files.is_empty());
            let initial = volume
                .snapshot(&Query::Task(task.clone()), 8)
                .await
                .map_err(storage)?;
            volume
                .append(
                    initial.generation,
                    &FileValue::new(
                        FileKey::Task {
                            task: task.clone(),
                            file: TaskFile::Request(id),
                        },
                        serde_json::to_vec(&request).unwrap(),
                    ),
                )
                .await
                .map_err(storage)?;
            let observed = published(&mut client).await?;
            assert_eq!(observed.key, result);
            assert_eq!(
                serde_json::from_slice::<SearchOutcome>(&observed.bytes).unwrap(),
                SearchOutcome::Succeeded {
                    response: response.clone()
                }
            );
        }
        Ok::<_, Error>(())
    };
    tokio::select! { biased; result = worker => panic!("worker stopped: {result:?}"), result = client => result? }
    assert_eq!(resolves.get(), 2);
    assert_eq!(backend.remaining(), 0);
    drive_for_test(
        &volume,
        &task,
        owner.task_binding().1,
        || panic!("same-owner replay redispatched"),
        condition(&owner, None, false).await?,
        None,
        6,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn replaced_owner_settles_started_request_without_redispatch_and_fences_stale_worker()
-> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let old = fixture.running(1).await?;
    let volume = TaskVolume::open(stream, StreamPath::new("search-recovery").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new(hex::encode(old.task_id().into_bytes())).unwrap();
    let id = Component::new("search-interrupted").unwrap();
    for value in [
        FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::Request(id.clone()),
            },
            br#"{"query":"test"}"#.as_slice(),
        ),
        FileValue::new(
            file(&task, &id, false),
            serde_json::to_vec(old.task_binding().1).unwrap(),
        ),
    ] {
        let snapshot = volume
            .snapshot(&Query::Task(task.clone()), 8)
            .await
            .map_err(storage)?;
        volume
            .append(snapshot.generation, &value)
            .await
            .map_err(storage)?;
    }
    let interrupted = drive_for_test(
        &volume,
        &task,
        old.task_binding().1,
        || panic!("active claim redispatched"),
        condition(&old, None, false).await?,
        None,
        6,
    )
    .await;
    assert!(matches!(interrupted, Err(Error::SearchPublicationDeferred)));
    let replacement = fixture.replace(1).await?;
    let mut client = volume
        .observe(Query::File(file(&task, &id, true)), None, 1)
        .await
        .map_err(storage)?;
    assert!(client.snapshot.take().unwrap().files.is_empty());
    let recovering = follow(
        &volume,
        &replacement,
        || panic!("replacement redispatched uncertain HTTP request"),
        None,
        6,
    );
    let result = tokio::select! { biased;
        result = recovering => panic!("replacement worker stopped: {result:?}"),
        result = published(&mut client) => result?,
    };
    assert_eq!(result.key, file(&task, &id, true));
    assert_eq!(
        serde_json::from_slice::<SearchOutcome>(&result.bytes).unwrap(),
        SearchOutcome::Indeterminate
    );
    let snapshot = volume
        .snapshot(&Query::Task(task.clone()), 8)
        .await
        .map_err(storage)?;
    volume
        .append(
            snapshot.generation,
            &FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Request(Component::new("search-new").unwrap()),
                },
                br#"{"query":"test"}"#.as_slice(),
            ),
        )
        .await
        .map_err(storage)?;
    assert!(
        follow(
            &volume,
            &old,
            || panic!("stale owner contacted provider"),
            None,
            6
        )
        .await
        .is_err()
    );
    Ok(())
}

struct NativeOnly(Arc<ScriptedSearchProvider>);
impl SearchProvider for NativeOnly {
    fn search<'a>(
        &'a self,
        _: &'a SearchRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async { panic!("native worker used ordinary search") })
    }
    fn supports_native_search(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        self.0.supports_native_search(work)
    }
    fn search_with_native_work<'a>(
        &'a self,
        request: &'a SearchRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (SearchResponse, acyclic_fs::NativeAllocation)>> {
        self.0.search_with_native_work(request, work)
    }
}

#[tokio::test]
async fn file_worker_preserves_native_provider_custody_and_terminal_replay() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let running = fixture.running(1).await?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let owner = fixture
        .host
        .journal_owner(running.task_id(), running.task_binding().1, Some(&io))
        .await?;
    let volume = TaskVolume::open(stream, StreamPath::new("native-search-files").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new(hex::encode(owner.task_id().into_bytes())).unwrap();
    let id = Component::new("search-native").unwrap();
    let initial = volume
        .snapshot(&Query::Task(task.clone()), 4)
        .await
        .map_err(storage)?;
    volume
        .append(
            initial.generation,
            &FileValue::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Request(id.clone()),
                },
                br#"{"query":"test"}"#.as_slice(),
            ),
        )
        .await
        .map_err(storage)?;
    let response = SearchResponse {
        results: vec![super::super::SearchResult {
            url: "https://example.com".into(),
            title: "source".into(),
            snippet: "x".repeat(32_768),
        }],
    };
    let script = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "test".into(),
            max_results: 5,
        },
        response: Some(response.clone()),
        error: None,
    }])?);
    let backend = Arc::new(NativeOnly(script.clone()));
    let mut client = volume
        .observe(Query::File(file(&task, &id, true)), None, 1)
        .await
        .map_err(storage)?;
    let worker = follow(
        &volume,
        &owner,
        || Ok(Some(backend.clone() as Arc<dyn SearchProvider>)),
        Some(&work),
        3,
    );
    tokio::select! { biased;
        result = worker => panic!("native worker stopped: {result:?}"),
        result = published(&mut client) => { result?; }
    }
    assert_eq!(script.remaining(), 0);
    let snapshot = volume
        .snapshot(&Query::Task(task.clone()), 4)
        .await
        .map_err(storage)?;
    assert_eq!(snapshot.files.len(), 3);
    let result = snapshot
        .files
        .iter()
        .find(|value| value.key == file(&task, &id, true))
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<SearchOutcome>(&result.bytes).unwrap(),
        SearchOutcome::Succeeded { response }
    );
    for published in &snapshot.files {
        if matches!(
            published.key,
            FileKey::Task {
                file: TaskFile::RequestStarted(_)
                    | TaskFile::RequestReady(_)
                    | TaskFile::RequestResult(_),
                ..
            }
        ) {
            assert!(published.bytes.matches_meter(&work.runtime_meter()));
        }
    }
    let completion = volume
        .file_volume(&ready(&task, &id))
        .await
        .map_err(storage)?;
    let retained = completion
        .snapshot(&Query::File(ready(&task, &id)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(retained.files.len(), 1);
    assert!(retained.files[0].bytes.matches_meter(&work.runtime_meter()));
    assert!(work.receipt().source_bytes_read > 0);
    assert!(work.receipt().bytes_encoded > 0);
    assert!(work.receipt().bytes_copied >= 2 * 32_768);
    drive_for_test(
        &volume,
        &task,
        owner.task_binding().1,
        || panic!("native replay contacted provider"),
        condition(&owner, Some(&work), false).await?,
        Some(&work),
        3,
    )
    .await?;
    assert_eq!(
        volume
            .snapshot(&Query::Task(task), 4)
            .await
            .map_err(storage)?
            .files,
        snapshot.files
    );
    Ok(())
}

#[tokio::test]
async fn request_file_types_keep_host_ownership_and_reset_legacy_cursors() -> Result<()> {
    use crate::task_volume::Owner;
    let input = FileKey::parse(".tasks/t/requests/search-one.json").unwrap();
    assert_eq!(input.owner(), Owner::Caller);
    for path in [
        ".tasks/t/requests/search-one/started.json",
        ".tasks/t/requests/search-one/ready.json",
        ".tasks/t/requests/search-one/result.json",
    ] {
        let key = FileKey::parse(path).unwrap();
        assert_eq!(key.path(), path);
        assert_eq!(key.owner(), Owner::Harness);
    }
    for version in [2, 3] {
        let stream = Arc::new(MemoryStream::default());
        let path = StreamPath::new("legacy-task-files").unwrap();
        stream
        .append(acyclic_stream::AppendRequest {
            path: path.clone(),
            records: vec![
                serde_json::to_vec(
                    &json!({"volume":uuid::Uuid::new_v4(),"epoch":uuid::Uuid::new_v4(),"schema":version}),
                )
                .unwrap()
                .into(),
            ],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        let volume = TaskVolume::open(stream, path).await.map_err(storage)?;
        let snapshot = volume.snapshot(&Query::Root, 0).await.map_err(storage)?;
        let mut cursor = snapshot.cursor;
        cursor.schema = version;
        let reset = volume
            .observe(Query::Root, Some(cursor), 0)
            .await
            .map_err(storage)?;
        assert!(reset.reset);
        assert!(reset.snapshot.unwrap().files.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn malformed_native_requests_publish_rejections_and_worker_keeps_following() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    let running = fixture.running(1).await?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let owner = fixture
        .host
        .journal_owner(running.task_id(), running.task_binding().1, Some(&io))
        .await?;
    let volume = TaskVolume::open(stream, StreamPath::new("malformed-search-files").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new(hex::encode(owner.task_id().into_bytes())).unwrap();
    let deep = format!("{{\"query\":{}0{}}}", "[".repeat(258), "]".repeat(258));
    let requests = [
        "{",
        "]",
        "{\"query\":}",
        deep.as_str(),
        r#"{"query":"large"}"#,
        r#"{"query":"packed"}"#,
        r#"{"query":"test"}"#,
    ];
    let maximum_files = requests.len();
    for (index, bytes) in requests.iter().enumerate() {
        let snapshot = volume
            .snapshot(&Query::Root, maximum_files * 4)
            .await
            .map_err(storage)?;
        volume
            .append(
                snapshot.generation,
                &FileValue::new(
                    FileKey::Task {
                        task: task.clone(),
                        file: TaskFile::Request(Component::new(format!("search-{index}")).unwrap()),
                    },
                    bytes.as_bytes().to_vec(),
                ),
            )
            .await
            .map_err(storage)?;
    }
    let mut packed = SearchResponse {
        results: vec![super::super::SearchResult {
            url: "https://example.com".into(),
            title: "source".into(),
            snippet: String::new(),
        }],
    };
    let metadata = serde_json::to_vec(
        &json!({"key":file(&task, &Component::new("search-5").unwrap(), true),"channel":null}),
    )
    .unwrap()
    .len()
        + 12;
    let overhead = serde_json::to_vec(&SearchOutcome::Succeeded {
        response: packed.clone(),
    })
    .unwrap()
    .len();
    packed.results[0].snippet =
        "x".repeat(acyclic_stream::MAX_RECORD_BYTES - metadata - overhead + 1);
    let script = Arc::new(ScriptedSearchProvider::new([
        ScriptedSearch {
            request: SearchRequest {
                query: "large".into(),
                max_results: 5,
            },
            response: Some(SearchResponse {
                results: vec![super::super::SearchResult {
                    url: "https://example.com".into(),
                    title: "source".into(),
                    snippet: "x".repeat(65_536),
                }],
            }),
            error: None,
        },
        ScriptedSearch {
            request: SearchRequest {
                query: "packed".into(),
                max_results: 5,
            },
            response: Some(packed),
            error: None,
        },
        ScriptedSearch {
            request: SearchRequest {
                query: "test".into(),
                max_results: 5,
            },
            response: Some(SearchResponse { results: vec![] }),
            error: None,
        },
    ])?);
    let mut clients = Vec::new();
    for index in 0..requests.len() {
        clients.push(
            volume
                .observe(
                    Query::File(file(
                        &task,
                        &Component::new(format!("search-{index}")).unwrap(),
                        true,
                    )),
                    None,
                    1,
                )
                .await
                .map_err(storage)?,
        );
    }
    let worker = follow(
        &volume,
        &owner,
        || {
            Ok(Some(
                Arc::new(NativeOnly(script.clone())) as Arc<dyn SearchProvider>
            ))
        },
        Some(&work),
        maximum_files,
    );
    let results = async {
        for client in &mut clients {
            published(client).await?;
        }
        Ok::<_, Error>(())
    };
    tokio::select! { biased;
        result = worker => panic!("worker stopped on malformed input: {result:?}"),
        result = results => result?,
    }
    assert_eq!(script.remaining(), 0);
    let snapshot = volume
        .snapshot(&Query::Root, maximum_files * 4)
        .await
        .map_err(storage)?;
    assert_eq!(snapshot.files.len(), maximum_files * 3);
    assert_eq!(
        volume
            .snapshot(&Query::Requests(task.clone()), maximum_files)
            .await
            .map_err(storage)?
            .files
            .len(),
        requests.len(),
        "claims and results consumed submission capacity",
    );
    for index in 0..requests.len() {
        let key = file(
            &task,
            &Component::new(format!("search-{index}")).unwrap(),
            true,
        );
        let result = snapshot
            .files
            .iter()
            .find(|value| value.key == key)
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<SearchOutcome>(&result.bytes).unwrap(),
            if index == requests.len() - 1 {
                SearchOutcome::Succeeded {
                    response: SearchResponse { results: vec![] },
                }
            } else {
                SearchOutcome::Rejected
            }
        );
    }
    // A fixed syntax rejection must not hide exhaustion of the original ledger.
    for read_denial in [true, false] {
        let mut budget = acyclic_fs::performance::BOUNDARY_BUDGET;
        if read_denial {
            budget.source_bytes_read = 0;
        } else {
            budget.items_examined = 0;
        }
        let denied = acyclic_fs::NativeMaterializationWork::new(
            budget,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let failure = request(b"{", Some(&denied)).unwrap_err();
        assert!(failure.is_native_failure(), "{failure:?}");
    }
    let denied = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            bytes_encoded: 1,
            ..acyclic_fs::performance::BOUNDARY_BUDGET
        },
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let failure = encode(&SearchOutcome::Rejected, Some(&denied)).unwrap_err();
    assert!(failure.is_native_failure(), "{failure:?}");
    Ok(())
}
