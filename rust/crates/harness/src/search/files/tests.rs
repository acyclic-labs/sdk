use super::*;
use crate::{
    durable_host::tests::fixture::Fixture,
    test_support::{ScriptedSearch, ScriptedSearchProvider},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::FutureExt as _;
use serde_json::json;
use std::cell::Cell;

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
            .snapshot(&Query::Requests(task.clone()), 3)
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
            .snapshot(&Query::Requests(task.clone()), 3)
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
                let change =
                    tokio::time::timeout(std::time::Duration::from_secs(1), client.changes.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .map_err(storage)?
                        .file
                        .unwrap();
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
                    .snapshot(&Query::Requests(task.clone()), 3)
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
                .snapshot(&Query::Requests(task.clone()), 6)
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
            let observed =
                tokio::time::timeout(std::time::Duration::from_secs(1), client.changes.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .map_err(storage)?
                    .file
                    .unwrap();
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
    assert!(
        follow(
            &volume,
            &owner,
            || panic!("same-owner replay redispatched"),
            None,
            6
        )
        .now_or_never()
        .is_none()
    );
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
            .snapshot(&Query::Requests(task.clone()), 6)
            .await
            .map_err(storage)?;
        volume
            .append(snapshot.generation, &value)
            .await
            .map_err(storage)?;
    }
    assert!(
        follow(
            &volume,
            &old,
            || panic!("active claim redispatched"),
            None,
            6
        )
        .now_or_never()
        .is_none()
    );
    let replacement = fixture.replace(1).await?;
    let recovering = follow(
        &volume,
        &replacement,
        || panic!("replacement redispatched uncertain HTTP request"),
        None,
        6,
    );
    assert!(recovering.now_or_never().is_none());
    let snapshot = volume
        .snapshot(&Query::File(file(&task, &id, true)), 1)
        .await
        .map_err(storage)?;
    assert_eq!(
        serde_json::from_slice::<SearchOutcome>(&snapshot.files[0].bytes).unwrap(),
        SearchOutcome::Indeterminate
    );
    let snapshot = volume
        .snapshot(&Query::Requests(task.clone()), 6)
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

#[tokio::test]
async fn file_worker_preserves_native_provider_custody_and_terminal_replay() -> Result<()> {
    let stream = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(stream, StreamPath::new("native-search-files").unwrap())
        .await
        .map_err(storage)?;
    let task = Component::new("native").unwrap();
    let initial = volume
        .snapshot(&Query::Requests(task.clone()), 3)
        .await
        .map_err(storage)?;
    volume
        .append(
            initial.generation,
            &FileValue::new(
                FileKey::parse(".tasks/native/requests/search-native.json").unwrap(),
                br#"{"query":"test"}"#.as_slice(),
            ),
        )
        .await
        .map_err(storage)?;
    let backend = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: SearchRequest {
            query: "test".into(),
            max_results: 5,
        },
        response: Some(SearchResponse { results: vec![] }),
        error: None,
    }])?);
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let fence = LeaseFence {
        reservation_id: "native".into(),
        placement: "host".into(),
    };
    drive_for_test(
        &volume,
        &task,
        &fence,
        || Ok(Some(backend.clone())),
        vec![],
        Some(&work),
        3,
    )
    .await?;
    assert_eq!(backend.remaining(), 0);
    let snapshot = volume
        .snapshot(&Query::Requests(task.clone()), 3)
        .await
        .map_err(storage)?;
    assert_eq!(snapshot.files.len(), 3);
    assert!(work.receipt().source_bytes_read > 0);
    assert!(work.receipt().bytes_encoded > 0);
    drive_for_test(
        &volume,
        &task,
        &fence,
        || panic!("native replay contacted provider"),
        vec![],
        Some(&work),
        3,
    )
    .await?;
    assert_eq!(
        volume
            .snapshot(&Query::Requests(task), 3)
            .await
            .map_err(storage)?
            .files,
        snapshot.files
    );
    Ok(())
}
