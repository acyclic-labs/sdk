use super::*;
use proptest::prelude::*;
fn component(text: &str) -> Component {
    Component::new(text).unwrap()
}
fn key(task: &Component, file: TaskFile) -> FileKey {
    FileKey::Task {
        task: task.clone(),
        file,
    }
}
fn publish(index: &mut Index, task: &Component, file: TaskFile, bytes: &[u8]) {
    let position = index.next + 1;
    apply(
        index,
        FileValue::new(key(task, file), bytes.to_vec()),
        position,
    );
    index.next = position;
}

#[test]
fn approval_history_cannot_fill_the_session_snapshot() {
    let task = component("task");
    let mut index = Index::default();
    for i in 0..5000 {
        let request = component(&format!("request-{i}"));
        publish(
            &mut index,
            &task,
            TaskFile::ApprovalRequest(request.clone()),
            &[1; 1024],
        );
        publish(
            &mut index,
            &task,
            TaskFile::ApprovalReview {
                request: request.clone(),
                principal: component("human"),
            },
            b"waiting",
        );
        publish(
            &mut index,
            &task,
            TaskFile::ApprovalDecision {
                request,
                principal: component("human"),
            },
            b"allow",
        );
    }
    let pending = component("pending");
    publish(
        &mut index,
        &task,
        TaskFile::ApprovalRequest(pending.clone()),
        b"subject",
    );
    publish(
        &mut index,
        &task,
        TaskFile::ApprovalReview {
            request: pending,
            principal: component("human"),
        },
        b"waiting",
    );
    let query = Query::TaskSession {
        task: task.clone(),
        session: component("session"),
    };
    for query in [query, task_view(&task)] {
        let files = recent::snapshot(&index, &query, 8).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|file| query.matches(&file.key)));
        assert_eq!(files.iter().map(|file| file.bytes.len()).sum::<usize>(), 14);
    }
    // Restoring the historical-copy loop violates both shipping view bounds.
    let archived: Vec<_> = index
        .files
        .range(Query::Approvals(task.clone()).ranges().remove(0))
        .collect();
    assert!(archived.len() > 4096);
    assert!(
        archived
            .iter()
            .map(|(_, file)| file.file.bytes.len())
            .sum::<usize>()
            > 4 * 1024 * 1024
    );
    // Settled history remains addressable through the same canonical keys.
    let exact = Query::Approval {
        task: task.clone(),
        request: component("request-0"),
    };
    assert_eq!(
        Query::folder(".tasks/task/approvals/request-0").unwrap(),
        exact
    );
    assert_eq!(
        exact
            .ranges()
            .into_iter()
            .flat_map(|range| index.files.range(range))
            .filter(|(key, _)| exact.matches(key))
            .count(),
        3
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn approval_pending_index_retires_only_the_decided_binding(events in prop::collection::vec((0u8..4, any::<bool>()), 1..100)) {
        let task = component("task");
        let other = component("other");
        let mut index = Index::default();
        let mut decided = BTreeSet::new();
        let mut admitted = BTreeSet::new();
        for (id, settle) in events {
            let request = component(&format!("request-{id}"));
            publish(&mut index, &task, TaskFile::ApprovalRequest(request.clone()), b"subject");
            admitted.insert(request.clone());
            if settle {
                publish(&mut index, &task, TaskFile::ApprovalDecision { request: request.clone(), principal: component("human") }, b"deny");
                decided.insert(request.clone());
            }
            // A late/replayed review cannot resurrect a decided request.
            publish(&mut index, &task, TaskFile::ApprovalReview { request: request.clone(), principal: component("human") }, b"waiting");
            publish(&mut index, &other, TaskFile::ApprovalRequest(request), b"other task");
            let actual: BTreeSet<_> = index.approvals.files(&index, &task).into_iter().filter_map(|file| match file.key {
                FileKey::Task { file: TaskFile::ApprovalRequest(id), .. } => Some(id), _ => None,
            }).collect();
            let expected: BTreeSet<_> = admitted.difference(&decided).cloned().collect();
            prop_assert_eq!(&actual, &expected);
            // Negative control: retaining every request differs as soon as a
            // decision occurs; another task's same ID must remain pending.
            prop_assert_eq!(actual == admitted, decided.is_empty());
            prop_assert!(!index.approvals.files(&index, &other).is_empty());
        }
    }
}

fn task_view(task: &Component) -> Query {
    Query::TaskView {
        task: task.clone(),
        recent: 2,
        reasoning: false,
        details: false,
        terminal: false,
    }
}

#[tokio::test]
async fn approval_live_window_matches_snapshot_after_settlement() {
    let task = component("task");
    for query in [
        Query::TaskSession {
            task: task.clone(),
            session: component("session"),
        },
        task_view(&task),
    ] {
        check_live_window(query).await;
    }
}

async fn check_live_window(query: Query) {
    use futures::StreamExt as _;
    let task = component("task");
    let volume = TaskVolume::open(
        std::sync::Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_stream::StreamPath::new("approval-live-window").unwrap(),
    )
    .await
    .unwrap();
    let mut generation = volume.snapshot(&Query::Root, 16).await.unwrap().generation;
    let pending = key(
        &task,
        TaskFile::ApprovalRequest(component("old-unanswered")),
    );
    generation = volume
        .append(
            generation,
            &FileValue::new(pending.clone(), b"pending".to_vec()),
        )
        .await
        .unwrap();
    let observed = volume
        .observe_files(query.clone(), None, 8, 1024)
        .await
        .unwrap();
    let snapshot = observed.snapshot.unwrap();
    let mut old_feed = volume.follow(query.clone(), snapshot.cursor).await.unwrap();
    let mut live: BTreeMap<_, _> = snapshot
        .files
        .into_iter()
        .map(|file| (file.key.clone(), file))
        .collect();
    let mut historical = live.clone();
    let mut changes = observed.changes;
    for id in 0..16 {
        let request = component(&format!("request-{id}"));
        for file in [
            TaskFile::ApprovalRequest(request.clone()),
            TaskFile::ApprovalReview {
                request: request.clone(),
                principal: component("human"),
            },
            TaskFile::ApprovalDecision {
                request: request.clone(),
                principal: component("human"),
            },
            TaskFile::ApprovalReview {
                request,
                principal: component("late"),
            },
        ] {
            generation = volume
                .append(
                    generation,
                    &FileValue::new(key(&task, file), b"value".to_vec()),
                )
                .await
                .unwrap();
            let raw = old_feed.next().await.unwrap().unwrap();
            if let Some(file) = raw.file {
                historical.insert(file.key.clone(), file);
            }
            let change = changes.next().await.unwrap().unwrap();
            for key in change.removed {
                live.remove(&key);
            }
            for file in change.file.into_iter().chain(change.files) {
                live.insert(file.key.clone(), file);
            }
        }
        let fresh = volume.snapshot(&query, 8).await.unwrap();
        assert_eq!(
            live.keys().cloned().collect::<Vec<_>>(),
            fresh
                .files
                .into_iter()
                .map(|file| file.key)
                .collect::<Vec<_>>()
        );
        assert!(live.contains_key(&pending));
        assert_eq!(live.len(), 1);
    }
    assert!(
        historical.len() > 8,
        "the raw historical follow must exceed the live bound"
    );
    // A durable settlement covers its earlier request in the same prefix.
    // Reopening publication must confirm that receipt, not append a request
    // after its decision and resurrect it in the live view.
    let replay = FileValue::new(
        key(&task, TaskFile::ApprovalRequest(component("request-0"))),
        b"value".to_vec(),
    );
    super::super::publish_durable(&volume, &replay, None)
        .await
        .unwrap();
    assert_eq!(
        volume.snapshot(&query, 8).await.unwrap().generation,
        generation
    );
    assert_eq!(
        volume
            .snapshot(
                &Query::Approval {
                    task,
                    request: component("request-0")
                },
                8
            )
            .await
            .unwrap()
            .files
            .len(),
        4
    );
}

#[tokio::test]
async fn approval_live_window_refuses_overfull_pending_set_instead_of_eviction() {
    let task = component("task");
    for query in [
        Query::TaskSession {
            task: task.clone(),
            session: component("session"),
        },
        task_view(&task),
    ] {
        check_pending_bound(query).await;
    }
}

async fn check_pending_bound(query: Query) {
    use futures::StreamExt as _;
    let task = component("task");
    let volume = TaskVolume::open(
        std::sync::Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_stream::StreamPath::new("approval-pending-bound").unwrap(),
    )
    .await
    .unwrap();
    let mut generation = volume.snapshot(&Query::Root, 8).await.unwrap().generation;
    generation = volume
        .append(
            generation,
            &FileValue::new(
                key(&task, TaskFile::ApprovalRequest(component("first"))),
                b"first!".to_vec(),
            ),
        )
        .await
        .unwrap();
    let mut follow = volume
        .observe_files(query.clone(), None, 8, 8)
        .await
        .unwrap()
        .changes;
    assert!(
        matches!(
            volume.observe_files(query.clone(), None, 8, 5).await,
            Err(Error::LimitExceeded)
        ),
        "an individually oversized unanswered request must be refused, not omitted"
    );
    volume
        .append(
            generation,
            &FileValue::new(
                key(&task, TaskFile::ApprovalRequest(component("second"))),
                b"second".to_vec(),
            ),
        )
        .await
        .unwrap();
    assert!(
        matches!(follow.next().await.unwrap(), Err(Error::LimitExceeded)),
        "a live unanswered request must not be evicted to admit another"
    );
    assert!(
        matches!(
            volume.observe_files(query, None, 8, 8).await,
            Err(Error::LimitExceeded)
        ),
        "snapshot and follow must enforce the same pending-set bound"
    );
    assert_eq!(
        volume
            .snapshot(&Query::Approvals(task), 8)
            .await
            .unwrap()
            .files
            .len(),
        2,
        "the refusing view cannot discard either canonical request"
    );
}
