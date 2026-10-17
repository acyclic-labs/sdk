//! Requests cannot mutate owner results or internal scheduler streams.
use super::*;
use proptest::prelude::*;

#[test]
fn client_commit_authority_never_widens_to_owner_paths() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(
        runtime
            .block_on(LocalStream::open(directory.path(), Default::default()))
            .unwrap(),
    );
    let client = ClientStream::workspace(Arc::clone(&local));
    let task = crate::TaskId::from_bytes([1; 16]);
    let writable = super::super::super::local_paths::path("external/requests", task);
    runtime
        .block_on(local.append(AppendRequest {
            path: writable,
            records: vec![b"request header".as_slice().into()],
            if_tail: Some(0),
            idempotency_key: None,
        }))
        .unwrap();
    let protected = super::super::super::local_paths::path("external/results", task);
    runtime
        .block_on(local.append(AppendRequest {
            path: protected.clone(),
            records: vec![b"owner".as_slice().into()],
            if_tail: Some(0),
            idempotency_key: None,
        }))
        .unwrap();
    let strategy = prop_oneof![
        Just(format!("external/requests/{task}")),
        Just(format!("external/results/{task}")),
        Just(format!("external/requests/{task}/private")),
        Just(format!("external/requests/{{{task}}}")),
        Just(format!(
            "external/requests/{}",
            uuid::Uuid::from_bytes(task.into_bytes()).simple()
        )),
        Just("external/requests".to_owned()),
        Just("external/results".to_owned()),
        Just("owner/control".to_owned()),
        "[a-z]{1,24}/[a-z]{1,24}",
    ];
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(24))
        .run(&strategy, |name| {
            let path = StreamPath::new(&name).unwrap();
            prop_assert_eq!(
                client.children_readable(Some(&path)).is_ok(),
                name == "external/requests" || name == "external/results"
            );
            prop_assert!(client.children_readable(None).is_err());
            let children = runtime.block_on(client.children_page(ChildrenPageRequest {
                parent: Some(path.clone()),
                after: None,
                hierarchy_version: None,
                limit: 1,
            }));
            if name == "external/requests" || name == "external/results" {
                prop_assert_eq!(children?.children.len(), 1);
                prop_assert_eq!(
                    runtime.block_on(client.children_page(ChildrenPageRequest {
                        parent: Some(path.clone()),
                        after: Some(StreamPath::new("scheduler/private").unwrap()),
                        hierarchy_version: None,
                        limit: 1,
                    })),
                    Err(StreamError::AccessDenied)
                );
            } else {
                prop_assert_eq!(children, Err(StreamError::AccessDenied));
            }
            prop_assert_eq!(
                runtime.block_on(
                    client.inspect_idempotency(
                        IdempotencyKey::new(name.as_bytes().to_vec()).unwrap()
                    )
                ),
                Err(StreamError::AccessDenied)
            );
            let request = CommitRequest {
                conditions: vec![CommitCondition::Tail {
                    path: path.clone(),
                    expected: runtime.block_on(local.tail(path.clone())).unwrap_or(0),
                }],
                mutations: vec![CommitMutation::Append {
                    path: path.clone(),
                    records: vec![b"caller".as_slice().into()],
                }],
                idempotency_key: IdempotencyKey::new(uuid::Uuid::new_v4().as_bytes().to_vec())
                    .unwrap(),
            };
            let result = runtime.block_on(client.commit(request));
            if name == format!("external/requests/{task}") {
                prop_assert!(
                    matches!(result, Ok(CommitOutcome::Committed(_))),
                    "{result:?}"
                );
            } else {
                prop_assert_eq!(result, Err(StreamError::AccessDenied));
            }
            prop_assert_eq!(runtime.block_on(local.tail(protected.clone())).unwrap(), 1);
            prop_assert_eq!(
                runtime.block_on(client.tail(StreamPath::new("scheduler/private").unwrap())),
                Err(StreamError::AccessDenied)
            );
            Ok(())
        })
        .unwrap();
}
