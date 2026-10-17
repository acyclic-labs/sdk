use super::{append, boundary_stream::BoundaryStream, *};
use acyclic_harness::task_volume::{Component, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

#[test]
fn concurrent_request_files_both_publish_without_losing_immutable_identity() {
    model(false);
}

#[test]
#[should_panic(expected = "independent request was rejected")]
fn negative_control_detects_rejecting_a_concurrent_volume_append() {
    model(true);
}

fn model(broken: bool) {
    shuttle::check_dfs(
        move || {
            shuttle::future::block_on(async move {
                let provider = Arc::new(BoundaryStream {
                    inner: Arc::new(MemoryStream::default()),
                    broken_follow: false,
                    owner: None,
                    broken_fence: false,
                    lose_result_ack: false,
                });
                let requests = Arc::new(
                    TaskVolume::open(provider, StreamPath::new("requests").unwrap())
                        .await
                        .unwrap(),
                );
                let task = Component::new("task").unwrap();
                let mut publishers = Vec::new();
                let mut originals = Vec::new();
                for call in ["first", "second"] {
                    let native = request(
                        ExternalHost::Pi,
                        "tool_call",
                        json!({
                            "sessionId":"session", "toolCallId":call, "toolName":"write",
                            "input":{"path":call,"content":call}
                        }),
                    )
                    .unwrap();
                    let file = native.to_file(task.clone()).unwrap();
                    originals.push(file.clone());
                    let requests = requests.clone();
                    publishers.push(shuttle::future::spawn(async move {
                        let result = if broken {
                            // Deliberately omit publication rebasing, as the former
                            // client did; retain the real snapshot and CAS append.
                            let cut = requests
                                .snapshot(&Query::File(file.key.clone()), 1)
                                .await
                                .unwrap();
                            requests.append(cut.generation, &file).await.map(|_| ())
                        } else {
                            append::publish(&requests, &file, None, None).await
                        };
                        assert!(
                            result.is_ok(),
                            "independent request was rejected: {result:?}"
                        );
                    }));
                }
                for publisher in publishers {
                    publisher.await.unwrap();
                }
                for file in originals {
                    let cut = requests
                        .snapshot(&Query::File(file.key.clone()), 1)
                        .await
                        .unwrap();
                    assert_eq!(cut.files, vec![file.clone()]);
                    append::publish(&requests, &file, None, None).await.unwrap();
                    let after = requests
                        .snapshot(&Query::File(file.key.clone()), 1)
                        .await
                        .unwrap();
                    assert_eq!(cut.generation, after.generation);
                    let mut changed = file.clone();
                    changed.bytes = b"changed".to_vec().into();
                    assert!(
                        append::publish(&requests, &changed, None, None)
                            .await
                            .is_err()
                    );
                }
            });
        },
        None,
    );
}
