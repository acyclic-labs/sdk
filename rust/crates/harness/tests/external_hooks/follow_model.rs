//! Model the client at the provider's atomic snapshot/follow handoff. Provider
//! operations are contract-atomic; this does not model dependency-internal locks.
use acyclic_harness::{
    external_executor::{ExternalHost, request},
    task_volume::{Component, Query, TaskVolume},
};
use acyclic_stream::*;
use futures::StreamExt as _;
use serde_json::json;
use std::sync::Arc;

use super::boundary_stream::BoundaryStream;

#[test]
fn client_never_loses_an_answer_at_the_snapshot_follow_handoff() {
    model(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_detects_following_the_new_tail_instead_of_the_snapshot_cursor() {
    model(true);
}

fn model(broken: bool) {
    shuttle::check_dfs(
        move || {
            let result: Result<(), Box<dyn std::error::Error + Send + Sync>> =
                shuttle::future::block_on(async move {
                    let provider = Arc::new(MemoryStream::default());
                    let requests = Arc::new(
                        TaskVolume::open(provider.clone(), StreamPath::new("requests")?).await?,
                    );
                    let results = Arc::new(
                        TaskVolume::open(provider.clone(), StreamPath::new("results")?).await?,
                    );
                    let client_results = TaskVolume::open(
                        Arc::new(BoundaryStream {
                            inner: provider,
                            broken_follow: broken,
                            owner: None,
                            broken_fence: false,
                            lose_result_ack: false,
                        }),
                        StreamPath::new("results")?,
                    )
                    .await?;
                    let task = Component::new("task")?;
                    let mut incoming = requests
                        .observe(Query::Requests(task.clone()), None, 1)
                        .await?
                        .changes;
                    let client_requests = requests.clone();
                    let client_task = task.clone();
                    let client = shuttle::future::spawn(async move {
                        let request = request(
                            ExternalHost::Codex,
                            "SessionEnd",
                            json!({"session_id":"session"}),
                        )?;
                        let call = request.send(&client_requests, &client_results, client_task);
                        assert!(std::mem::size_of_val(&call) < 16 * 1024);
                        assert_eq!(call.await?, json!({"detached":true}));
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                    });
                    let actor = shuttle::future::spawn(async move {
                        let file = incoming
                            .next()
                            .await
                            .ok_or("no request")??
                            .file
                            .ok_or("no request file")?;
                        let request =
                            acyclic_harness::external_executor::ExternalRequest::from_file(
                                &task, &file,
                            )?;
                        shuttle::future::yield_now().await;
                        let answer = request.result_file(task, &json!({"detached":true}))?;
                        let snapshot = results
                            .snapshot(&Query::File(answer.key.clone()), 1)
                            .await?;
                        results.append(snapshot.generation, &answer).await?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                    });
                    client.await.map_err(|error| format!("client: {error}"))??;
                    actor.await.map_err(|error| format!("actor: {error}"))??;
                    Ok(())
                });
            assert!(result.is_ok(), "model failed: {result:?}");
        },
        None,
    );
}
