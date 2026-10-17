//! Schedule the actual consumer at immutable payload-store boundaries. Provider
//! operations remain atomic; dependency-internal mutexes are not loom models.
use super::{
    session_runtime::{Fixture, VolumeBinding},
    *,
};
use acyclic_harness::{
    OperationId, TaskId,
    conversation::{BoundedContentFuture, ContentIo, FileRef},
    distributed::{SchedulerPayloadStore, WorkPull, Worker},
    scheduler::{LeaseFence, ResourceRequest},
    task_volume::{Component, FileValue, Query},
};
use std::{collections::BTreeMap, sync::Arc};

#[test]
fn competing_native_deliveries_cannot_overwrite_an_accepted_workflow_input() {
    model(false);
}

#[test]
#[should_panic(expected = "distinct requests shared one accepted input")]
fn negative_control_detects_a_payload_store_aliasing_distinct_resume_inputs() {
    model(true);
}

// The negative control deliberately violates immutable payload identity. The
// model must detect an acknowledgement whose input belongs to another request.
struct Payloads {
    inner: Arc<dyn SchedulerPayloadStore>,
    alias_inputs: bool,
    // Test-only fault state; no lock is held across an await or another lock.
    first: shuttle::sync::Mutex<Option<FileRef>>,
}

impl SchedulerPayloadStore for Payloads {
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, FileRef>> {
        self.inner.stage(operation, key, bytes, io)
    }

    fn stage_input<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, FileRef>> {
        let staged = self.inner.stage_input(operation, key, bytes, io)?;
        BoundedContentFuture::new(io, || async move {
            if key.starts_with("external-resume:") {
                shuttle::future::yield_now().await;
            }
            let file = staged.await?;
            if self.alias_inputs && key.starts_with("external-resume:") {
                return Ok(self.first.lock().unwrap().get_or_insert(file).clone());
            }
            Ok(file)
        })
    }
}

fn model(broken: bool) {
    shuttle::check_dfs(
        move || {
            shuttle::future::block_on(Box::pin(async move {
                let fixture = Arc::new(Fixture::new(VolumeBinding::Separate).await.unwrap());
                let task = TaskId::from_bytes([77; 16]);
                let folder = Component::new(task.to_string()).unwrap();
                let start = request(
                    ExternalHost::Pi,
                    "session_start",
                    json!({"sessionId":"session","cwd":"/project"}),
                )
                .unwrap();
                let file = publish(&fixture, &folder, &start).await;
                consumer(&fixture, fixture.publisher.as_ref())
                    .accept(&folder, &file)
                    .await
                    .unwrap();
                let fence = claim(&fixture, task).await;
                suspend(&fixture, task, &fence, 2).await;
                let payloads = Arc::new(Payloads {
                    inner: fixture.publisher.clone(),
                    alias_inputs: broken,
                    first: shuttle::sync::Mutex::new(None),
                });
                let mut requests = Vec::new();
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
                    let file = publish(&fixture, &folder, &native).await;
                    requests.push((native, file));
                }
                let mut deliveries = Vec::new();
                for (_, file) in &requests {
                    let fixture = fixture.clone();
                    let payloads = payloads.clone();
                    let folder = folder.clone();
                    let file = file.clone();
                    deliveries.push(shuttle::future::spawn(async move {
                        consumer(&fixture, payloads.as_ref())
                            .accept(&folder, &file)
                            .await
                    }));
                }
                let mut accepted = Vec::new();
                for delivery in deliveries {
                    let result = delivery.await.unwrap();
                    assert!(result.is_ok() || matches!(result, Err(Error::Conflict(_))));
                    accepted.push(result.is_ok());
                }
                assert_eq!(
                    accepted.iter().filter(|&&ok| ok).count(),
                    1,
                    "distinct requests shared one accepted input"
                );
                let fence = claim(&fixture, task).await;
                let expected = requests
                    .iter()
                    .zip(&accepted)
                    .find(|(_, admitted)| **admitted)
                    .unwrap()
                    .0;
                let revision = assert_input(&fixture, task, &fence, &expected.0).await;
                suspend(&fixture, task, &fence, revision + 1).await;
                for ((native, file), admitted) in requests.iter().zip(accepted) {
                    if !admitted {
                        consumer(&fixture, fixture.publisher.as_ref())
                            .accept(&folder, file)
                            .await
                            .unwrap();
                        let fence = claim(&fixture, task).await;
                        assert_input(&fixture, task, &fence, native).await;
                    }
                }
            }));
        },
        None,
    );
}

pub(super) fn consumer<'a>(
    fixture: &'a Fixture,
    payloads: &'a dyn SchedulerPayloadStore,
) -> RequestConsumer<
    'a,
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
> {
    RequestConsumer::new(
        &fixture.runtime,
        &fixture.requests,
        &fixture.results,
        payloads,
        "/project",
    )
}

pub(super) async fn publish(
    fixture: &Fixture,
    folder: &Component,
    native: &ExternalRequest,
) -> FileValue {
    let file = native.to_file(folder.clone()).unwrap();
    let snapshot = fixture
        .requests
        .snapshot(&Query::File(file.key.clone()), 1)
        .await
        .unwrap();
    fixture
        .requests
        .append(snapshot.generation, &file)
        .await
        .unwrap();
    file
}

// Model only intake and its retained slot. Full worker execution and result
// publication are exercised by intake.rs and session_runtime.rs. Preparing the
// real slot with the host's lease API introduces no competing worker effects.
pub(super) async fn claim(fixture: &Fixture, task: TaskId) -> LeaseFence {
    let worker = Worker {
        id: "native-model".into(),
        available: ResourceRequest::default(),
        labels: BTreeMap::new(),
    };
    let WorkPull::Claimed(lease) = fixture
        .runtime
        .task_host()
        .pull_work(
            &worker,
            Some(OperationId::from_bytes(task.into_bytes())),
            &mut None,
        )
        .await
        .unwrap()
    else {
        panic!("input was not claimable")
    };
    fixture
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await
        .unwrap();
    LeaseFence {
        reservation_id: lease.reservation.id,
        placement: lease.reservation.placement,
    }
}

pub(super) async fn suspend(fixture: &Fixture, task: TaskId, fence: &LeaseFence, revision: u64) {
    fixture
        .runtime
        .task_host()
        .suspend_workflow(task, fence, (revision, None), None, None, &mut None)
        .await
        .unwrap();
}

async fn assert_input(
    fixture: &Fixture,
    task: TaskId,
    fence: &LeaseFence,
    expected: &ExternalRequest,
) -> u64 {
    let (revision, input) = fixture
        .runtime
        .task_host()
        .workflow_input(task, fence, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        input["command"]["operation_id"],
        expected.operation_id.to_string(),
        "distinct requests shared one accepted input"
    );
    revision
}
