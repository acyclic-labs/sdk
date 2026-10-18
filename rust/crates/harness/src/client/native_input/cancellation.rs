use super::*;
use crate::{
    TaskId,
    client::{CommandFileRoot, CommandSession, TaskFileClient},
    task_volume::{Component, FileKey, FileValue, Query, TaskVolume},
    tui::{
        ClientRenderer, PromptInput,
        ratatui::{Terminal, backend::TestBackend, widgets::Paragraph},
    },
    tui_files::CompletionFiles,
    wire,
};
use acyclic_stream::*;
use futures::{
    FutureExt,
    future::{BoxFuture, poll_fn},
};
use shuttle::sync::{Arc as ModelArc, Condvar, Mutex};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Poll, Waker},
};

type StreamResult<T> = std::result::Result<T, StreamError>;
struct Provider {
    inner: MemoryStream,
    pause: Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,
}
#[async_trait::async_trait]
impl StreamProvider for Provider {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> StreamResult<Option<IdempotencyObservation>> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> StreamResult<u64> {
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> StreamResult<StreamBounds> {
        self.inner.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> StreamResult<StreamWatermarks> {
        (self.pause)().await;
        self.inner.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> StreamResult<AppendOutcome> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> StreamResult<ForkReceipt> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> StreamResult<RecordStream> {
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> StreamResult<RecordStream> {
        self.inner.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        commit: CommitId,
    ) -> StreamResult<RecordStream> {
        self.inner.follow_checked(path, from, commit).await
    }
    async fn children(&self, request: ChildrenRequest) -> StreamResult<ChildStream> {
        self.inner.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> StreamResult<CommitOutcome> {
        self.inner.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> StreamResult<CommittedEnvelope> {
        self.inner.read_commit(id).await
    }
}

fn model<const RETAIN: bool>(return_to_initial: bool) {
    // The sole consumer owns every provider operation. The other actor releases
    // only this modeled I/O gate: no uninstrumented provider lock is contended.
    // Lock order: gate only, released before provider I/O or waking the consumer.
    shuttle::check_dfs(
        move || {
            shuttle::future::block_on(async {
                // Only the consumer arms and retires this local provider hook.
                // Concurrent release/wakeup state lives solely in the modeled
                // gate below; no schedule can change this flag from elsewhere.
                let enabled = Arc::new(AtomicBool::new(false));
                let gate =
                    ModelArc::new((Mutex::new((false, false, None::<Waker>)), Condvar::new()));
                let pause = {
                    let enabled = enabled.clone();
                    let gate = gate.clone();
                    Arc::new(move || -> BoxFuture<'static, ()> {
                        let enabled = enabled.clone();
                        let gate = gate.clone();
                        Box::pin(async move {
                            if enabled.load(Ordering::SeqCst) {
                                poll_fn(move |cx| {
                                    let mut state = gate.0.lock().unwrap();
                                    state.0 = true;
                                    gate.1.notify_all();
                                    if state.1 {
                                        // This consumer has observed the permanent
                                        // release. Later I/O cannot block here.
                                        enabled.store(false, Ordering::SeqCst);
                                        Poll::Ready(())
                                    } else {
                                        state.2 = Some(cx.waker().clone());
                                        Poll::Pending
                                    }
                                })
                                .await;
                            }
                        })
                    })
                };
                let provider = Arc::new(Provider {
                    inner: MemoryStream::default(),
                    pause,
                });
                let volume = Arc::new(
                    TaskVolume::open(provider, StreamPath::new("selection-cancellation").unwrap())
                        .await
                        .unwrap(),
                );
                let initial = TaskId::from_bytes([5; 16]);
                let next = TaskId::from_bytes([7; 16]);
                for (task, marker) in [(initial, "initial"), (next, "selected")] {
                    let settings = FileKey::Ui {
                        directory: Component::new(format!("task-{task}")).unwrap(),
                        name: Component::new("settings.json").unwrap(),
                    };
                    let cut = volume.snapshot(&Query::Root, 128).await.unwrap().generation;
                    volume
                        .append(
                            cut,
                            &FileValue::new(
                                settings,
                                serde_json::to_vec(&serde_json::json!({"marker":marker})).unwrap(),
                            ),
                        )
                        .await
                        .unwrap();
                }
                let session = uuid::Uuid::from_bytes([6; 16]);
                let mut commands = CommandSession::open(volume.clone(), session, 65536, 128)
                    .await
                    .unwrap();
                commands
                    .write_state(
                        CommandFileRoot::Session,
                        "layout.json",
                        &serde_json::json!({"selected_task":initial.to_string()}),
                    )
                    .await
                    .unwrap();
                let task = if return_to_initial { initial } else { next };
                let owner = wire::Authority {
                    kind: wire::AggregateKind::Task as i32,
                    id: task.to_string(),
                };
                let mut client = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65536)
                    .await
                    .unwrap();
                client.follow_commands(&commands).await.unwrap();
                let mut prompt = PromptInput::default();
                let mut completions =
                    CompletionFiles::observe(volume.as_ref(), &mut prompt, None, 128)
                        .await
                        .unwrap();
                let mut observed = volume.observe(Query::Inputs, None, 128).await.unwrap();
                client
                    .views_mut()
                    .query_snapshot(Query::Inputs, observed.snapshot.take().unwrap(), 128, 65536)
                    .unwrap();
                let mut documents = observed.changes;
                let layout = FileKey::Ui {
                    directory: Component::new(format!("session-{session}")).unwrap(),
                    name: Component::new("layout.json").unwrap(),
                };
                for selected in std::iter::once(next).chain(return_to_initial.then_some(initial)) {
                    let cut = volume.snapshot(&Query::Root, 128).await.unwrap().generation;
                    volume
                        .append(
                            cut,
                            &FileValue::new(
                                layout.clone(),
                                serde_json::to_vec(
                                    &serde_json::json!({"selected_task":selected.to_string()}),
                                )
                                .unwrap(),
                            ),
                        )
                        .await
                        .unwrap();
                }
                enabled.store(true, Ordering::SeqCst);
                let releasing = gate.clone();
                let producer = shuttle::thread::spawn(move || {
                    let mut state = releasing.0.lock().unwrap();
                    while !state.0 {
                        state = releasing.1.wait(state).unwrap();
                    }
                    state.1 = true;
                    let waker = state.2.take();
                    drop(state);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                });
                let mut input = futures::stream::pending();
                let mut renderer = ClientRenderer::default();
                // Completion has a ready checkpoint precisely while the selected
                // task's actual settings snapshot is blocked at provider I/O.
                assert!(
                    next_event::<_, false, RETAIN>(
                        &mut client,
                        &mut input,
                        &mut completions,
                        &mut prompt,
                        &mut commands,
                        false,
                        &mut renderer,
                        &mut documents
                    )
                    .await
                    .unwrap()
                    .is_none()
                );
                assert_eq!(commands.selected_task().unwrap(), Some(task));
                let settings = commands
                    .state(CommandFileRoot::Task, "settings.json")
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    settings["marker"],
                    if return_to_initial {
                        "initial"
                    } else {
                        "selected"
                    }
                );
                let mut terminal = Terminal::new(TestBackend::new(24, 2)).unwrap();
                let mut draws = 0;
                terminal
                    .draw(|frame| {
                        draws += 1;
                        frame.render_widget(
                            Paragraph::new(settings["marker"].as_str().unwrap()),
                            frame.area(),
                        );
                    })
                    .unwrap();
                assert_eq!(draws, 1);
                assert!(
                    next_event::<_, false, RETAIN>(
                        &mut client,
                        &mut input,
                        &mut completions,
                        &mut prompt,
                        &mut commands,
                        false,
                        &mut renderer,
                        &mut documents
                    )
                    .now_or_never()
                    .is_none()
                );
                producer.join().unwrap();
            });
        },
        None,
    );
}

#[test]
fn task_selection_and_settings_survive_checkpoint_cancellation() {
    for return_to_initial in [false, true] {
        model::<true>(return_to_initial);
    }
}
#[test]
fn negative_control_consumed_selection_can_be_lost_during_metadata_io() {
    let failure = std::panic::catch_unwind(|| model::<false>(false))
        .expect_err("unretained selection must deadlock");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains("deadlock"), "{text}");
}
