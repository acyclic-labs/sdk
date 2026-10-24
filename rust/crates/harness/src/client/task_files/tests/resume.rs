use super::*;
use crate::task_volume::{Cursor, Observation};
use acyclic_stream::BoxProviderFuture;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

type Attempt = (Query, Option<Cursor>, usize, usize);
#[derive(Clone)]
struct Source {
    volume: Arc<TaskVolume<MemoryStream>>,
    attempts: Arc<Mutex<Vec<Attempt>>>,
    count: Arc<AtomicUsize>,
    pause: bool,
    invalid: bool,
    closed: bool,
}
impl FileQuerySource for Source {
    fn retain(&self) -> Arc<dyn FileQuerySource> {
        Arc::new(self.clone())
    }
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        files: usize,
        bytes: usize,
    ) -> BoxProviderFuture<'_, crate::task_volume::Result<Observation>> {
        Box::pin(async move {
            let attempt = self.count.fetch_add(1, Ordering::SeqCst);
            self.attempts.lock().expect("test journal").push((
                query.clone(),
                cursor.clone(),
                files,
                bytes,
            ));
            if attempt == 1 && self.pause {
                return futures::future::pending().await;
            }
            let mut observed = self
                .volume
                .observe_files(query, cursor, files, bytes)
                .await?;
            if attempt == 0 || self.closed {
                observed.changes = Box::pin(futures::stream::empty());
            }
            if attempt != 0
                && self.invalid
                && let Some(snapshot) = &mut observed.snapshot
            {
                snapshot.generation.commit[0] ^= 1;
            }
            Ok(observed)
        })
    }
}
async fn fixture() -> Source {
    Source {
        volume: Arc::new(
            TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("resume-client").expect("path"),
            )
            .await
            .expect("volume"),
        ),
        attempts: Arc::default(),
        count: Arc::default(),
        pause: false,
        invalid: false,
        closed: false,
    }
}
async fn append(source: &Source, task: crate::TaskId) {
    source
        .volume
        .append(
            source.volume.head().await.expect("cut"),
            &FileValue {
                requester: None,
                publication: None,
                key: FileKey::Task {
                    task: Component::new(task.to_string()).expect("task"),
                    file: TaskFile::Chunk {
                        turn: 0,
                        sequence: 0,
                    },
                },
                bytes: b"resumed".to_vec().into(),
                channel: Some(Channel::Text),
            },
        )
        .await
        .expect("append");
}
async fn check<const RESUME: bool>(window: bool, pause: bool) {
    let mut source = fixture().await;
    source.pause = pause;
    let task = crate::TaskId::from_bytes([39; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let mut client = if window {
        TaskFileClient::observe_task(&source, &owner, task, 16, 4096).await
    } else {
        TaskFileClient::observe(&source, &owner, 16, 4096).await
    }
    .expect("observe");
    let cut = selection(&client).cursor().clone();
    append(&source, task).await;
    if pause {
        assert!(client.next_change().now_or_never().is_none());
        assert_eq!(selection(&client).cursor(), &cut);
    }
    let start = std::time::Instant::now();
    let result = if RESUME {
        client.next_change().await
    } else {
        client
            .changes
            .next()
            .await
            .ok_or_else(|| Error::Conflict("ended".into()))
            .and_then(|change| client.apply_change(change?))
    };
    assert!(result.is_ok(), "a closed follow must resume: {result:?}");
    assert_eq!(joined_text(&client, &owner.id, false), "resumed");
    let attempts = source.attempts.lock().expect("test journal");
    assert_eq!(attempts.len(), if pause { 3 } else { 2 });
    for (query, cursor, files, bytes) in &attempts[1..] {
        assert_eq!(query, selection(&client).query());
        assert_eq!(cursor.as_ref(), Some(&cut));
        assert_eq!((*files, *bytes), (16, 4096));
    }
    eprintln!(
        "CLIENT_FOLLOW_AFTER window={window} cancelled={pause} resumed=true elapsed_us={}",
        start.elapsed().as_micros()
    );
}
#[tokio::test]
async fn resumes_exact_cursor_or_atomic_window_reset_after_cancellation() {
    for window in [false, true] {
        for pause in [false, true] {
            check::<true>(window, pause).await;
        }
    }
}
#[tokio::test]
#[should_panic(expected = "a closed follow must resume")]
async fn negative_control_exiting_on_end_loses_the_next_publication() {
    check::<false>(false, false).await;
}

#[tokio::test]
async fn rejects_invalid_reset_without_replacing_the_accepted_view() {
    let mut source = fixture().await;
    source.invalid = true;
    let task = crate::TaskId::from_bytes([39; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let mut client = TaskFileClient::observe_task(&source, &owner, task, 16, 4096)
        .await
        .expect("observe");
    let before = selection(&client).clone();
    append(&source, task).await;
    assert!(client.next_change().await.is_err());
    assert_eq!(selection(&client), &before);
}
#[tokio::test]
async fn repeatedly_closed_follow_without_progress_returns_without_spinning() {
    let mut source = fixture().await;
    source.closed = true;
    let task = crate::TaskId::from_bytes([39; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    for window in [false, true] {
        source.count.store(0, Ordering::SeqCst);
        let mut client = if window {
            TaskFileClient::observe_task(&source, &owner, task, 16, 4096).await
        } else {
            TaskFileClient::observe(&source, &owner, 16, 4096).await
        }
        .expect("observe");
        assert!(client.next_change().await.is_err());
        assert_eq!(source.count.load(Ordering::SeqCst), 2);
    }
}
