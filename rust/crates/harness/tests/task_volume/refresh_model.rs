//! Two readers and one publication on the production refresh/index paths.
use super::*;
use crate::task_volume::{
    Header, Query, SCHEMA,
    model_checking::{Fault, set_fault},
};
use std::{
    cell::Cell,
    future::{Future, poll_fn},
    task::{Context, Poll, Waker},
};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

loom::thread_local! {
    static ENTRY: Cell<Option<Option<u64>>> = Cell::new(None);
    static PARK_COMMIT: Cell<bool> = Cell::new(false);
}

pub(super) fn capture_tail(accepted: u64) {
    ENTRY.with(|entry| {
        if entry.get() == Some(None) {
            entry.set(Some(Some(accepted)));
        }
    });
}

pub(super) async fn before_commit() {
    if PARK_COMMIT.with(|park| park.replace(false)) {
        let mut first = true;
        poll_fn(move |_| {
            if std::mem::replace(&mut first, false) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }
}

fn scenario(faulty_reader: Option<bool>) {
    let path = StreamPath::new("refresh/files").unwrap();
    let header = serde_json::to_vec(&Header {
        volume: uuid::Uuid::nil(),
        epoch: uuid::Uuid::nil(),
        schema: SCHEMA,
    })
    .unwrap();
    let provider = Arc::new(Provider(
        Mutex::new(Histories {
            original: vec![Record {
                sequence: 0,
                commit_id: CommitId::from_bytes(*blake3::hash(&header).as_bytes()),
                value: header.into(),
                committed_at_micros: 0,
            }],
            replacement: Vec::new(),
            replaced: false,
        }),
        None,
        None,
    ));
    let readers =
        Arc::new(block_on(TaskVolume::open_existing(provider.clone(), path.clone())).unwrap());
    let writer = block_on(TaskVolume::open_existing(provider, path)).unwrap();
    let first = block_on(writer.append(
        block_on(writer.head()).unwrap(),
        &FileValue::new(FileKey::Goal, b"a".to_vec()),
    ))
    .unwrap();
    let mut publish = Box::pin(async move {
        writer
            .append(first, &FileValue::new(FileKey::Goal, b"b".to_vec()))
            .await
            .unwrap()
    });
    // Start the real append through validation/refresh, stopping immediately
    // before provider acceptance. Only its shared publication can affect readers.
    PARK_COMMIT.with(|park| park.set(true));
    assert!(
        publish
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let writer = thread::spawn(move || block_on(publish));
    let read = move |volume: Arc<TaskVolume<Provider>>, snapshot: bool| {
        ENTRY.with(|entry| entry.set(Some(None)));
        set_fault(if faulty_reader == Some(snapshot) {
            Fault::ReturnOnPeerAdvance
        } else {
            Fault::None
        });
        let (generation, bytes) = if snapshot {
            let observed = block_on(volume.snapshot(&Query::Root, 1)).unwrap();
            assert_eq!(observed.cursor.position, observed.generation.position);
            (observed.generation, observed.files[0].bytes.clone())
        } else {
            let generation = block_on(volume.head()).unwrap();
            (generation, OwnedBytes::default())
        };
        let captured = ENTRY.with(|entry| entry.get().unwrap().unwrap());
        assert!(
            generation.position >= captured,
            "refresh returned before its entry accepted tail"
        );
        assert!((2..=3).contains(&generation.position));
        if snapshot {
            let expected = if generation.position == 2 { b"a" } else { b"b" };
            assert_eq!(
                &*bytes, expected,
                "snapshot contents do not match its accepted cut"
            );
        }
    };
    let first = thread::spawn({
        let volume = readers.clone();
        move || read(volume, false)
    });
    read(readers, true);
    first
        .join()
        .unwrap_or_else(|error| std::panic::resume_unwind(error));
    assert_eq!(writer.join().unwrap().position, 3);
}

fn check(faulty_reader: Option<bool>) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = None;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.check(move || scenario(faulty_reader));
}

#[test]
fn readers_reach_the_entry_accepted_tail_during_publication() {
    check(None);
}

#[test]
fn negative_control_detects_the_advanced_early_return() {
    for snapshot in [false, true] {
        let failure = std::panic::catch_unwind(|| check(Some(snapshot)))
            .expect_err("the advanced early return escaped the entry-tail oracle");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("refresh returned before its entry accepted tail"),
            "unexpected model failure: {message}"
        );
    }
}
