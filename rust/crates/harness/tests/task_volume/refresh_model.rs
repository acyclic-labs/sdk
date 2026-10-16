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
    static READ_NUMBER: Cell<usize> = Cell::new(0);
}

// Entry, provider and index locks never nest. The writer waits for both readers
// to sample their index before accepting the new tail and releasing watermarks.
#[derive(Default)]
pub(super) struct Entries(Mutex<(usize, bool)>, Condvar);

impl Entries {
    fn arrive(&self) {
        let mut state = self.0.lock().unwrap();
        state.0 += 1;
        self.1.notify_all();
        while !state.1 {
            state = self.1.wait(state).unwrap();
        }
    }
    fn both_readers(&self) {
        let mut state = self.0.lock().unwrap();
        while state.0 != 2 {
            state = self.1.wait(state).unwrap();
        }
    }
    fn release(&self) {
        self.0.lock().unwrap().1 = true;
        self.1.notify_all();
    }
}

pub(super) fn before_watermarks(pauses: &ReadPauses) {
    if ENTRY.with(|entry| entry.get() == Some(None))
        && let Some(entry) = &pauses.entry
    {
        entry.arrive();
    }
}

pub(super) fn read_number() -> Option<usize> {
    READ_NUMBER.with(|read| {
        let number = read.get();
        if number == 0 {
            return None;
        }
        read.set(number + 2);
        Some(number)
    })
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

fn scenario(snapshot: bool, short_first: bool, broken: bool) {
    let path = StreamPath::new("refresh/files").unwrap();
    let header = serde_json::to_vec(&Header {
        volume: uuid::Uuid::nil(),
        epoch: uuid::Uuid::nil(),
        schema: SCHEMA,
    })
    .unwrap();
    let entry = Arc::new(Entries::default());
    let pauses = Arc::new(ReadPauses {
        short_first,
        entry: Some(entry.clone()),
        ..Default::default()
    });
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
        Some(pauses.clone()),
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
    pauses.first.0.lock().unwrap().remaining = 1;
    pauses.second.0.lock().unwrap().remaining = 1;
    let read = move |volume: Arc<TaskVolume<Provider>>, snapshot: bool, target: bool| {
        ENTRY.with(|entry| entry.set(Some(None)));
        READ_NUMBER.with(|read| read.set(if target { 1 } else { 2 }));
        set_fault(if broken && target {
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
        (generation, bytes, captured)
    };
    let first = thread::spawn({
        let volume = readers.clone();
        move || read(volume, snapshot, true)
    });
    let writer = thread::spawn(move || {
        entry.both_readers();
        let publication = block_on(publish);
        entry.release();
        pauses.first.reached();
        pauses.second.reached();
        // A captured tail of 3 can be missed only by accepting the peer's
        // intermediate cut 2. Hold that cut until the target read returns.
        pauses.first.release();
        let observed = first.join().unwrap();
        pauses.second.release();
        (publication, observed)
    });
    let peer = read(readers, !snapshot, false);
    let (publication, observed) = writer.join().unwrap();
    assert_eq!(publication.position, 3);
    for (snapshot, (generation, bytes, captured)) in [(snapshot, observed), (!snapshot, peer)] {
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
    }
}

fn check(snapshot: bool, short_first: bool, broken: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = None;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.check(move || scenario(snapshot, short_first, broken));
}

#[test]
fn readers_reach_the_entry_accepted_tail_during_publication() {
    for snapshot in [false, true] {
        for short_first in [false, true] {
            check(snapshot, short_first, false);
        }
    }
}

#[test]
fn negative_control_detects_the_advanced_early_return() {
    for snapshot in [false, true] {
        for short_first in [false, true] {
            let failure = std::panic::catch_unwind(|| check(snapshot, short_first, true))
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
}
