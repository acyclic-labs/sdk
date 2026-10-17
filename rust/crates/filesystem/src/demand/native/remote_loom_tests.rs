use super::{RemoteDelivery, RemoteRead, RemoteReaders};
use loom::sync::mpsc;
use std::sync::Arc;

/// A read that waits for `gate` when it has one, then answers `value`.
struct ModelRead {
    value: u32,
    gate: Option<mpsc::Receiver<()>>,
    answer: mpsc::Sender<u32>,
}

impl RemoteRead for ModelRead {
    type Delivery = (mpsc::Sender<u32>, u32);
    fn read(self) -> Self::Delivery {
        if let Some(gate) = &self.gate {
            let _ = gate.recv();
        }
        (self.answer, self.value)
    }
}

impl RemoteDelivery for (mpsc::Sender<u32>, u32) {
    fn deliver(self) {
        let _ = self.0.send(self.1);
    }
}

fn read(
    readers: &Arc<RemoteReaders<ModelRead>>,
    value: u32,
    gate: Option<mpsc::Receiver<()>>,
) -> Option<mpsc::Receiver<u32>> {
    let (answer, answered) = mpsc::channel();
    readers
        .submit(ModelRead {
            value,
            gate,
            answer,
        })
        .ok()
        .map(|_| answered)
}

/// A blocked read never delays another accepted read, and a caller holding
/// its answer can always start the next read on the freed reader.
fn reuse_model(readers: fn(usize) -> Arc<RemoteReaders<ModelRead>>) {
    loom::model(move || {
        let readers = readers(2);
        let (release, gate) = mpsc::channel();
        let blocked = read(&readers, 1, Some(gate)).expect("first reader starts");
        let quick = read(&readers, 2, None).expect("second reader starts");
        assert_eq!(quick.recv().ok(), Some(2));
        let reused = read(&readers, 3, None).expect("freed reader was not reusable");
        assert_eq!(reused.recv().ok(), Some(3));
        assert!(release.send(()).is_ok());
        assert_eq!(blocked.recv().ok(), Some(1));
        readers.close();
    });
}

#[test]
fn freed_readers_take_the_next_read_without_waiting_behind_a_blocked_one() {
    reuse_model(RemoteReaders::new);
}

#[test]
#[should_panic(expected = "freed reader was not reusable")]
fn releasing_after_delivery_refuses_the_callers_next_read() {
    reuse_model(RemoteReaders::releasing_late);
}

/// Reads beyond the limit are refused, not queued behind busy readers.
#[test]
fn reads_beyond_the_limit_are_refused() {
    loom::model(|| {
        let readers = RemoteReaders::new(1);
        let (release, gate) = mpsc::channel();
        let blocked = read(&readers, 1, Some(gate)).expect("first reader starts");
        assert!(read(&readers, 2, None).is_none());
        assert!(release.send(()).is_ok());
        assert_eq!(blocked.recv().ok(), Some(1));
        let next = read(&readers, 3, None).expect("the reader is free again");
        assert_eq!(next.recv().ok(), Some(3));
        readers.close();
    });
}

fn shutdown_model(close: bool) {
    loom::model(move || {
        let readers = RemoteReaders::new(1);
        let (release, gate) = mpsc::channel();
        let blocked = read(&readers, 1, Some(gate)).expect("reader starts");
        if close {
            readers.close();
        }
        assert!(release.send(()).is_ok());
        assert_eq!(blocked.recv().ok(), Some(1));
        // The answer frees a reader, but a closed pool must refuse new work.
        let refused = read(&readers, 2, None).is_none();
        readers.close();
        readers.wait_closed();
        assert!(refused, "closed pool accepted another read");
        assert_eq!(readers.started(), 0);
    });
}

#[test]
fn shutdown_drains_accepted_reads_and_retires_readers() {
    shutdown_model(true);
}

#[test]
#[should_panic(expected = "closed pool accepted another read")]
fn missing_shutdown_is_detected() {
    shutdown_model(false);
}
