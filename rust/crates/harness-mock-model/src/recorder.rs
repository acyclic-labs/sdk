//! One ordered writer keeps filesystem latency off the HTTP executor.
use std::io::{self, Write};
use std::sync::mpsc;
use tokio::sync::oneshot;

struct Pending {
    record: Vec<u8>,
    event: Vec<u8>,
    committed: oneshot::Sender<io::Result<()>>,
}

pub(super) struct Recorder(mpsc::Sender<Pending>);

impl Recorder {
    pub(super) fn open(file: std::fs::File) -> io::Result<Self> {
        let (sender, receiver) = mpsc::channel();
        std::thread::Builder::new()
            .name("mock-records".into())
            .spawn(move || run(&receiver, file, io::stdout()))?;
        Ok(Self(sender))
    }

    pub(super) fn enqueue(
        &self,
        record: Vec<u8>,
        event: Vec<u8>,
    ) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let (committed, done) = oneshot::channel();
        self.0
            .send(Pending {
                record,
                event,
                committed,
            })
            .map_err(io::Error::other)?;
        Ok(done)
    }
}

fn run(receiver: &mpsc::Receiver<Pending>, mut file: impl Write, mut events: impl Write) {
    let mut failure: Option<io::Error> = None;
    while let Ok(first) = receiver.recv() {
        // Drain work already ready, without a timer or a batching delay.
        let batch: Vec<_> = std::iter::once(first)
            .chain(receiver.try_iter().take(63))
            .collect();
        if failure.is_none() {
            failure = publish(&batch, &mut file, &mut events).err();
        }
        for pending in batch {
            let result = failure.as_ref().map_or(Ok(()), |error| {
                Err(io::Error::new(error.kind(), error.to_string()))
            });
            let _ = pending.committed.send(result);
        }
        // A partial append poisons this writer. Never acknowledge a record
        // behind an unpublished tail, even if a later write could succeed.
    }
}

fn publish(batch: &[Pending], file: &mut impl Write, events: &mut impl Write) -> io::Result<()> {
    let records: Vec<_> = batch
        .iter()
        .flat_map(|pending| pending.record.iter().copied())
        .collect();
    let notifications: Vec<_> = batch
        .iter()
        .flat_map(|pending| pending.event.iter().copied())
        .collect();
    file.write_all(&records)?;
    file.flush()?;
    events.write_all(&notifications)?;
    events.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct Output {
        bytes: Arc<Mutex<Vec<u8>>>,
        record: bool,
        fail: bool,
    }

    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.fail {
                return Err(io::Error::other("injected append failure"));
            }
            self.bytes
                .lock()
                .map_err(|_| io::Error::other("recorder trace lock poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            // The same trace records the file fence before any event bytes.
            if self.record {
                self.bytes
                    .lock()
                    .map_err(|_| io::Error::other("recorder trace lock poisoned"))?
                    .push(b'|');
            }
            Ok(())
        }
    }

    #[test]
    fn queued_requests_share_one_append_and_publish_only_after_flush() {
        for fail in [false, true] {
            let trace = Arc::new(Mutex::new(Vec::new()));
            let file = Output {
                bytes: trace.clone(),
                record: true,
                fail,
            };
            let events = Output {
                bytes: trace.clone(),
                record: false,
                fail: false,
            };
            let (sender, receiver) = mpsc::channel();
            let recorder = Recorder(sender);
            let first = recorder.enqueue(b"1\n".to_vec(), b"a\n".to_vec()).unwrap();
            let second = recorder.enqueue(b"2\n".to_vec(), b"b\n".to_vec()).unwrap();
            // Queue both before starting the writer: no scheduler dependence.
            drop(recorder);
            run(&receiver, file, events);
            assert_eq!(first.blocking_recv().unwrap().is_err(), fail);
            assert_eq!(second.blocking_recv().unwrap().is_err(), fail);
            assert_eq!(
                trace.lock().unwrap().as_slice(),
                if fail {
                    &b""[..]
                } else {
                    &b"1\n2\n|a\nb\n"[..]
                }
            );
        }
    }
}
