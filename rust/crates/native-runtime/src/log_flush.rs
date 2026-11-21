use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

/// Coordinates durable cuts for an append-only file with monotonically
/// increasing append positions, including across file replacement.
#[derive(Default)]
pub struct LogFlush {
    covered: AtomicU64,
    producer: Mutex<()>,
}

impl LogFlush {
    /// Covers the current append cut. The snapshot runs after any previous
    /// flush finishes, so writes made in the meantime share this operation.
    /// The snapshot must pin the associated file until the flush ends.
    pub fn flush<F>(
        &self,
        snapshot: impl FnOnce() -> io::Result<(u64, F)>,
        flush: impl FnOnce(F) -> io::Result<()>,
    ) -> io::Result<()> {
        let _producer = self.producer.lock().unwrap_or_else(PoisonError::into_inner);
        let (cut, file) = snapshot()?;
        if self.covered.load(Ordering::Acquire) < cut {
            flush(file)?;
            self.publish(cut);
        }
        Ok(())
    }

    /// Publishes a cut made durable by replacement of the log.
    pub fn publish(&self, cut: u64) {
        self.covered.fetch_max(cut, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn appends_during_a_flush_share_the_next_cut() {
        let flush = LogFlush::default();
        let file = tempfile::tempfile().unwrap();
        crate::write_all_at(&file, 0, b"1", None).unwrap();
        let flushes = AtomicU64::new(0);
        let snapshot = || Ok((file.metadata()?.len(), &file));
        let (entered, in_flight) = sync_channel(0);
        let (release, released) = sync_channel(0);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                flush.flush(snapshot, |file| {
                    crate::flush_file_data(file)?;
                    flushes.fetch_add(1, Ordering::Relaxed);
                    entered.send(()).unwrap();
                    let released = released;
                    released.recv().unwrap();
                    Ok(())
                })
            });
            in_flight.recv().unwrap();
            for offset in 1..5 {
                crate::write_all_at(&file, offset, b"1", None).unwrap();
            }
            let rest = [(); 4].map(|()| {
                scope.spawn(|| {
                    flush.flush(snapshot, |file| {
                        crate::flush_file_data(file)?;
                        flushes.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    })
                })
            });
            release.send(()).unwrap();
            first.join().unwrap().unwrap();
            for waiter in rest {
                waiter.join().unwrap().unwrap();
            }
        });
        assert_eq!(flushes.load(Ordering::Relaxed), 2);
    }
}
