use crate::Durability;
use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// The ordered and device-flushed prefixes of an append-only log.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogFlushProgress {
    /// Latest ordered append position.
    pub ordered: u64,
    /// Latest position covered by a full device flush, if one finished.
    pub flushed: Option<u64>,
}

impl LogFlushProgress {
    /// Whether the requested flush strength covers this append position.
    #[must_use]
    pub fn covers(&self, cut: u64, strength: Durability) -> bool {
        match strength {
            Durability::Full => self.flushed.is_some_and(|flushed| flushed >= cut),
            Durability::Barrier => self.ordered >= cut,
        }
    }

    /// Records the cut actually covered by a completed operation.
    pub fn publish(&mut self, cut: u64, strength: Durability) {
        self.ordered = self.ordered.max(cut);
        if strength == Durability::Full {
            self.flushed = Some(self.flushed.unwrap_or(0).max(cut));
        }
    }
}

/// One producer and its completed cuts. Store this under the lock that
/// serializes the log's publication, or under a dedicated progress lock.
#[derive(Debug, Default)]
pub struct LogFlushGroup {
    /// Cuts already covered by completed flushes.
    pub progress: LogFlushProgress,
    active: bool,
}

impl LogFlushGroup {
    /// Admits one producer if no current flush or completed cut covers it.
    pub fn claim(&mut self, cut: u64, strength: Durability) -> bool {
        if self.active || self.progress.covers(cut, strength) {
            return false;
        }
        self.active = true;
        true
    }

    /// Releases the producer before publishing a cut or reporting a failure.
    pub fn release(&mut self) {
        self.active = false;
    }
}

/// Coordinates durable cuts for an append-only file with monotonically
/// increasing append positions, including across file replacement.
#[derive(Default)]
pub struct LogFlush {
    group: Arc<FlushState>,
}

#[derive(Default)]
struct FlushState {
    group: Mutex<LogFlushGroup>,
    producer: Arc<AsyncMutex<()>>,
}

/// The next physical flush producer, admitted without occupying a worker.
pub struct LogFlushPermit {
    group: Arc<FlushState>,
    _producer: OwnedMutexGuard<()>,
}

impl LogFlush {
    /// Parks outside the persistence pool until this caller owns the next cut.
    pub async fn admit(&self) -> LogFlushPermit {
        let group = Arc::clone(&self.group);
        let producer = Arc::clone(&group.producer).lock_owned().await;
        LogFlushPermit {
            group,
            _producer: producer,
        }
    }

    /// Flushes during exclusive close, when no producer can still be admitted.
    pub fn flush_unique<F>(
        &mut self,
        snapshot: impl FnOnce() -> io::Result<(u64, F)>,
        flush: impl FnOnce(F) -> io::Result<()>,
    ) -> io::Result<()> {
        Arc::get_mut(&mut self.group)
            .ok_or(io::ErrorKind::WouldBlock)?
            .flush(snapshot, flush)
    }

    /// Publishes a cut made durable by replacement of the log.
    pub fn publish(&self, cut: u64) {
        self.group.group().progress.publish(cut, Durability::Full);
    }
}

impl LogFlushPermit {
    /// Covers the accepted cut at the worker's snapshot. The permit remains
    /// held if its observer disappears while the worker is in flight.
    pub fn flush<F>(
        self,
        snapshot: impl FnOnce() -> io::Result<(u64, F)>,
        flush: impl FnOnce(F) -> io::Result<()>,
    ) -> io::Result<()> {
        self.group.flush(snapshot, flush)
    }
}

impl FlushState {
    fn flush<F>(
        &self,
        snapshot: impl FnOnce() -> io::Result<(u64, F)>,
        flush: impl FnOnce(F) -> io::Result<()>,
    ) -> io::Result<()> {
        let (cut, file) = snapshot()?;
        if cut == 0 || !self.group().claim(cut, Durability::Full) {
            return Ok(());
        }
        struct Producer<'a>(&'a FlushState);
        impl Drop for Producer<'_> {
            fn drop(&mut self) {
                self.0.group().release();
            }
        }
        let _active = Producer(self);
        flush(file)?;
        self.group().progress.publish(cut, Durability::Full);
        Ok(())
    }

    fn group(&self) -> std::sync::MutexGuard<'_, LogFlushGroup> {
        self.group.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn appends_during_a_flush_share_the_next_cut() {
        let log = LogFlush::default();
        let file = Arc::new(tempfile::tempfile().unwrap());
        crate::write_all_at(&file, 0, b"1", None).unwrap();
        let flushes = Arc::new(AtomicU64::new(0));
        let (entered, in_flight) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let permit = log.admit().await;
        let first = tokio::task::spawn_blocking({
            let file = Arc::clone(&file);
            let flushes = Arc::clone(&flushes);
            move || {
                permit.flush(
                    || Ok((file.metadata()?.len(), &file)),
                    |file| {
                        crate::flush_file_data(file)?;
                        flushes.fetch_add(1, Ordering::Relaxed);
                        entered.send(()).unwrap();
                        released.blocking_recv().unwrap();
                        Ok(())
                    },
                )
            }
        });
        in_flight.await.unwrap();
        for offset in 1..5 {
            crate::write_all_at(&file, offset, b"1", None).unwrap();
        }
        let late = futures_util::future::join_all((1..5).map(|_| async {
            let permit = log.admit().await;
            let file = Arc::clone(&file);
            let flushes = Arc::clone(&flushes);
            tokio::task::spawn_blocking(move || {
                permit.flush(
                    || Ok((file.metadata()?.len(), &file)),
                    |file| {
                        crate::flush_file_data(file)?;
                        flushes.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    },
                )
            })
            .await
            .unwrap()
            .unwrap();
        }));
        let mut late = std::pin::pin!(late);
        let parked = std::future::poll_fn(|context| {
            std::task::Poll::Ready(late.as_mut().poll(context).is_pending())
        })
        .await;
        assert!(parked);
        release.send(()).unwrap();
        first.await.unwrap().unwrap();
        late.await;
        assert_eq!(flushes.load(Ordering::Relaxed), 2);
    }
}
