//! Fixed native producers, selected by the source they own.

use super::*;
use std::num::NonZeroUsize;
use std::sync::Condvar;
use std::sync::atomic::{AtomicUsize, Ordering};

static POOL: OnceLock<io::Result<Arc<Pool>>> = OnceLock::new();

pub(super) struct Pool {
    hubs: Box<[Hub]>,
    failure: Arc<OnceLock<Arc<io::Error>>>,
    next_deadline: AtomicUsize,
    // Startup is a single event. A refused spawn releases every thread already
    // created before any of them can enter an unbounded native wait.
    startup: Mutex<Option<bool>>,
    startup_changed: Condvar,
}

impl Pool {
    fn open(count: NonZeroUsize) -> io::Result<Self> {
        let failure = Arc::new(OnceLock::new());
        let hubs = (0..count.get())
            .map(|index| {
                Ok(Hub {
                    poller: platform::Poller::new()?,
                    registry: Mutex::new(Registry {
                        next: index,
                        ..Registry::default()
                    }),
                    stride: count.get(),
                    service_failure: Arc::clone(&failure),
                    #[cfg(test)]
                    locks: AtomicUsize::new(0),
                })
            })
            .collect::<io::Result<Box<[_]>>>()?;
        Ok(Self {
            hubs,
            failure,
            next_deadline: AtomicUsize::new(0),
            startup: Mutex::new(None),
            startup_changed: Condvar::new(),
        })
    }

    fn release_startup(&self, running: bool) {
        let mut startup = self.startup.lock().unwrap_or_else(PoisonError::into_inner);
        if startup.is_none() {
            *startup = Some(running);
            self.startup_changed.notify_all();
        }
    }

    fn run(&self, index: usize) {
        let mut startup = self.startup.lock().unwrap_or_else(PoisonError::into_inner);
        let running = loop {
            if let Some(running) = *startup {
                break running;
            }
            startup = self
                .startup_changed
                .wait(startup)
                .unwrap_or_else(PoisonError::into_inner);
        };
        drop(startup);
        if running {
            self.hubs[index].run();
        }
    }

    fn owner(&self, key: usize) -> &Hub {
        &self.hubs[key % self.hubs.len()]
    }

    #[cfg(unix)]
    pub(super) fn source(&self, source: SourceKey) -> &Hub {
        let key = match source {
            SourceKey::Descriptor(fd) => fd.unsigned_abs() as usize,
            #[cfg(target_vendor = "apple")]
            SourceKey::Process(pid) => pid.unsigned_abs() as usize,
        };
        self.owner(key)
    }

    #[cfg(windows)]
    pub(super) fn handle(&self, handle: std::os::windows::io::RawHandle) -> &Hub {
        // Native handles are aligned; the low alignment bits carry no identity.
        self.owner(handle.addr() >> 2)
    }

    pub(super) fn deadline(&self) -> &Hub {
        self.owner(self.next_deadline.fetch_add(1, Ordering::Relaxed))
    }

    pub(super) fn arm(
        &self,
        sources: &[Source<'_>],
        wake_at: Option<Instant>,
    ) -> io::Result<Wait<'_>> {
        arm_with(sources, wake_at, |source| match source {
            #[cfg(unix)]
            Some(source) => self.source(source.key()),
            #[cfg(windows)]
            Some(Source::Exit(handle)) => {
                use std::os::windows::io::AsRawHandle as _;
                self.handle(handle.as_raw_handle())
            }
            #[cfg(windows)]
            Some(Source::Pipe(operation)) => operation.owner(),
            None => self.deadline(),
        })
    }

    #[cfg(any(windows, test))]
    pub(super) fn completion(&self, token: usize) -> &Hub {
        // Each producer advances by the immutable pool width, so callbacks
        // recover their original port without a borrowed context or a scan.
        self.owner(token)
    }

    pub(super) fn check(&self) -> io::Result<()> {
        if let Some(error) = self.failure.get() {
            return Err(io::Error::new(error.kind(), Arc::clone(error)));
        }
        Ok(())
    }
}

struct Startup<'a>(&'a Pool);

impl Drop for Startup<'_> {
    fn drop(&mut self) {
        self.0.release_startup(false);
    }
}

fn start() -> io::Result<Arc<Pool>> {
    #[cfg(test)]
    STARTED.store(true, Ordering::Release);
    let pool = Arc::new(Pool::open(std::thread::available_parallelism()?)?);
    let startup = Startup(&pool);
    for index in 0..pool.hubs.len() {
        let original = Arc::clone(&pool);
        std::thread::Builder::new()
            .name(format!("acyclic-process-events-{index}"))
            .spawn(move || original.run(index))?;
    }
    pool.release_startup(true);
    drop(startup);
    Ok(pool)
}

pub(super) fn pool() -> io::Result<&'static Pool> {
    POOL.get_or_init(start)
        .as_ref()
        .map(Arc::as_ref)
        .map_err(copy_error)
}

#[cfg(unix)]
pub(super) fn existing() -> Option<&'static Pool> {
    POOL.get()?.as_ref().ok().map(Arc::as_ref)
}

#[cfg(windows)]
pub(super) fn completion(token: usize) -> Option<&'static Hub> {
    Some(POOL.get()?.as_ref().ok()?.completion(token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn startup_abort_releases_original_native_producers() -> io::Result<()> {
        let pool = Pool::open(NonZeroUsize::new(2).unwrap())?;
        std::thread::scope(|scope| {
            // The omission control observes the missing terminal at its
            // publication point. Its rescue releases the actual startup wait
            // before scoped joins, even when that assertion unwinds.
            struct Release<'a>(&'a Pool);
            impl Drop for Release<'_> {
                fn drop(&mut self) {
                    self.0.release_startup(false);
                }
            }
            let _release = Release(&pool);
            let startup = Startup(&pool);
            let (finished, completion) = mpsc::channel();
            for index in 0..pool.hubs.len() {
                let pool = &pool;
                let finished = finished.clone();
                std::thread::Builder::new().spawn_scoped(scope, move || {
                    pool.run(index);
                    let _ = finished.send(());
                })?;
            }
            drop(finished);
            drop(startup);
            assert_eq!(
                *pool.startup.lock().unwrap_or_else(PoisonError::into_inner),
                Some(false),
                "startup refusal retained a producer without a start event"
            );
            for _ in pool.hubs.iter() {
                completion.recv().map_err(io::Error::other)?;
            }
            Ok(())
        })
    }

    #[cfg(unix)]
    #[test]
    fn original_producers_retire_independently_of_other_sources_and_history() -> io::Result<()> {
        use std::io::{Read as _, Write as _};
        use std::os::fd::AsFd as _;

        if crate::delivery::isolated()? {
            return Ok(());
        }
        let pool = Pool::open(NonZeroUsize::new(2).unwrap())?;
        let (reader, writer) = std::os::unix::net::UnixStream::pair()?;
        let readable = Source::Readable(reader.as_fd());
        let writable = Source::Writable(writer.as_fd());
        let reading = pool.source(readable.key());
        let writing = pool.source(writable.key());
        assert!(
            !std::ptr::eq(reading, writing),
            "distinct native sources share one registry"
        );

        struct Notified(AtomicUsize);
        impl Wake for Notified {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let notified = Arc::new(Notified(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&notified));
        let mut context = Context::from_waker(&waker);
        let deadline = Instant::now() + Duration::from_secs(3600);
        let mut mixed = pool.arm(&[readable, writable], Some(deadline))?;
        for entry in mixed.first.iter().chain(&mixed.other) {
            assert!(
                std::ptr::eq(pool.completion(entry.token), entry.hub),
                "native packet lost its original producer"
            );
            assert!(Arc::ptr_eq(
                &entry.hub.registry().armed[0].1.signal,
                &mixed.signal
            ));
        }
        assert!(Pin::new(&mut mixed).poll(&mut context).is_pending());
        let mut ready = Vec::new();
        writing.poller.wait(None, &mut ready)?;
        writing.dispatch(&mut ready, Instant::now());
        assert!(matches!(
            Pin::new(&mut mixed).poll(&mut context),
            Poll::Ready(ProcessWaitOutcome::Ready)
        ));
        (&writer).write_all(b"x")?;
        let token = mixed.first.as_ref().unwrap().token;
        while !ready.contains(&token) {
            // Retained data drives each native batch, with no readiness probe.
            reading.poller.wait(None, &mut ready)?;
        }
        reading.dispatch(&mut ready, Instant::now());
        assert_eq!(notified.0.load(Ordering::Relaxed), 1);
        let mut byte = [0];
        (&reader).read_exact(&mut byte)?;
        assert_eq!(byte, [b'x']);
        drop(mixed);

        // Count the actual registry acquisitions at one and 100,000 live
        // deadlines. Deadline index work is bounded by the actual clock/token
        // key width; source admission and withdrawal visit their original owner.
        for population in [1, 100_000] {
            let retained = (0..population)
                .map(|_| pool.arm(&[], Some(deadline)))
                .collect::<io::Result<Vec<_>>>()?;
            let live = reading.registry().armed.len();
            let before = reading.registry().timers.examined();
            let allocations = reading.registry().timers.allocations();
            let deadline_wait = super::super::arm_on(reading, &[], Some(deadline))?;
            let admitted = reading.registry().timers.examined() - before;
            assert!(
                admitted <= 25 * std::mem::size_of::<super::super::deadlines::Key>() as u64 + 3,
                "deadline admission scanned the live population: {admitted}"
            );
            assert!(reading.registry().timers.allocations() - allocations <= 2);
            let before = reading.registry().timers.examined();
            let allocations = reading.registry().timers.allocations();
            drop(deadline_wait);
            let retired = reading.registry().timers.examined() - before;
            assert!(
                retired <= 17 * std::mem::size_of::<super::super::deadlines::Key>() as u64 + 1,
                "deadline retirement scanned the live population: {retired}"
            );
            assert_eq!(reading.registry().timers.allocations(), allocations);
            let before = [
                reading.locks.load(Ordering::Relaxed),
                writing.locks.load(Ordering::Relaxed),
            ];
            let source = pool.arm(&[readable], None)?;
            assert_eq!(reading.locks.load(Ordering::Relaxed) - before[0], 1);
            assert_eq!(writing.locks.load(Ordering::Relaxed) - before[1], 0);
            drop(source);
            assert_eq!(reading.locks.load(Ordering::Relaxed) - before[0], 2);
            assert_eq!(writing.locks.load(Ordering::Relaxed) - before[1], 0);
            assert_eq!(
                reading.registry().armed.len(),
                live,
                "cancelled source retained a registry entry"
            );
            drop(retained);
        }

        let mut read = pool.arm(&[readable], None)?;
        let mut write = pool.arm(&[writable], None)?;
        assert!(Pin::new(&mut read).poll(&mut context).is_pending());
        assert!(Pin::new(&mut write).poll(&mut context).is_pending());
        drop(Retirement(reading));
        let Poll::Ready(ProcessWaitOutcome::ServiceFailed(cause)) =
            Pin::new(&mut read).poll(&mut context)
        else {
            panic!("original producer loss left its source pending")
        };
        assert!(Pin::new(&mut write).poll(&mut context).is_pending());
        let mut late = pool.arm(&[readable], None)?;
        let Poll::Ready(ProcessWaitOutcome::ServiceFailed(original)) =
            Pin::new(&mut late).poll(&mut context)
        else {
            panic!("later source lost its original producer failure")
        };
        assert!(Arc::ptr_eq(&cause, &original));
        assert!(pool.check().is_err());
        while !write.fired() {
            writing.poller.wait(None, &mut ready)?;
            writing.dispatch(&mut ready, Instant::now());
        }
        assert!(matches!(
            Pin::new(&mut write).poll(&mut context),
            Poll::Ready(ProcessWaitOutcome::Ready)
        ));
        Ok(())
    }
}
