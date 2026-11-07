use std::convert::Infallible;
use std::io;
use std::time::Instant;

use acyclic_collections::{FixedIndex, IndexWork};

pub(super) type Key = [u8; 16 + std::mem::size_of::<usize>()];

// The native clock remains opaque. Only its checked Duration differences are
// encoded: Duration's u64 seconds occupy fewer than 95 nanosecond bits, so
// either side of the 128-bit midpoint fits without rounding or a time horizon.
pub(super) struct Deadlines {
    origin: Instant,
    index: FixedIndex<(Instant, usize), (), { std::mem::size_of::<Key>() }>,
    counts: Counts,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
            index: FixedIndex::default(),
            counts: Counts::default(),
        }
    }
}

impl Deadlines {
    #[cfg(test)]
    pub(super) fn examined(&self) -> u64 {
        self.counts.examined.get()
    }

    #[cfg(test)]
    pub(super) fn allocations(&self) -> u64 {
        self.counts.allocations.get()
    }

    fn key(&self, at: Instant, token: usize) -> io::Result<Key> {
        let midpoint = 1_u128 << 127;
        let time = match at.checked_duration_since(self.origin) {
            Some(after) => midpoint + after.as_nanos(),
            None => {
                let before = self.origin.checked_duration_since(at).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "native deadline difference is not representable by Duration",
                    )
                })?;
                midpoint - before.as_nanos()
            }
        };
        let mut key = [0; std::mem::size_of::<Key>()];
        key[..16].copy_from_slice(&time.to_be_bytes());
        key[16..].copy_from_slice(&token.to_be_bytes());
        Ok(key)
    }

    pub(super) fn insert(&mut self, at: Instant, token: usize) -> io::Result<Key> {
        let key = self.key(at, token)?;
        completed(
            self.index
                .get_or_try_insert_with(key, &mut Work(&self.counts), |_| Ok((at, token))),
        );
        Ok(key)
    }

    pub(super) fn first(&self) -> Option<(Instant, usize)> {
        completed(self.index.first(&mut Work(&self.counts))).map(|(_, value)| *value)
    }

    pub(super) fn remove(&mut self, key: Key) {
        // Original keys, not a fresh clock conversion or admission, retire the
        // original entries. Infallible work cannot strand cancelled timers.
        completed(self.index.remove(key, &mut Work(&self.counts)));
    }

    pub(super) fn clear(&mut self) {
        self.index = FixedIndex::default();
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use std::time::Duration;

    proptest! {
        #[test]
        fn deadline_histories_preserve_native_order_and_bounded_retirement(
            history in prop::collection::vec((any::<i64>(), any::<usize>()), 1..64),
            token_prefix in any::<usize>(),
        ) {
            let hub = Hub {
                poller: platform::Poller::new()?,
                registry: Mutex::new(Registry {
                    // Exercise every native token byte without exhausting it.
                    next: token_prefix & !255,
                    ..Registry::default()
                }),
                stride: 1,
                service_failure: Arc::new(OnceLock::new()),
                locks: std::sync::atomic::AtomicUsize::new(0),
            };
            let origin = Instant::now();
            let mut expected = BTreeSet::new();
            let mut live = Vec::new();
            let epoch = hub.registry().timers.origin;
            // Before-origin order must survive the signed clock encoding too.
            for delta in [2, 1] {
                if let Some(at) = epoch.checked_sub(Duration::from_nanos(delta)) {
                    let wait = arm_on(&hub, &[], Some(at))?;
                    let token = wait.first.as_ref().unwrap().token;
                    live.push((at, token, wait));
                    expected.insert((at, token));
                }
            }
            prop_assert_eq!(hub.registry().timers.first(), expected.first().copied(),
                "deadline order or original cancellation diverged");
            if !live.is_empty() {
                let (at, token, wait) = live.remove(0);
                drop(wait);
                expected.remove(&(at, token));
                prop_assert_eq!(hub.registry().timers.first(), expected.first().copied(),
                    "cancelled deadline retained native wait ownership");
            }
            for (offset, withdraw) in history {
                let distance = Duration::from_nanos(offset.unsigned_abs());
                let at = if offset < 0 {
                    origin.checked_sub(distance)
                } else {
                    origin.checked_add(distance)
                };
                let Some(at) = at else { continue };
                let before = hub.registry().timers.examined();
                let wait = arm_on(&hub, &[], Some(at))?;
                let token = wait.first.as_ref().unwrap().token;
                let work = hub.registry().timers.examined() - before;
                prop_assert!(work <= 25 * std::mem::size_of::<deadlines::Key>() as u64 + 3,
                    "deadline admission scanned the live population: {}", work);
                expected.insert((at, token));
                live.push((at, token, wait));
                if withdraw % 2 == 0 {
                    let position = withdraw % live.len();
                    let (at, token, wait) = live.swap_remove(position);
                    let before = hub.registry().timers.examined();
                    drop(wait);
                    let work = hub.registry().timers.examined() - before;
                    prop_assert!(work <= 17 * std::mem::size_of::<deadlines::Key>() as u64 + 1,
                        "deadline retirement scanned the live population: {}", work);
                    expected.remove(&(at, token));
                }
                prop_assert_eq!(hub.registry().timers.first(), expected.first().copied(),
                    "deadline order or original cancellation diverged");
            }
            let mut ready = Vec::new();
            for (at, _) in expected {
                hub.dispatch(&mut ready, at);
                for (deadline, _, wait) in &mut live {
                    let outcome = Pin::new(wait).poll(&mut Context::from_waker(Waker::noop()));
                    if *deadline <= at {
                        prop_assert!(matches!(outcome, Poll::Ready(ProcessWaitOutcome::Deadline)),
                            "original deadline did not progress at its dispatch cut");
                    } else {
                        prop_assert!(outcome.is_pending(), "a future deadline settled early");
                    }
                }
            }
            drop(live);
            prop_assert!(hub.registry().timers.first().is_none(),
                "cancelled deadline retained native wait ownership");
        }
    }
}

fn completed<T>(result: Result<T, Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}

#[derive(Default)]
struct Counts {
    #[cfg(test)]
    examined: std::cell::Cell<u64>,
    #[cfg(test)]
    allocations: std::cell::Cell<u64>,
}

// Like native queue and registration backing, these nodes belong to the host
// readiness service. There is no new guest admission during original cleanup.
struct Work<'a>(&'a Counts);

impl IndexWork for Work<'_> {
    type Error = Infallible;
    type Allocation = ();

    fn check(&mut self) -> Result<(), Infallible> {
        let _ = self.0;
        Ok(())
    }

    fn charge_items(&mut self, items: u64) -> Result<(), Infallible> {
        #[cfg(test)]
        self.0.examined.set(self.0.examined.get() + items);
        #[cfg(not(test))]
        let _ = items;
        Ok(())
    }

    fn claim(&mut self, _: u64) -> Result<(), Infallible> {
        #[cfg(test)]
        self.0.allocations.set(self.0.allocations.get() + 1);
        Ok(())
    }
}
