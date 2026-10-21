//! Wall-clock jumps are independent of monotonic timeout advancement.
use super::*;
use std::time::Duration;

/// A provider retains its seed's clock when callbacks move to driver threads.
/// The runtime handle is essential: Tokio's paused clock belongs to the runtime,
/// not to whichever thread happens to invoke the callback.
#[derive(Clone)]
pub struct CapturedClock {
    schedule: Schedule,
    runtime: tokio::runtime::Handle,
    origin: std::time::Instant,
}

impl CapturedClock {
    /// Captures the active seed and runtime, or returns None outside simulation.
    pub fn capture() -> Option<Self> {
        let context = Context::simulation()?;
        if !context.schedule.is_seeded() {
            return None;
        }
        let runtime = context.schedule.0.state().wall.runtime.clone()?;
        let origin = {
            let _runtime = runtime.enter();
            tokio::time::Instant::now().into_std()
        };
        Some(Self {
            schedule: context.schedule,
            runtime,
            origin,
        })
    }

    /// Reads simulated wall time, including elapsed time and injected jumps.
    pub fn unix_millis(&self) -> u64 {
        let _runtime = self.runtime.enter();
        self.schedule.0.state().wall.now()
    }

    /// Reads the creating runtime's monotonic clock on any callback thread.
    pub fn instant(&self) -> std::time::Instant {
        let _runtime = self.runtime.enter();
        tokio::time::Instant::now().into_std()
    }

    /// Monotonic time since capture; wall-clock jumps do not affect it.
    pub fn elapsed(&self) -> Duration {
        self.instant().duration_since(self.origin)
    }
}

pub(super) struct Wall {
    millis: u64,
    anchor: Option<tokio::time::Instant>,
    runtime: Option<tokio::runtime::Handle>,
    #[cfg(test)]
    lose_elapsed: bool,
}

impl Default for Wall {
    fn default() -> Self {
        Self {
            millis: 1_700_000_000_000,
            anchor: None,
            runtime: None,
            #[cfg(test)]
            lose_elapsed: false,
        }
    }
}

impl Wall {
    // Anchor on entry into the runtime, before the first user poll or timer.
    // Later scopes and captured callbacks retain the same clock origin.
    pub(super) fn start(&mut self) {
        self.runtime
            .get_or_insert_with(tokio::runtime::Handle::current);
        self.anchor.get_or_insert_with(tokio::time::Instant::now);
    }

    fn now(&mut self) -> u64 {
        let _runtime = self.runtime.as_ref().map(tokio::runtime::Handle::enter);
        let now = tokio::time::Instant::now();
        #[cfg(test)]
        if self.lose_elapsed {
            self.anchor = Some(now);
        }
        let anchor = self.anchor.get_or_insert(now);
        self.millis.saturating_add(
            u64::try_from(now.duration_since(*anchor).as_millis()).unwrap_or(u64::MAX),
        )
    }
}

/// The active simulation's wall clock, or `None` during ordinary execution.
pub fn unix_millis() -> Option<u64> {
    Context::simulation()
        .filter(|context| context.schedule.is_seeded())
        .map(|context| context.schedule.0.state().wall.now())
}

impl Schedule {
    /// Only a seeded schedule has a simulated clock; an unseeded one keeps a
    /// wall that nothing reads.
    pub(super) fn is_seeded(&self) -> bool {
        self.0.executor.is_some()
    }

    /// Changes wall time without changing elapsed time or expiring a timer.
    pub fn set_unix_millis(&self, millis: u64) {
        let mut state = self.0.state();
        let wall = &mut state.wall;
        wall.millis = millis;
        wall.anchor = Some(tokio::time::Instant::now());
    }

    /// Advances both simulated elapsed time and wall time. The driver must use
    /// Tokio's paused current-thread clock; production timers remain unchanged.
    pub async fn advance(&self, duration: Duration) {
        {
            let mut state = self.0.state();
            // Establish the wall's anchor before any monotonic advancement,
            // including Tokio's own automatic advancement while the driver idles.
            state.wall.now();
        }
        tokio::time::advance(duration).await;
    }
}

#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn automatic_timeout_advancement_cannot_leave_wall_time_behind(
            delay in 1u64..1_000_000,
            capture in proptest::prelude::any::<bool>(),
        ) {
            automatic_timeout(false, false, capture, delay);
        }

        #[test]
        fn wall_jumps_do_not_expire_monotonic_waits(before in 0u64..u64::MAX / 2, after in 0u64..u64::MAX / 2) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
            let (schedule, _) = Schedule::seeded(71);
            runtime.block_on(async {
                tokio::time::pause();
                schedule.set_unix_millis(before);
                schedule.scope(async {
                    assert_eq!(unix_millis(), Some(before));
                    let start = tokio::time::Instant::now();
                    let timer = tokio::time::sleep(Duration::from_millis(10));
                    let mut timer = std::pin::pin!(timer);
                    std::future::poll_fn(|cx| {
                        assert!(timer.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    }).await;
                    schedule.set_unix_millis(after);
                    assert_eq!(unix_millis(), Some(after));
                    assert_eq!(tokio::time::Instant::now(), start);
                    std::future::poll_fn(|cx| {
                        assert!(timer.as_mut().poll(cx).is_pending(), "wall jump expired a timeout");
                        Poll::Ready(())
                    }).await;
                    schedule.advance(Duration::from_millis(100)).await;
                    timer.await;
                    assert_eq!(tokio::time::Instant::now() - start, Duration::from_millis(100));
                    assert_eq!(unix_millis(), Some(after + 100));
                }).await;
                assert_eq!(unix_millis(), None, "clock escaped its scope");
            });
        }
    }

    fn automatic_timeout(lose_elapsed: bool, lazy_anchor: bool, capture: bool, delay: u64) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (schedule, _) = Schedule::seeded(71);
        schedule.0.state().wall.lose_elapsed = lose_elapsed;
        runtime.block_on(async {
            tokio::time::pause();
            schedule
                .scope(async {
                    if lazy_anchor {
                        schedule.0.state().wall.anchor = None;
                    }
                    let captured = capture.then(|| CapturedClock::capture().unwrap());
                    let start = tokio::time::Instant::now();
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    let elapsed = u64::try_from(start.elapsed().as_millis()).unwrap();
                    assert!(elapsed >= delay, "timer fired before its deadline");
                    let now = captured
                        .as_ref()
                        .map_or_else(unix_millis, |clock| Some(clock.unix_millis()));
                    assert_eq!(
                        now,
                        Some(1_700_000_000_000 + elapsed),
                        "wall clock diverged"
                    );
                })
                .await;
        });
    }

    #[test]
    #[should_panic(expected = "wall clock diverged")]
    fn discarding_elapsed_time_negative_control() {
        automatic_timeout(true, false, false, 10);
    }

    #[test]
    #[should_panic(expected = "wall clock diverged")]
    fn first_read_anchor_negative_control() {
        automatic_timeout(false, true, false, 10);
    }

    #[test]
    #[should_panic(expected = "wall clock diverged")]
    fn captured_first_read_anchor_negative_control() {
        automatic_timeout(false, true, true, 10);
    }
}
