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
    pub(super) runtime: Option<tokio::runtime::Handle>,
    #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
    alarms: Option<Arc<crate::wall_alarm::Simulated>>,
    #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
    alarms_closed: bool,
}

impl Default for Wall {
    fn default() -> Self {
        Self {
            millis: 1_700_000_000_000,
            anchor: None,
            runtime: None,
            #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
            alarms: None,
            #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
            alarms_closed: false,
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

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
pub(crate) fn wall_alarms() -> std::io::Result<Option<Arc<crate::wall_alarm::Simulated>>> {
    let Some(context) = Context::simulation() else {
        return Ok(None);
    };
    if !context.schedule.is_seeded() {
        return Ok(None);
    }
    let mut state = context.schedule.0.state();
    let wall = &mut state.wall;
    // An existing index retains its original failure. Without one, closure
    // must still refuse creation without allocating an index during shutdown.
    if wall.alarms_closed && wall.alarms.is_none() {
        return Err(crate::wall_alarm::Simulated::producer_lost());
    }
    let alarms = wall.alarms.get_or_insert_with(Default::default).clone();
    Ok(Some(alarms))
}

impl Schedule {
    /// Only a seeded schedule has a simulated clock; an unseeded one keeps a
    /// wall that nothing reads.
    pub(super) fn is_seeded(&self) -> bool {
        self.0.executor.is_some()
    }

    /// Changes wall time without changing elapsed time. Absolute wall alarms
    /// observe the jump; monotonic timeouts retain their original deadlines.
    pub fn set_unix_millis(&self, millis: u64) {
        {
            let mut state = self.0.state();
            let wall = &mut state.wall;
            wall.millis = millis;
            wall.anchor = Some(tokio::time::Instant::now());
        }
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        let alarms = self.0.state().wall.alarms.clone();
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        if let Some(alarms) = alarms {
            alarms.changed();
        }
    }

    pub(super) fn poll_wall_alarms(&self, cx: &mut std::task::Context<'_>) {
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        {
            let active = {
                let mut state = self.0.state();
                let alarms = state.wall.alarms.clone();
                alarms.map(|alarms| (alarms, state.wall.now()))
            };
            if let Some((alarms, now)) = active {
                alarms.poll(now, cx);
            }
        }
    }

    pub(super) fn close_wall_alarms(&self) {
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        let alarms = {
            let mut state = self.0.state();
            state.wall.alarms_closed = true;
            state.wall.alarms.clone()
        };
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        if let Some(alarms) = alarms {
            alarms.close();
        }
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
#[cfg(not(loom))]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn automatic_timeout_advancement_cannot_leave_wall_time_behind(
            delay in 1u64..1_000_000,
            capture in proptest::prelude::any::<bool>(),
        ) {
            automatic_timeout(capture, delay);
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

    fn automatic_timeout(capture: bool, delay: u64) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (schedule, _) = Schedule::seeded(71);
        runtime.block_on(async {
            tokio::time::pause();
            schedule
                .scope(async {
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
}
