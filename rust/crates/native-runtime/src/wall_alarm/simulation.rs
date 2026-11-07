//! A scoped clock adapter over the original alarm index and callbacks.
use super::*;
use std::{future::Future, pin::Pin, task::Context};
use tokio::time::{Instant, Sleep};

#[derive(Default)]
struct Timer {
    sleep: Option<Pin<Box<Sleep>>>,
    driver: Option<std::task::Waker>,
    changed: bool,
}

#[derive(Default)]
pub(crate) struct Simulated {
    pub(super) index: Mutex<Index>,
    timer: Mutex<Timer>,
}

impl Simulated {
    pub(crate) fn producer_lost() -> io::Error {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "step=wall-alarm-producer; recovery=retain the original clock controller until its alarms settle",
        )
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        deadline: u64,
        fire: AlarmCallback,
    ) -> io::Result<WallAlarm> {
        let registration = Owner::Scoped(self.clone()).register(deadline, fire)?;
        self.changed();
        Ok(registration)
    }

    pub(crate) fn changed(&self) {
        let driver = {
            let mut timer = self.timer.lock().unwrap_or_else(PoisonError::into_inner);
            timer.changed = true;
            timer.driver.clone()
        };
        if let Some(driver) = driver {
            driver.wake();
        }
    }

    // Called by the original scoped driver, after its current user poll. No
    // per-alarm task or physical thread can publish readiness out of scope.
    pub(crate) fn poll(&self, now: u64, cx: &mut Context<'_>) {
        let mut driver = Driver {
            index: &self.index,
            due: None,
        };
        {
            let mut timer = self.timer.lock().unwrap_or_else(PoisonError::into_inner);
            timer.driver = Some(cx.waker().clone());
            let elapsed = timer
                .sleep
                .as_mut()
                .is_some_and(|sleep| sleep.as_mut().poll(cx).is_ready());
            if !std::mem::take(&mut timer.changed) && !elapsed {
                return;
            }
        }
        let (due, next) = locked(&self.index).take_due(now);
        driver.due = Some(due.into_entries(|_| {}));
        crate::delivery::deliver(driver.due.iter_mut().flatten(), |entry| {
            let fire = entry.into_parts().1;
            fire(Ok(()));
        });
        let mut timer = self.timer.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(until) = next.and_then(|deadline| {
            Instant::now().checked_add(std::time::Duration::from_millis(
                deadline.saturating_sub(now),
            ))
        }) {
            match &mut timer.sleep {
                Some(sleep) => sleep.as_mut().reset(until),
                None => timer.sleep = Some(Box::pin(tokio::time::sleep_until(until))),
            }
        } else {
            timer.sleep = None;
        }
        if let Some(sleep) = &mut timer.sleep {
            let _ = sleep.as_mut().poll(cx);
        }
    }

    pub(crate) fn close(&self) {
        let timer = {
            let mut timer = self.timer.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut *timer)
        };
        drop(timer);
        fail(&self.index, Self::producer_lost());
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::testing::{Schedule, seeded_runtime, unix_millis};

    fn original_alarm(
        deadline: u64,
    ) -> (
        WallAlarm,
        tokio::sync::oneshot::Receiver<Result<(), Arc<io::Error>>>,
    ) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let sender = Mutex::new(Some(sender));
        let alarm = WallAlarm::register(
            deadline,
            Arc::new(move |outcome| {
                if let Some(sender) = sender.lock().unwrap().take() {
                    sender.send(outcome).ok();
                }
            }),
        )
        .unwrap();
        (alarm, receiver)
    }

    #[test]
    fn scoped_alarms_use_the_original_clock_and_retain_producer_loss() {
        let runtime = seeded_runtime(71).unwrap();
        for initialized in [false, true] {
            let (schedule, _) = Schedule::seeded(71);
            let late = runtime.block_on(schedule.scope(async {
                if initialized {
                    let (alarm, _) = original_alarm(unix_millis().unwrap() + 100);
                    drop(alarm);
                }
                crate::testing::inherit_host(|| {
                    WallAlarm::register(unix_millis().unwrap() + 100, Arc::new(|_| {}))
                })
            }));
            drop(schedule);
            let error = match std::thread::spawn(late).join().unwrap() {
                Ok(_) => panic!("late callback admitted an alarm after its clock producer closed"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert!(error.to_string().contains("step=wall-alarm-producer"));
        }
        let (schedule, _) = Schedule::seeded(71);
        let (held, mut result) = runtime.block_on(async {
            tokio::time::pause();
            schedule
                .scope(async {
                    let now = unix_millis().unwrap();
                    let start = Instant::now();
                    let (alarm, quiet) = original_alarm(now + 10);
                    quiet.await.unwrap().unwrap();
                    assert!(
                        start.elapsed() >= std::time::Duration::from_millis(10),
                        "physical wall time fired the original scoped alarm early",
                    );
                    drop(alarm);

                    let start = Instant::now();
                    let now = unix_millis().unwrap();
                    let (alarm, jumped) = original_alarm(now + 100);
                    schedule.set_unix_millis(now - 100);
                    schedule.set_unix_millis(now + 100);
                    jumped.await.unwrap().unwrap();
                    assert_eq!(start.elapsed(), std::time::Duration::ZERO);
                    drop(alarm);

                    let (cancelled, result) = original_alarm(unix_millis().unwrap() + 100);
                    drop(cancelled);
                    assert!(result.await.is_err(), "a cancelled original callback fired");
                    original_alarm(unix_millis().unwrap() + 1000)
                })
                .await
        });
        drop(schedule);
        let error = result
            .try_recv()
            .expect("original clock producer was dropped without settling its alarm")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("step=wall-alarm-producer"));
        drop(held);
    }
}
