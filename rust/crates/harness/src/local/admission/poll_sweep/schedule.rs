//! Push barriers around the real native submissions; no timer or poll-index cuts.
use acyclic_native_runtime::testing::{Boundary, Schedule as IoSchedule};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Default)]
struct State {
    cancelled: bool,
    active: usize,
    trace: Vec<String>,
    points: Vec<&'static str>,
    publication: Arc<str>,
    cancelled_at: Option<String>,
    admission_submission: Option<usize>,
}

pub(super) struct Schedule {
    state: Mutex<State>,
    io: IoSchedule,
}

impl Schedule {
    pub fn new() -> (Arc<Self>, UnboundedReceiver<Boundary>) {
        let (io, events) = IoSchedule::new();
        (
            Arc::new(Self {
                state: Mutex::default(),
                io,
            }),
            events,
        )
    }
    // Sole test-state lock; no callback, send, or await while held.
    pub fn start(&self) {
        *self.state.lock().unwrap() = State::default();
    }
    pub async fn point(&self, name: &'static str) {
        acyclic_native_runtime::testing::boundary(name).await;
    }
    pub fn describe(&self, publication: impl FnOnce() -> String) {
        let publication = publication();
        self.state.lock().unwrap().publication = publication.clone().into();
        acyclic_native_runtime::testing::describe(|| publication);
    }
    pub fn failure(&self) -> String {
        let state = self.state.lock().unwrap();
        state
            .cancelled_at
            .clone()
            .unwrap_or_else(|| state.publication.to_string())
    }
    pub fn dispatch(self: &Arc<Self>, label: &'static str) -> Active {
        let mut state = self.state.lock().unwrap();
        assert!(
            !state.cancelled,
            "external dispatch after cancellation: {label}"
        );
        state.active += 1;
        state.trace.push(label.into());
        Active(self.clone())
    }
    pub fn drained(&self) -> Vec<String> {
        assert_eq!(
            self.state.lock().unwrap().active,
            0,
            "external effects remain active"
        );
        let trace = self.io.trace();
        let cancel = trace.iter().position(|record| record == "CANCEL").unwrap();
        assert!(
            trace[cancel + 1..]
                .iter()
                .all(|record| record.ends_with(":completion")),
            "new dispatch after cancellation: {trace:?}"
        );
        trace
    }
    pub fn recover(&self) {
        self.state.lock().unwrap().cancelled = false;
    }
    pub fn admission_submission(&self) -> Option<usize> {
        self.state.lock().unwrap().admission_submission
    }
    pub fn ready_reads(&self) -> usize {
        self.io.ready_reads()
    }
    pub fn points(&self) -> Vec<&'static str> {
        self.state.lock().unwrap().points.clone()
    }
}

pub(super) struct Active(Arc<Schedule>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().active -= 1;
    }
}

pub(super) enum Exit<T> {
    Cancelled,
    Completed(T),
}

/// Drives `future`, cancelling at the `cut`-th boundary that `counted`
/// selects. Returns the exit, every boundary hit, and the counted ones.
///
/// Physical blocking and native work is atomic in the schedule: once its
/// submission is released, every other boundary waits until that work's
/// completion boundary has been taken. Its owner reports completion from a
/// real worker thread, so without this the completion would interleave with
/// independent owners' boundaries by wall-clock timing.
pub(super) async fn drive<F: Future>(
    future: F,
    cut: Option<usize>,
    counted: fn(&str) -> bool,
    schedule: &Schedule,
    events: &mut UnboundedReceiver<Boundary>,
) -> (Exit<F::Output>, usize, usize) {
    let mut future = Box::pin(schedule.io.scope(tokio::task::unconstrained(future)));
    let mut hit = 0;
    let mut ordinal = 0;
    let mut physical = false;
    let mut deferred = std::collections::VecDeque::new();
    loop {
        let next = if physical { None } else { deferred.pop_front() };
        let event = match next {
            Some(event) => event,
            None => tokio::select! {
                // Poll the caller's ready continuation before acknowledging another
                // queued barrier. Random branch priority changes the replay order
                // when a blocked read and an owned completion are both ready.
                biased;
                output = &mut future => {
                    drop(future);
                    release(deferred);
                    drain(schedule, events).await;
                    return (Exit::Completed(output), hit, ordinal);
                },
                event = events.recv() => {
                    let event = event.expect("provider lost its push driver");
                    if physical && !completes_physical(event.name) {
                        deferred.push_back(event);
                        continue;
                    }
                    physical = false;
                    event
                }
            },
        };
        hit += 1;
        let counts = counted(event.name);
        ordinal += usize::from(counts);
        {
            let mut state = schedule.state.lock().unwrap();
            if event.name == "task:submitted"
                && (event.detail.contains("append harness/v3/coordinator/events ")
                    || (event.detail.starts_with("commit ")
                        && event.detail.contains("\"harness/v3/coordinator/events\"")))
            {
                state.admission_submission.get_or_insert(hit);
            }
            state.points.push(event.name);
            state.publication = event.detail.clone();
        }
        if counts && cut == Some(ordinal) {
            {
                let mut state = schedule.state.lock().unwrap();
                state.cancelled = true;
                state.cancelled_at = Some(format!("{}: {}", event.name, event.detail));
            }
            schedule.io.cancel();
            // A JoinHandle's drop detaches its already submitted work.
            // Cancel the caller first, then release and drain those owners.
            drop(future);
            event.release();
            release(deferred);
            drain(schedule, events).await;
            return (Exit::Cancelled, hit, ordinal);
        }
        physical = submits_physical(event.name);
        event.release();
    }
}

fn submits_physical(name: &str) -> bool {
    matches!(name, "blocking:submitted" | "native:submitted")
}

fn completes_physical(name: &str) -> bool {
    matches!(name, "blocking:completed" | "native:completed")
}

fn release(deferred: std::collections::VecDeque<Boundary>) {
    deferred.into_iter().for_each(Boundary::release);
}

async fn drain(schedule: &Schedule, events: &mut UnboundedReceiver<Boundary>) {
    loop {
        tokio::select! {
            _ = schedule.io.drained() => break,
            event = events.recv() => event.unwrap().release(),
        }
    }
}

pub(super) fn check_cut<T>(exit: &Exit<T>, cut: usize, hit: usize) -> Result<(), &'static str> {
    if matches!(exit, Exit::Cancelled) && cut == hit {
        Ok(())
    } else {
        Err("cancellation point was skipped")
    }
}

/// Which boundary reaches the driver first while the blocking job runs.
#[derive(Clone, Copy, Debug)]
enum Arrival {
    /// The independent boundary is queued before the job may finish.
    IndependentFirst,
    /// The job's completion is queued before the independent boundary is raised.
    CompletionFirst,
}

/// Signals the blocking job when dropped, so a job waiting for the independent boundary also
/// ends when the independent owner finishes without raising it.
struct Unblock(std::sync::mpsc::Sender<()>);

impl Drop for Unblock {
    fn drop(&mut self) {
        drop(self.0.send(()));
    }
}

/// One driven race: its exit, boundaries hit and counted, the named points, and whether the
/// independent owner raised its boundary and then ran to its end.
struct Race {
    cancelled: bool,
    hit: usize,
    ordinal: usize,
    points: Vec<&'static str>,
    raised: bool,
    finished: bool,
}

/// Drives one blocking job racing an independent owned task, cancelling at the `cut`-th
/// boundary. Boundaries reach the driver through a forwarder that reports what it has queued,
/// so each arrival order is forced by a handshake, not by timing. The independent boundary is
/// raised by an owned task that outlives a cut, so a deferred boundary the driver fails to
/// release panics that owner instead of vanishing with the cancelled caller.
fn physical_race(arrival: Arrival, cut: Option<usize>) -> Race {
    use std::sync::atomic::{AtomicBool, Ordering};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (schedule, mut events) = Schedule::new();
        schedule.start();
        let (forward, mut forwarded) = tokio::sync::mpsc::unbounded_channel();
        let (independent_queued, independent_seen) = std::sync::mpsc::channel();
        let unblock = Unblock(independent_queued.clone());
        let (completion_queued, completion_seen) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut completion_queued = Some(completion_queued);
            while let Some(boundary) = events.recv().await {
                let name = boundary.name;
                if forward.send(boundary).is_err() {
                    break;
                }
                match name {
                    "independent" => drop(independent_queued.send(())),
                    "blocking:completed" => {
                        if let Some(queued) = completion_queued.take() {
                            drop(queued.send(()));
                        }
                    }
                    _ => {}
                }
            }
        });
        let (raised, finished) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let (started, running) = tokio::sync::oneshot::channel();
        let owner = schedule.clone();
        let (raising, finishing) = (raised.clone(), finished.clone());
        let (begin, begun) = tokio::sync::oneshot::channel();
        let raced = async move {
            // Both are submitted inside the driven scope, so each owns its barriers and outlives
            // a cut of the caller. The independent owner is submitted and running before the job
            // is: a submission queued behind the job's would wait for its completion, which in
            // turn waits for the independent boundary.
            let independent = acyclic_native_runtime::testing::spawn(async move {
                let _unblock = unblock;
                drop(begin.send(()));
                let ran = running.await.is_ok();
                if ran && matches!(arrival, Arrival::CompletionFirst) {
                    drop(completion_seen.await);
                }
                raising.store(true, Ordering::SeqCst);
                owner.point("independent").await;
                finishing.store(true, Ordering::SeqCst);
            });
            drop(begun.await);
            let job = acyclic_native_runtime::testing::spawn_blocking(move || {
                drop(started.send(()));
                if matches!(arrival, Arrival::IndependentFirst) {
                    drop(independent_seen.recv());
                }
            });
            let (job, independent) = futures::join!(job, independent);
            job.unwrap();
            independent.unwrap();
        };
        let (exit, hit, ordinal) = drive(raced, cut, |_| true, &schedule, &mut forwarded).await;
        Race {
            cancelled: matches!(exit, Exit::Cancelled),
            hit,
            ordinal,
            points: schedule.points(),
            raised: raised.load(Ordering::SeqCst),
            finished: finished.load(Ordering::SeqCst),
        }
    })
}

/// The race's named points, without the independent task's own submission boundaries.
fn named(points: &[&'static str]) -> Vec<&'static str> {
    points
        .iter()
        .copied()
        .filter(|name| {
            matches!(
                *name,
                "blocking:submitted" | "blocking:completed" | "independent"
            )
        })
        .collect()
}

#[test]
fn physical_completion_precedes_boundaries_raised_while_it_runs() {
    let expected = ["blocking:submitted", "blocking:completed", "independent"];
    for arrival in [Arrival::IndependentFirst, Arrival::CompletionFirst] {
        let race = physical_race(arrival, None);
        assert!(!race.cancelled, "{arrival:?}");
        assert!(race.raised && race.finished, "{arrival:?}");
        assert_eq!(named(&race.points), expected, "{arrival:?}");
    }
}

/// Every cut point of every arrival order, including a cut at the physical completion while
/// the independent boundary is deferred behind it: the drive cancels exactly there, drains
/// all owned work, and releases every deferred boundary, so each owner that raised one ends.
#[test]
fn every_cut_releases_deferred_boundaries_and_drains_owned_work() {
    let mut deferred_cut = false;
    for arrival in [Arrival::IndependentFirst, Arrival::CompletionFirst] {
        let boundaries = physical_race(arrival, None).hit;
        for cut in 1..=boundaries {
            let race = physical_race(arrival, Some(cut));
            assert!(race.cancelled, "{arrival:?} cut {cut}: {:?}", race.points);
            assert_eq!(
                (race.hit, race.ordinal),
                (cut, cut),
                "{arrival:?} cut {cut}"
            );
            assert_eq!(
                race.raised, race.finished,
                "{arrival:?} cut {cut}: {:?}",
                race.points
            );
            let at = race.points[cut - 1];
            deferred_cut |= matches!(arrival, Arrival::IndependentFirst)
                && at == "blocking:completed"
                && race.raised;
        }
    }
    assert!(
        deferred_cut,
        "no cut fell on a completion with a deferred boundary"
    );
}
