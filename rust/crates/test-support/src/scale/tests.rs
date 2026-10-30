//! Deterministic lifecycle simulation and adversarial observation controls.
use super::*;
use std::{
    collections::BTreeMap,
    hint::black_box,
    pin::pin,
    task::{Context, Poll, Waker},
};

const RECOVERY: &str = "cargo test -p acyclic-test-support scale::tests";

#[derive(Clone, Copy, PartialEq)]
enum Inject {
    None,
    Scan,
    Thread,
    Timer,
    LostProducer,
}

struct Model {
    agents: u64,
    phase: [u64; LIFECYCLE_OPERATIONS.len()],
    inject: Inject,
    // Real threads retain a channel receiver, not a timed sleep.
    threads: Vec<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
    // Virtual timer registrations retain their actual cancellation producer.
    timers: BTreeMap<u64, std::sync::mpsc::Sender<()>>,
}

impl Drop for Model {
    fn drop(&mut self) {
        for (stop, thread) in self.threads.drain(..) {
            drop(stop);
            thread
                .join()
                .expect("injected thread must retire on producer loss");
        }
    }
}

impl Population for Model {
    async fn operation(&mut self, operation: Operation, agent: u64) -> Result<Work, Failure> {
        let phase = LIFECYCLE_OPERATIONS
            .iter()
            .position(|&op| op == operation)
            .expect("known operation");
        if self.phase[phase] != agent || (phase != 0 && self.phase[phase - 1] != self.agents) {
            return Err(failure(
                "lifecycle safety",
                format!("{operation:?} agent={agent}"),
                RECOVERY,
            ));
        }
        // A dropped original producer is a typed terminal, never a deadline.
        let (commit, completed) = std::sync::mpsc::channel();
        if self.inject != Inject::LostProducer {
            commit.send(()).expect("consumer alive");
        }
        drop(commit);
        completed
            .recv()
            .map_err(|error| failure("commit event", error.to_string(), RECOVERY))?;
        self.phase[phase] += 1;
        let mut work = Work([Some(1); METRICS.len()]);
        if operation == Operation::Turn && self.inject == Inject::Scan {
            let mut visits = 0;
            for index in 0..self.agents {
                black_box(index);
                visits += 1;
            }
            work.0[5] = Some(visits);
        }
        if operation == Operation::Create {
            match self.inject {
                Inject::Thread => {
                    let (stop, stopped) = std::sync::mpsc::channel();
                    let (ready, entered) = std::sync::mpsc::channel();
                    let thread = std::thread::spawn(move || {
                        ready.send(()).expect("arrival consumer alive");
                        assert!(stopped.recv().is_err());
                    });
                    entered.recv().expect("original thread publishes readiness");
                    self.threads.push((stop, thread));
                }
                Inject::Timer => {
                    let (alarm, cancelled) = std::sync::mpsc::channel();
                    self.timers.insert(agent, alarm);
                    drop(cancelled);
                }
                _ => {}
            }
        }
        Ok(work)
    }
    fn idle(&mut self) -> Result<Idle, Failure> {
        Ok(Idle {
            bytes: Some(self.agents * 64),
            threads: Some(self.threads.len() as u64),
            timers: Some(self.timers.len() as u64),
        })
    }
    async fn close(self) -> Result<(), Failure> {
        if self.phase != [self.agents; LIFECYCLE_OPERATIONS.len()] {
            return Err(failure(
                "lifecycle terminal",
                format!("phases={:?}", self.phase),
                RECOVERY,
            ));
        }
        Ok(())
    }
}

fn simulate(inject: Inject) -> Result<Report, Failure> {
    let future = run(
        &[1, 4, 16],
        &LIFECYCLE_OPERATIONS,
        |agents| async move {
            Ok(Model {
                agents,
                phase: [0; LIFECYCLE_OPERATIONS.len()],
                inject,
                threads: Vec::new(),
                timers: BTreeMap::new(),
            })
        },
        RECOVERY,
    );
    // All modeled events publish on the operation path before its await.
    // A pending result here is a broken progress proof, not a reason to retry.
    match pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => Err(failure(
            "simulation progress",
            "original event producer did not complete".into(),
            RECOVERY,
        )),
    }
}

#[test]
fn lifecycle_detects_scan_thread_timer_and_lost_producer() {
    let flat = simulate(Inject::None).expect("event-driven lifecycle");
    assert!(flat.findings(&LIFECYCLE_OPERATIONS, RECOVERY).is_empty());
    assert_eq!(
        flat.qualify(&LIFECYCLE_OPERATIONS, RECOVERY)
            .unwrap_err()
            .step,
        "population coverage"
    );
    for (inject, expected) in [
        (Inject::Scan, "constant work"),
        (Inject::Thread, "idle resources"),
        (Inject::Timer, "idle resources"),
    ] {
        let report = simulate(inject).expect("injected lifecycle completes before checking cost");
        assert!(
            report
                .findings(&LIFECYCLE_OPERATIONS, RECOVERY)
                .iter()
                .any(|finding| finding.step == expected),
            "{report:?}"
        );
    }
    assert_eq!(
        simulate(Inject::LostProducer).unwrap_err().step,
        "commit event"
    );
}

#[test]
fn rejects_missing_coverage_hidden_tail_work_memory_growth_and_forged_reports() {
    let original = simulate(Inject::None).expect("lifecycle");
    let mut missing = original.clone();
    missing.points[1].samples[2].work[0].missing = 1;
    assert!(
        missing
            .findings(&LIFECYCLE_OPERATIONS, RECOVERY)
            .iter()
            .any(|failure| failure.step == "counter coverage")
    );
    let mut tail = original.clone();
    let counter = &mut tail.points[2].samples[2].work[1];
    // The mean falls below the smaller point's mean, but one op becomes dearer.
    counter.total = 2;
    counter.maximum = 2;
    assert!(
        tail.findings(&LIFECYCLE_OPERATIONS, RECOVERY)
            .iter()
            .any(|failure| failure.step == "constant work")
    );
    let mut memory = original.clone();
    memory.points[2].idle.bytes = Some(16 * 64 + 1);
    assert!(
        memory
            .findings(&LIFECYCLE_OPERATIONS, RECOVERY)
            .iter()
            .any(|failure| failure.step == "constant idle memory")
    );
    let mut forged = original;
    forged.points[1].samples.pop();
    assert!(
        forged
            .findings(&LIFECYCLE_OPERATIONS, RECOVERY)
            .iter()
            .any(|failure| failure.step == "report shape")
    );
    assert_eq!(
        Report::default().findings(&LIFECYCLE_OPERATIONS, RECOVERY)[0].step,
        "report shape"
    );
    // Maximum-width counts must not overflow a ratio comparison or round away
    // one extra unit of work at a large population.
    let n = u64::MAX;
    let total = u128::from(n) * u128::from(n);
    assert!(mean_grows(total - 1, n, total, n));
    assert!(!mean_grows(total, n, total - 1, n));
}

#[test]
fn fixed_sample_counts_use_observations_instead_of_population_as_denominator() {
    // Time and work share a fixed sample count at each resident population.
    // Dividing by population would conceal this injected allocation-free scan.
    let mut report = Report::default();
    for agents in POPULATIONS {
        let mut sample = Sample::new(Operation::ObjectHead);
        for _ in 0..4 {
            sample
                .record(Work([Some(1); METRICS.len()]), RECOVERY)
                .expect("bounded observations");
        }
        report.points.push(Point {
            agents,
            samples: vec![sample],
            idle: Idle {
                bytes: Some(agents * 64),
                threads: Some(0),
                timers: Some(0),
            },
        });
    }
    let operations = [Operation::ObjectHead];
    report
        .qualify(&operations, RECOVERY)
        .expect("constant sampled work");
    let counter = &mut report.points[2].samples[0].work[5];
    counter.total = 8;
    counter.maximum = 2;
    assert!(
        report
            .findings(&operations, RECOVERY)
            .iter()
            .any(|finding| finding.step == "constant work")
    );
    report.points[1].samples[0].observations = 0;
    assert!(
        report
            .findings(&operations, RECOVERY)
            .iter()
            .any(|finding| finding.step == "report shape")
    );
    assert!(
        report
            .findings(&[Operation::ObjectHead, Operation::ObjectHead], RECOVERY)
            .iter()
            .any(|finding| finding.step == "report shape")
    );
}
