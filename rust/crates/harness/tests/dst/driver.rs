//! The controller outlives the killed turn and all of its accepted operations.
use super::*;
use acyclic_native_runtime::{
    ProcessCaptureFailure, ProcessInput,
    testing::{Boundary, DurableFault, InjectedStorageFault, ProcessCapture, Schedule},
};
use std::time::Duration;
use tokio::sync::mpsc;

pub(super) enum Outcome<T> {
    Completed(T),
    Crashed(InjectedStorageFault),
}

pub(super) struct Driver {
    pub schedule: Schedule,
    events: mpsc::UnboundedReceiver<Boundary>,
    pub boundaries: Vec<(String, String)>,
}

const UNSIMULATED_PROCESS: &str = "unsimulated process reached a seeded replay";

impl Driver {
    pub fn new(seed: u64, cut: Option<(usize, DurableFault)>) -> Self {
        Self::from_scope(Schedule::seeded(seed), cut).refuse_processes()
    }

    pub fn without_entropy(seed: u64) -> Self {
        Self::from_scope(Schedule::without_entropy(seed), None).refuse_processes()
    }

    fn from_scope(
        (schedule, events): (Schedule, mpsc::UnboundedReceiver<Boundary>),
        cut: Option<(usize, DurableFault)>,
    ) -> Self {
        if let Some((ordinal, fault)) = cut {
            schedule.cut_at(ordinal, fault);
        }
        Self {
            schedule,
            events,
            boundaries: Vec::new(),
        }
    }

    // Without a scripted provider the original capture would spawn a real OS
    // process inside the replay. Refuse it, and fail the run that reached it.
    fn refuse_processes(self) -> Self {
        self.schedule.set_process_provider(|_, _, _| {
            Err(ProcessCaptureFailure {
                error: std::io::Error::other(UNSIMULATED_PROCESS),
                status: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                cleanup_completed: true,
            })
        });
        self
    }

    pub fn run<'a, T: 'a>(&'a mut self, future: EntryFuture<'a, T>) -> EntryFuture<'a, Outcome<T>> {
        entry(async move {
            let schedule = self.schedule.clone();
            let mut turn = entry(schedule.scope(future));
            // This deadline is an oracle: exceeding it fails the simulation.
            // It does not provide the production operation's missing timeout.
            let deadline = tokio::time::sleep(Duration::from_secs(30));
            tokio::pin!(deadline);
            let outcome = loop {
                tokio::select! {
                    biased;
                    event = self.events.recv() => {
                        let event = event.expect("lost boundary driver");
                        self.boundaries.push((event.name.into(), event.detail.to_string()));
                        if let Some(cause) = schedule.storage_fault() {
                            schedule.cancel();
                            // Drop all caller state before any accepted completion
                            // receives its acknowledgement permit.
                            drop(turn);
                            event.release();
                            break Outcome::Crashed(cause);
                        }
                        event.release();
                    },
                    result = &mut turn => {
                        let cause = schedule.storage_fault();
                        if cause.is_some() {
                            schedule.cancel();
                        }
                        drop(turn);
                        break match cause {
                            Some(cause) => {
                                drop(result);
                                Outcome::Crashed(cause)
                            },
                            None => Outcome::Completed(result),
                        };
                    },
                    () = &mut deadline => panic!(
                        "turn exceeded the simulated deadline; last boundary: {:?}; pending: {}; polls: {:?}; trace: {:?}",
                        self.boundaries.last(), schedule.pending_native_work(),
                        schedule.polls().iter().rev().take(20).collect::<Vec<_>>(),
                        schedule.trace().iter().rev().take(10).collect::<Vec<_>>()
                    ),
                }
            };
            loop {
                tokio::select! {
                    biased;
                    event = self.events.recv() => {
                        let event = event.expect("lost drain driver");
                        self.boundaries.push((event.name.into(), event.detail.to_string()));
                        event.release();
                    },
                    () = schedule.drained() => break,
                    () = &mut deadline => panic!("accepted work exceeded the simulated deadline"),
                }
            }
            let spawned = schedule.trace();
            let spawned = spawned.iter().find(|event| event.starts_with("process:"));
            assert!(spawned.is_none(), "{UNSIMULATED_PROCESS}: {spawned:?}");
            outcome
        })
    }
}

#[test]
fn every_file_cut_drops_the_turn_before_independent_reopen() -> Result<()> {
    use std::io::Read;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let directory = tempfile::tempdir().map_err(storage)?;
        let path = directory.path().join("journal");
        let write = |path: std::path::PathBuf| -> EntryFuture<'static, std::io::Result<()>> {
            entry(async move {
                let file = std::fs::File::create(path)?;
                // Real original worker, held file, and physical write calls.
                acyclic_native_runtime::testing::spawn_blocking(move || {
                    let mut file = file;
                    acyclic_native_runtime::write_file_all(&mut file, b"first")?;
                    acyclic_native_runtime::write_file_all(&mut file, b"second")
                })
                .await?
            })
        };
        let mut baseline = Driver::new(71, None);
        match baseline.run(write(path.clone())).await {
            Outcome::Completed(result) => result.map_err(storage)?,
            Outcome::Crashed(cause) => panic!("uncut run crashed: {cause}"),
        }
        let writes = baseline.schedule.durable_writes();
        assert_eq!(writes.len(), 2, "original writes escaped enumeration");
        for cut in 0..writes.len() {
            // Every fault, including a tear at each prefix of the offered bytes.
            let offered_len = writes[cut].requested;
            let faults = [
                DurableFault::Before,
                DurableFault::After,
                DurableFault::Failed,
                DurableFault::DiskFull,
            ]
            .into_iter()
            .chain((0..offered_len).map(DurableFault::Torn));
            for fault in faults {
                let mut driver = Driver::new(71, Some((cut, fault)));
                let Outcome::Crashed(cause) = driver.run(write(path.clone())).await else {
                    panic!("cut {cut} {fault:?} did not kill the original turn");
                };
                assert_eq!((cause.ordinal, cause.fault), (cut, fault));
                assert_eq!(driver.schedule.durable_writes().len(), cut + 1);
                drop(driver);
                let mut restored = Vec::new();
                std::fs::File::open(&path)
                    .map_err(storage)?
                    .read_to_end(&mut restored)
                    .map_err(storage)?;
                let prefix = if cut == 0 { b"".as_slice() } else { b"first" };
                let offered = if cut == 0 {
                    b"first".as_slice()
                } else {
                    b"second"
                };
                let retained = match fault {
                    DurableFault::After => offered.len(),
                    DurableFault::Torn(count) => count,
                    _ => 0,
                };
                assert_eq!(restored, [prefix, &offered[..retained]].concat());
            }
        }
        Ok(())
    })
}

#[test]
#[should_panic(expected = "unsimulated process reached a seeded replay")]
fn a_replay_without_a_scripted_process_refuses_the_os() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::pause();
        let mut driver = Driver::new(71, None);
        let capture = entry(async {
            let command = std::process::Command::new("must-not-spawn");
            acyclic_native_runtime::testing::capture_process(
                ProcessCapture {
                    command: &command,
                    timeout: Duration::from_secs(1),
                    maximum: 1,
                    input: None,
                },
                &mut || Ok(()),
                &mut |_| Ok(ProcessInput::default()),
            )
            .expect("the original OS path ran inside the replay")
            .expect_err("refusal must reach the original caller")
        });
        let Outcome::Completed(failure) = driver.run(capture).await else {
            panic!("process refusal crashed the scope");
        };
        assert_eq!(failure.error.to_string(), UNSIMULATED_PROCESS);
    });
}
