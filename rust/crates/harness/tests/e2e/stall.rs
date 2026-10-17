//! Names what each admitted scenario is waiting for, so an external stop reports a typed stall instead of an
//! anonymous timeout. Scenarios declare their open waits; nothing here measures time.
use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
static OPEN: Mutex<BTreeMap<usize, (PathBuf, &'static str)>> = Mutex::new(BTreeMap::new());

tokio::task_local! { static CURRENT: usize; }

/// One admitted scenario; dropping it retires its entry whether the scenario passed, failed or panicked.
pub(super) struct Admitted(usize);

impl Drop for Admitted {
    fn drop(&mut self) {
        OPEN.lock().unwrap().remove(&self.0);
    }
}

/// Admits `path` and runs `scenario` with its wait names attributed to it.
pub(super) async fn admitted<F: Future>(path: &Path, scenario: F) -> F::Output {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    OPEN.lock()
        .unwrap()
        .insert(id, (path.to_path_buf(), "admitted"));
    let _admitted = Admitted(id);
    CURRENT.scope(id, scenario).await
}

/// Declares the wait the current scenario is about to enter.
pub(super) fn waiting(on: &'static str) {
    if let Ok(id) = CURRENT.try_with(|id| *id)
        && let Some(entry) = OPEN.lock().unwrap().get_mut(&id)
    {
        entry.1 = on;
    }
}

/// Every unfinished scenario and the wait it last declared.
pub(super) fn report() -> String {
    OPEN.lock()
        .unwrap()
        .values()
        .map(|(path, on)| format!("stalled: {} at {on}", path.display()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// When the runner stops the process (nextest `terminate-after`), report each open wait before exiting.
#[cfg(unix)]
pub(super) fn report_on_termination() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::spawn(async move {
        if terminate.recv().await.is_some() {
            // Written to the process stderr directly: libtest's captured `eprintln!` output dies with the exit.
            let _ = std::io::Write::write_all(
                &mut std::io::stderr(),
                format!("{}\n", report()).as_bytes(),
            );
            std::process::exit(1);
        }
    });
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn report_on_termination() -> std::io::Result<()> {
    Ok(())
}

#[tokio::test]
async fn an_open_wait_is_reported_until_its_scenario_ends() {
    let path = Path::new("negative-control/stalled-follow.json");
    let (entered, stalled) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let scenario = tokio::spawn(admitted(path, async move {
        waiting("status follow");
        entered.send(()).unwrap();
        // A follow that never delivers until the control releases it.
        released.await.unwrap();
    }));
    stalled.await.unwrap();
    assert!(
        report()
            .lines()
            .any(|line| line == "stalled: negative-control/stalled-follow.json at status follow"),
        "{}",
        report()
    );
    release.send(()).unwrap();
    scenario.await.unwrap();
    assert!(!report().contains("negative-control/stalled-follow.json"));
}
