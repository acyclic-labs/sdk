//! Every child process this runtime starts is created on one thread.
//!
//! Creating a process can leave Windows holding per-thread state for the
//! creating thread for as long as that thread lives: starting an app
//! execution alias (a Store app's `pwsh.exe`, say) goes through RPC, whose
//! runtime keeps a handle to the calling thread and an event for it. Created
//! on whichever pool thread ran each request, that state grows with the
//! number of threads that ever spawned, up to every pool thread; created on
//! this one thread, it is held once per process whatever the spawn count.
use std::io;
use std::process::{Child, Command};
use std::sync::{LazyLock, mpsc};

type Job = Box<dyn FnOnce() + Send>;

static SPAWNER: LazyLock<io::Result<mpsc::Sender<Job>>> = LazyLock::new(|| {
    let (sender, receiver) = mpsc::channel::<Job>();
    std::thread::Builder::new()
        .name("acyclic-spawn".to_owned())
        // Each job catches its own panic, so the thread serves for the life
        // of the process and the channel never closes.
        .spawn(move || {
            while let Ok(job) = receiver.recv() {
                job();
            }
        })?;
    Ok(sender)
});

/// The threads process creation ran on, so tests see that it ran on one.
#[cfg(test)]
pub(super) static CREATED_ON: std::sync::Mutex<Vec<std::thread::ThreadId>> =
    std::sync::Mutex::new(Vec::new());

/// Spawns `command` on the spawner thread, waiting for the result.
pub(super) fn spawn(command: &mut Command) -> io::Result<Child> {
    let sender = SPAWNER
        .as_ref()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
    // The command travels to the spawner and back, so the caller keeps it
    // (with whatever a failed spawn left in it) exactly as an inline spawn
    // would.
    let mut owned = std::mem::replace(command, Command::new(""));
    let (reply, result) = mpsc::sync_channel(1);
    let job: Job = Box::new(move || {
        #[cfg(test)]
        CREATED_ON
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(std::thread::current().id());
        let spawned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owned.spawn()))
            .unwrap_or_else(|_| Err(io::Error::other("process creation panicked")));
        let _ = reply.send((owned, spawned));
    });
    sender
        .send(job)
        .map_err(|_| io::Error::other("the process spawner has stopped"))?;
    let (owned, spawned) = result
        .recv()
        .map_err(|_| io::Error::other("the process spawner has stopped"))?;
    *command = owned;
    spawned
}
