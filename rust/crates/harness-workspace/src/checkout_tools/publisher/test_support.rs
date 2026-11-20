//! Negative-control switches the publication tests flip on this thread.
#![cfg(all(any(test, feature = "test-support"), not(loom)))]

std::thread_local! {
    /// Negative control: synchronize and advance the mount from before checkout.
    pub static PUBLISH_TO_INITIAL_MOUNT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Counterfactual: reread and hash even an identical authenticated payload.
    pub static REREAD_MATCHING_PAYLOAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Stops a publication after it owes its write, before the write.
    pub static KILL_BEFORE_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Negative control: observation requires access to every mount.
    pub static OBSERVE_EVERY_MOUNT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Negative control: a replay never captures, as before the fix.
    pub static REPLAY_SKIPS_CAPTURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
