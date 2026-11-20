use std::io;
use std::sync::OnceLock;

use signal_hook::consts::signal::{SIGCONT, SIGHUP, SIGINT, SIGTERM, SIGTSTP};
use signal_hook::iterator::Signals;

use super::lifecycle::{self, Arc, Mutex, Owner};
use super::tty::RawTty;

// Never unregister the last signal action: signal-hook does not restore the
// OS disposition. One permanent dispatcher preserves default behavior without
// an owner and does not retain dropped terminal descriptors or spawn per open.
static OWNER: OnceLock<io::Result<Arc<Owner<RawTty>>>> = OnceLock::new();

pub(crate) fn owner() -> io::Result<&'static Owner<RawTty>> {
    OWNER
        .get_or_init(|| {
            let mut signals = Signals::new([SIGTERM, SIGHUP, SIGINT, SIGTSTP, SIGCONT])?;
            let owner = Arc::new(Mutex::new(None));
            let worker_owner = Arc::clone(&owner);
            std::thread::Builder::new()
                .name("terminal-signals".into())
                .spawn(move || {
                    for signal in signals.forever() {
                        let _ = lifecycle::with_owner(&worker_owner, |state| {
                            if signal == SIGCONT {
                                if let Some(state) = state {
                                    let _ = lifecycle::resume(state, RawTty::reapply);
                                }
                            } else {
                                let terminating = signal != SIGTSTP;
                                if let Some(state) = state {
                                    let _ = lifecycle::restore(state, terminating, RawTty::restore);
                                }
                                // With or without a terminal owner, preserve
                                // termination/suspension's default disposition.
                                let _ = signal_hook::low_level::emulate_default_handler(signal);
                                if !terminating && let Some(state) = state {
                                    let _ = lifecycle::resume(state, RawTty::reapply);
                                }
                            }
                        });
                    }
                })?;
            Ok(owner)
        })
        .as_ref()
        .map(AsRef::as_ref)
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}
