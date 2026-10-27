//! A test pause belongs to the exact source enrolled by its mounting test.

use super::MountFilesystem;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

struct Gate {
    source: Weak<dyn MountFilesystem>,
    admitted: SyncSender<()>,
    release: Receiver<()>,
}

static NEXT_WRITE: OnceLock<Mutex<Option<Gate>>> = OnceLock::new();

pub(in crate::native_mount) struct WritePause {
    source: Weak<dyn MountFilesystem>,
    pub(in crate::native_mount) admitted: Receiver<()>,
    pub(in crate::native_mount) release: SyncSender<()>,
}

impl Drop for WritePause {
    fn drop(&mut self) {
        let mut pending = NEXT_WRITE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if pending
            .as_ref()
            .is_some_and(|gate| Weak::ptr_eq(&gate.source, &self.source))
        {
            pending.take();
        }
        // If the callback already took the gate, dropping our sender releases
        // its receive. The enclosing thread scope then joins that actual write.
    }
}

pub(in crate::native_mount) fn pause_next_write_after_admission(
    source: Arc<dyn MountFilesystem>,
) -> WritePause {
    let (admitted, observed) = sync_channel(0);
    let (release, released) = sync_channel(0);
    let source = Arc::downgrade(&source);
    *NEXT_WRITE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(Gate {
        source: source.clone(),
        admitted,
        release: released,
    });
    WritePause {
        source,
        admitted: observed,
        release,
    }
}

pub(in crate::native_mount::fuse) fn pause(source: &Arc<dyn MountFilesystem>) {
    let gate = {
        let mut pending = NEXT_WRITE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if pending.as_ref().is_some_and(|gate| {
            gate.source
                .upgrade()
                .is_some_and(|enrolled| Arc::ptr_eq(&enrolled, source))
        }) {
            pending.take()
        } else {
            None
        }
    };
    if let Some(gate) = gate {
        let _ = gate.admitted.send(());
        let _ = gate.release.recv_timeout(Duration::from_secs(10));
    }
}
