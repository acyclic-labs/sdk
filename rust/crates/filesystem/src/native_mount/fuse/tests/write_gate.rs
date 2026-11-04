//! A write pause is owned by the mounted projection that consumes it.

use super::super::ProjectionCore;
use crate::native_mount::{NativeMountError, NativeMountSession};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, PoisonError, Weak};

struct Gate {
    admitted: SyncSender<()>,
    release: Receiver<()>,
}

#[derive(Default)]
pub(in crate::native_mount::fuse) struct PauseSlot {
    // Taking the pending callback does not end the original guard's lease.
    active: bool,
    pending: Option<Gate>,
}

pub(in crate::native_mount) struct WritePause {
    core: Weak<ProjectionCore>,
    pub(in crate::native_mount) admitted: Receiver<()>,
    pub(in crate::native_mount) release: SyncSender<()>,
}

impl Drop for WritePause {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            let mut slot = core
                .write_pause
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            slot.pending.take();
            slot.active = false;
        }
        // Dropping the release sender ends an already admitted callback's wait.
    }
}

fn enroll(core: &Arc<ProjectionCore>) -> Result<WritePause, NativeMountError> {
    let mut slot = core
        .write_pause
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if slot.active {
        return Err(NativeMountError::ControlPending);
    }
    let (admitted, observed) = sync_channel(0);
    let (release, released) = sync_channel(0);
    slot.active = true;
    slot.pending = Some(Gate {
        admitted,
        release: released,
    });
    Ok(WritePause {
        core: Arc::downgrade(core),
        admitted: observed,
        release,
    })
}

pub(in crate::native_mount) fn pause_next_write_after_admission(
    session: &NativeMountSession,
) -> Result<WritePause, NativeMountError> {
    let driver = session
        .driver
        .as_ref()
        .ok_or(NativeMountError::ControlPending)?;
    enroll(&driver.core)
}

pub(in crate::native_mount::fuse) fn pause(core: &ProjectionCore) -> Result<(), NativeMountError> {
    let gate = core
        .write_pause
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .pending
        .take();
    if let Some(gate) = gate {
        gate.admitted
            .send(())
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
        gate.release
            .recv()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
    }
    Ok(())
}

#[test]
fn write_pauses_follow_their_projection_and_end_on_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    use super::*;
    let mut projections = Vec::new();
    for _ in 0..2 {
        let (source, _) = shared_sources(posix_config())?;
        let work = fuse_fixture_work();
        source.create_file(&name("file"), FileMetadata::default(), None, &work)?;
        let projection = super::super::FuseProjection {
            core: projection_core(
                Arc::new(source),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )?,
        };
        let inode = projection
            .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("file"), &work)?
            .attr
            .ino
            .0;
        let (handle, _) = projection.open_node(inode, libc::O_RDWR, &work)?;
        projections.push((projection, inode, handle));
    }
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        // Both original guards are alive before either actual write starts.
        // A process-wide pending slot cannot retain these two enrollments.
        let pauses = projections
            .iter()
            .map(|(projection, _, _)| enroll(&projection.core))
            .collect::<Result<Vec<_>, _>>()?;
        let writes = projections
            .iter()
            .map(|(projection, inode, handle)| {
                scope.spawn(move || {
                    projection.write_handle(*inode, *handle, 0, b"new", &fuse_fixture_work())
                })
            })
            .collect::<Vec<_>>();
        for (pause, (projection, _, _)) in pauses.iter().zip(&projections) {
            pause.admitted.recv()?;
            // Taking a callback cannot make another guard overwrite its lease.
            assert!(matches!(
                enroll(&projection.core),
                Err(NativeMountError::ControlPending)
            ));
            pause.release.send(())?;
        }
        for write in writes {
            assert_eq!(write.join().map_err(|_| "writer panicked")??, 3);
        }
        Ok(())
    })?;
    let (projection, inode, handle) = projections.pop().ok_or("second projection")?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let pause = enroll(&projection.core)?;
        let write = scope
            .spawn(|| projection.write_handle(inode, handle, 0, b"lost", &fuse_fixture_work()));
        pause.admitted.recv()?;
        drop(pause);
        assert!(
            matches!(write.join().map_err(|_| "writer panicked")?, Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
        );
        Ok(())
    })?;
    assert_eq!(
        projection
            .core
            .source
            .read_range(&name("file"), 0, 3)?
            .as_ref(),
        b"new"
    );
    let pause = enroll(&projection.core)?;
    drop(projection);
    assert!(
        pause.admitted.recv().is_err(),
        "projection loss must close admission"
    );
    Ok(())
}
