use super::*;
use crate::sync::{Arc, Mutex, MutexGuard};

// Kernel syscall shim: O_PATH pins a mount ID even after namespace detach.
// The native destination flock is nonblocking and held through detach. Lock
// order is destination fence -> kernel namespace; no reverse acquisition.
struct Kernel {
    fence: Mutex<()>,
    namespace: Mutex<Namespace>,
}
struct Namespace {
    current: Option<u64>,
    recovering: bool,
    detached: usize,
}
struct Pinned {
    kernel: Arc<Kernel>,
    mount: u64,
    broken_fence: bool,
}
struct Fence<'a> {
    kernel: &'a Kernel,
    _guard: Option<MutexGuard<'a, ()>>,
}
impl Drop for Fence<'_> {
    fn drop(&mut self) {
        self.kernel
            .namespace
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovering = false;
    }
}
impl Destination for Pinned {
    type Fence<'a> = Fence<'a>;

    fn disconnected(&self) -> Result<bool, NativeMountError> {
        // Mount zero is the killed provider; remount creates a distinct ID.
        Ok(self.mount == 0)
    }

    fn acquire(&self) -> Result<Self::Fence<'_>, NativeMountError> {
        let guard = if self.broken_fence {
            None
        } else {
            Some(
                self.kernel
                    .fence
                    .try_lock()
                    .map_err(|_| NativeMountError::DestinationBusy)?,
            )
        };
        let mut namespace = self.kernel.namespace.lock().unwrap();
        assert!(!namespace.recovering, "overlapping recovery owners");
        namespace.recovering = true;
        drop(namespace);
        Ok(Fence {
            kernel: &self.kernel,
            _guard: guard,
        })
    }

    fn still_current(&self) -> Result<bool, NativeMountError> {
        Ok(self.kernel.namespace.lock().unwrap().current == Some(self.mount))
    }

    fn detach(&self, _fence: Self::Fence<'_>) -> Result<(), NativeMountError> {
        let mut namespace = self.kernel.namespace.lock().unwrap();
        assert!(namespace.recovering);
        // This is an fd-bound unmount, never an unmount of the current path.
        if namespace.current == Some(self.mount) {
            assert_eq!(self.mount, 0, "detached a live remount");
            namespace.current = None;
            namespace.detached += 1;
        }
        Ok(())
    }
}

fn scenario(broken_fence: bool) {
    let kernel = Arc::new(Kernel {
        fence: Mutex::new(()),
        namespace: Mutex::new(Namespace {
            current: Some(0),
            recovering: false,
            detached: 0,
        }),
    });
    // Both reopeners hold the old O_PATH identity. Either can recover and
    // remount before the other obtains the destination fence.
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let kernel = kernel.clone();
            loom::thread::spawn(move || {
                let pinned = Pinned {
                    kernel: kernel.clone(),
                    mount: 0,
                    broken_fence,
                };
                match recover(&pinned) {
                    Ok(_) | Err(NativeMountError::DestinationBusy) => {}
                    Err(error) => panic!("{error}"),
                }
                // Normal mount acquisition uses the same kernel fence.
                if let Ok(_fence) = kernel.fence.try_lock() {
                    let mut namespace = kernel.namespace.lock().unwrap();
                    assert!(!namespace.recovering, "remount overlapped recovery");
                    if namespace.current.is_none() {
                        namespace.current = Some(1);
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let namespace = kernel.namespace.lock().unwrap();
    assert_eq!(namespace.current, Some(1));
    assert_eq!(namespace.detached, 1);
    assert!(!namespace.recovering);
}

#[test]
fn competing_reopeners_preserve_the_remount_with_broken_fence_control() {
    loom::model(|| scenario(false));
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true)))
        .expect_err("broken destination fence was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("overlapping recovery owners")
            || message.contains("remount overlapped recovery"),
        "{message}"
    );
}
