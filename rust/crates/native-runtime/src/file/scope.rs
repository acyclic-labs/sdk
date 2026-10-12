use crate::*;

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
thread_local! {
    static ACTIVE_FILES: Cell<*const ActiveFileScopeNode> = const { Cell::new(std::ptr::null()) };
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) struct ActiveFileScopeNode {
    pub(crate) identity: usize,
    pub(crate) previous: *const Self,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) struct FileScopeRestore(*const ActiveFileScopeNode);

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl Drop for FileScopeRestore {
    fn drop(&mut self) {
        ACTIVE_FILES.with(|active| active.set(self.0));
    }
}

pub(crate) fn with_file_scope<T>(
    health: Option<&Arc<FileHealth>>,
    operation: impl FnOnce() -> T,
) -> T {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        let Some(health) = health else {
            return operation();
        };
        let previous = ACTIVE_FILES.with(Cell::get);
        let node = std::pin::pin!(ActiveFileScopeNode {
            identity: Arc::as_ptr(health) as usize,
            previous,
        });
        let _restore = FileScopeRestore(previous);
        ACTIVE_FILES.with(|active| active.set(node.as_ref().get_ref()));
        operation()
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    {
        let _ = health;
        operation()
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[allow(unsafe_code)]
pub(crate) fn in_file_scope(health: &Arc<FileHealth>) -> bool {
    let identity = Arc::as_ptr(health) as usize;
    let mut node = ACTIVE_FILES.with(Cell::get);
    while !node.is_null() {
        // SAFETY: `with_file_scope` publishes live, pinned frames on this thread and restores TLS before they drop.
        let active = unsafe { &*node };
        if active.identity == identity {
            return true;
        }
        node = active.previous;
    }
    false
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn poison_borrowed_file_health(file: &File, health: &Arc<FileHealth>) {
    let anchor = Arc::new(NativeFilePayload {
        file: file.try_clone().unwrap_or_else(|_| std::process::abort()),
        _allocation: None,
    });
    poison_file_health(&anchor, health);
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
pub(crate) fn with_file_admission<T>(
    _: &File,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    operation()
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn with_file_admission<T>(
    file: &File,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let health = shared_file_health(file, None)?;
    let run = || {
        if health.uncertain.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "prior native I/O completion on this file is uncertain",
            ));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
            .unwrap_or_else(|_| std::process::abort());
        if result.as_ref().err().is_some_and(is_uncertain_io_error) {
            poison_borrowed_file_health(file, &health);
        }
        result
    };
    if in_file_scope(&health) {
        return run();
    }
    let (predecessor, completion) = {
        let mut last = health
            .tail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let predecessor = last.clone();
        let completion = Arc::new(OperationFence {
            health: Some(Arc::downgrade(&health)),
            ..OperationFence::default()
        });
        *last = Some(Arc::clone(&completion));
        (predecessor, completion)
    };
    if let Some(predecessor) = predecessor {
        predecessor.wait_until_complete();
    }
    let result = with_file_scope(Some(&health), run);
    completion.complete();
    result
}
