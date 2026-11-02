use crate::*;

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
thread_local! {
    static ACTIVE_FILES: Cell<*const ActiveFileScopeNode> = const { Cell::new(std::ptr::null()) };
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct ActiveFileScopeNode {
    health: *const Arc<FileHealth>,
    file: *const File,
    previous: *const Self,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct FileScopeRestore(*const ActiveFileScopeNode);

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
    with_scope(health, None, operation)
}

/// Borrows the exact file and its admitted health until the callback returns.
pub(super) fn with_owned_file_scope<T>(
    file: &File,
    health: &Arc<FileHealth>,
    operation: impl FnOnce() -> T,
) -> T {
    with_scope(Some(health), Some(file), operation)
}

fn with_scope<T>(
    health: Option<&Arc<FileHealth>>,
    file: Option<&File>,
    operation: impl FnOnce() -> T,
) -> T {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        let Some(health) = health else {
            return operation();
        };
        let previous = ACTIVE_FILES.with(Cell::get);
        let node = std::pin::pin!(ActiveFileScopeNode {
            health,
            file: file.map_or(std::ptr::null(), std::ptr::from_ref),
            previous,
        });
        let _restore = FileScopeRestore(previous);
        ACTIVE_FILES.with(|active| active.set(node.as_ref().get_ref()));
        operation()
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    {
        let _ = (health, file);
        operation()
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[allow(unsafe_code, reason = "walks live pinned frames owned by this thread")]
pub(crate) fn in_file_scope(health: &Arc<FileHealth>) -> bool {
    let mut node = ACTIVE_FILES.with(Cell::get);
    while !node.is_null() {
        // SAFETY: `with_scope` retains each health borrow and pinned node on
        // this thread, restoring TLS before either can drop.
        let active = unsafe { &*node };
        if Arc::ptr_eq(unsafe { &*active.health }, health) {
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
        #[cfg(target_vendor = "apple")]
        device: Default::default(),
        #[cfg(windows)]
        completion_attached: std::sync::atomic::AtomicBool::new(false),
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
#[allow(
    unsafe_code,
    reason = "borrows only the current live pinned admission frame"
)]
pub(crate) fn with_file_admission<T>(
    file: &File,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let node = ACTIVE_FILES.with(Cell::get);
    if !node.is_null() {
        // SAFETY: `with_scope` retains both borrows for this pinned frame's
        // entire lifetime. No reference escapes this synchronous operation.
        let active = unsafe { &*node };
        if std::ptr::eq(active.file, file) {
            return run_admitted(file, unsafe { &*active.health }, operation);
        }
    }
    let health = shared_file_health(file, None)?;
    admitted(file, &health, operation)
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn admitted<T>(
    file: &File,
    health: &Arc<FileHealth>,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let run = || run_admitted(file, health, operation);
    if in_file_scope(health) {
        return run();
    }
    let completion = Arc::new(OperationFence {
        health: Some(Arc::downgrade(health)),
        ..OperationFence::default()
    });
    let predecessor = completion.enqueue(health);
    if let Some(predecessor) = predecessor {
        crate::task::persistence_predecessor(Arc::as_ptr(&predecessor) as usize, || {
            predecessor.wait_until_complete();
        });
    }
    let result = with_owned_file_scope(file, health, run);
    completion.complete();
    result
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn run_admitted<T>(
    file: &File,
    health: &Arc<FileHealth>,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    if health.uncertain.load(Ordering::Acquire) {
        return Err(io::Error::other(
            "prior native I/O completion on this file is uncertain",
        ));
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
        .unwrap_or_else(|_| std::process::abort());
    if result.as_ref().err().is_some_and(is_uncertain_io_error) {
        poison_borrowed_file_health(file, health);
    }
    result
}

#[cfg(all(test, any(windows, target_os = "linux", target_vendor = "apple")))]
mod tests;
