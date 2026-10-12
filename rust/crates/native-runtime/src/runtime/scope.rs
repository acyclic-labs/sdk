use super::*;

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
pub(crate) struct FileScopeRestore(pub(crate) *const ActiveFileScopeNode);

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl Drop for FileScopeRestore {
    fn drop(&mut self) {
        ACTIVE_FILES.with(|active| active.set(self.0));
    }
}

pub(crate) fn with_file_scope<T>(tail: Option<&FileSequencer>, operation: impl FnOnce() -> T) -> T {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        let Some(tail) = tail else {
            return operation();
        };
        let previous = ACTIVE_FILES.with(Cell::get);
        let node = std::pin::pin!(ActiveFileScopeNode { identity: Arc::as_ptr(tail) as usize, previous });
        let _restore = FileScopeRestore(previous);
        ACTIVE_FILES.with(|active| active.set(node.as_ref().get_ref()));
        operation()
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    {
        let _ = tail;
        operation()
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[allow(unsafe_code)]
pub(crate) fn in_file_scope(tail: &FileSequencer) -> bool {
    let identity = Arc::as_ptr(tail) as usize;
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
