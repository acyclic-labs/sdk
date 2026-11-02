//! Admission of one visible query over a caller-owned pinned future slot.
use crate::QueryPage;
use std::pin::Pin;

/// Shared query selection and refresh state. The caller pins its original
/// future in place; the controller adds no allocation, task or source reader.
pub struct QueryController<S> {
    source: Option<S>,
    /// The one admitted page, retained while a refresh is pending.
    pub page: Option<QueryPage<S>>,
    dirty: bool,
}

impl<S> Default for QueryController<S> {
    fn default() -> Self {
        Self {
            source: None,
            page: None,
            dirty: false,
        }
    }
}

impl<S> QueryController<S> {
    /// Current selection, including its original opaque continuation.
    pub fn source(&self) -> Option<&S> {
        self.source.as_ref()
    }

    /// Replace the selection, cancelling only the old pending consumer.
    pub fn select<F>(&mut self, source: S, pending: Pin<&mut Option<F>>) {
        self.close(pending);
        self.source = Some(source);
    }

    /// Close the page and drop its pending consumer immediately.
    pub fn close<F>(&mut self, mut pending: Pin<&mut Option<F>>) {
        pending.set(None);
        self.source = None;
        self.page = None;
        self.dirty = false;
    }

    /// Coalesce source events without replacing a pending read. The predicate
    /// excludes frozen selections, such as an older history continuation.
    pub fn invalidate(&mut self, live: impl FnOnce(&S) -> bool) {
        self.dirty |= self.source.as_ref().is_some_and(live);
    }

    /// Start only an unfilled or invalidated visible selection. Resize alone
    /// cannot restart a read or discard an admitted page. Zero credit waits
    /// for the caller's existing resize/input event.
    pub fn start<F>(
        &mut self,
        rows: u32,
        mut pending: Pin<&mut Option<F>>,
        read: impl FnOnce(&S, u32) -> F,
    ) {
        if let Some(source) = &self.source
            && pending.as_ref().get_ref().is_none()
            && (self.page.is_none() || self.dirty)
            && rows != 0
        {
            self.dirty = false;
            pending.set(Some(read(source, rows)));
        }
    }

    /// Settle the original read. A source error closes the selection; success
    /// preserves any invalidation received during that read for one refresh.
    ///
    /// # Errors
    /// Returns the original source error unchanged, without retrying it.
    pub fn finish<F, E>(
        &mut self,
        result: Result<QueryPage<S>, E>,
        mut pending: Pin<&mut Option<F>>,
    ) -> Result<(), E> {
        pending.set(None);
        match result {
            Ok(page) => self.page = Some(page),
            Err(error) => {
                self.close(pending);
                return Err(error);
            }
        }
        Ok(())
    }
}
