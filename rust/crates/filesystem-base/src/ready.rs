// A ready list whose links live in each view's birth record.
//
// Scheduling and cancelling a view touch only that record and its immediate
// neighbours. Removing a view releases its record immediately, even while a
// service pass is running; there are no retired serials to scan or retain.

use std::collections::HashMap;

/// A ready link must name a live, queued birth record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyLinkError {
    /// A queue link names a record which is not queued.
    Unqueued(u64),
    /// A serial already owns its birth record.
    AlreadyInserted(u64),
}

impl std::fmt::Display for ReadyLinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unqueued(serial) => write!(f, "ready link {serial} has no queued birth record"),
            Self::AlreadyInserted(serial) => {
                write!(f, "ready view {serial} already owns its birth record")
            }
        }
    }
}

impl std::error::Error for ReadyLinkError {}

/// Serial-keyed ready work with linked scheduling and immediate retirement.
pub struct ReadyViews<V> {
    views: HashMap<u64, View<V>>,
    first: Option<u64>,
    last: Option<u64>,
}

struct View<V> {
    value: V,
    ready: Option<Links>,
}

struct Links {
    previous: Option<u64>,
    next: Option<u64>,
}

impl<V> Default for ReadyViews<V> {
    fn default() -> Self {
        Self {
            views: HashMap::new(),
            first: None,
            last: None,
        }
    }
}

impl<V> ReadyViews<V> {
    /// Updates work in its existing birth record without admitting a new one.
    pub fn get_mut(&mut self, serial: u64) -> Option<&mut V> {
        self.views.get_mut(&serial).map(|view| &mut view.value)
    }

    /// Reserves the view's only queue record before publishing its wake hook.
    ///
    /// # Errors
    /// Refuses an already admitted serial without replacing its original record.
    pub fn insert(&mut self, serial: u64, value: V) -> Result<(), ReadyLinkError> {
        match self.views.entry(serial) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(View { value, ready: None });
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(ReadyLinkError::AlreadyInserted(serial))
            }
        }
    }

    /// Returns whether this call queued the live view. Repeated or late wakes
    /// cannot duplicate a record or recreate a retired view.
    ///
    /// # Errors
    /// Reports a queue link that no longer names a queued birth record.
    pub fn schedule(&mut self, serial: u64) -> Result<bool, ReadyLinkError> {
        let Some(view) = self.views.get_mut(&serial) else {
            return Ok(false);
        };
        if view.ready.is_some() {
            return Ok(false);
        }
        if let Some(last) = self.last {
            self.links_mut(last)?.next = Some(serial);
        } else {
            self.first = Some(serial);
        }
        let view = self
            .views
            .get_mut(&serial)
            .ok_or(ReadyLinkError::Unqueued(serial))?;
        view.ready = Some(Links {
            previous: self.last,
            next: None,
        });
        self.last = Some(serial);
        Ok(true)
    }

    /// Takes the next pass while retaining the birth record. A wake during
    /// that pass can schedule the same view again.
    ///
    /// # Errors
    /// Reports a queue link that no longer names a queued birth record.
    pub fn pop(&mut self) -> Result<Option<(u64, &V)>, ReadyLinkError> {
        let Some(serial) = self.first else {
            return Ok(None);
        };
        self.links_mut(serial)?;
        self.unlink(serial)?;
        let view = self
            .views
            .get(&serial)
            .ok_or(ReadyLinkError::Unqueued(serial))?;
        Ok(Some((serial, &view.value)))
    }

    /// Cancels pending work and removes the original record in constant
    /// bookkeeping, independently of the number of other views or wakes.
    ///
    /// # Errors
    /// Reports a queue link that no longer names a queued birth record.
    pub fn remove(&mut self, serial: u64) -> Result<Option<V>, ReadyLinkError> {
        self.unlink(serial)?;
        Ok(self.views.remove(&serial).map(|view| view.value))
    }

    fn links_mut(&mut self, serial: u64) -> Result<&mut Links, ReadyLinkError> {
        self.views
            .get_mut(&serial)
            .and_then(|view| view.ready.as_mut())
            .ok_or(ReadyLinkError::Unqueued(serial))
    }

    fn unlink(&mut self, serial: u64) -> Result<(), ReadyLinkError> {
        let Some(links) = self
            .views
            .get_mut(&serial)
            .and_then(|view| view.ready.take())
        else {
            return Ok(());
        };
        if let Some(previous) = links.previous {
            self.links_mut(previous)?.next = links.next;
        } else {
            self.first = links.next;
        }
        if let Some(next) = links.next {
            self.links_mut(next)?.previous = links.previous;
        } else {
            self.last = links.previous;
        }
        Ok(())
    }
}
