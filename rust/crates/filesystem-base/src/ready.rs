// A ready list whose links live in each view's birth record.
//
// Scheduling and cancelling a view touch only that record and its immediate
// neighbours. Removing a view releases its record immediately, even while a
// service pass is running; there are no retired serials to scan or retain.

use std::collections::HashMap;

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
    pub fn insert(&mut self, serial: u64, value: V) {
        let previous = self.views.insert(serial, View { value, ready: None });
        assert!(previous.is_none(), "a view reserves its ready record once");
    }

    /// Returns whether this call queued the live view. Repeated or late wakes
    /// cannot duplicate a record or recreate a retired view.
    pub fn schedule(&mut self, serial: u64) -> bool {
        let Some(view) = self.views.get_mut(&serial) else {
            return false;
        };
        if view.ready.is_some() {
            return false;
        }
        view.ready = Some(Links {
            previous: self.last,
            next: None,
        });
        if let Some(last) = self.last {
            self.views
                .get_mut(&last)
                .and_then(|view| view.ready.as_mut())
                .expect("the last ready view owns its links")
                .next = Some(serial);
        } else {
            self.first = Some(serial);
        }
        self.last = Some(serial);
        true
    }

    /// Takes the next pass while retaining the birth record. A wake during
    /// that pass can schedule the same view again.
    pub fn pop(&mut self) -> Option<(u64, &V)> {
        let serial = self.first?;
        self.unlink(serial);
        Some((
            serial,
            &self
                .views
                .get(&serial)
                .expect("a ready view owns its record")
                .value,
        ))
    }

    /// Cancels pending work and removes the original record in constant
    /// bookkeeping, independently of the number of other views or wakes.
    pub fn remove(&mut self, serial: u64) -> Option<V> {
        self.unlink(serial);
        self.views.remove(&serial).map(|view| view.value)
    }

    fn unlink(&mut self, serial: u64) {
        let Some(links) = self
            .views
            .get_mut(&serial)
            .and_then(|view| view.ready.take())
        else {
            return;
        };
        if let Some(previous) = links.previous {
            self.views
                .get_mut(&previous)
                .and_then(|view| view.ready.as_mut())
                .expect("the preceding ready view owns its links")
                .next = links.next;
        } else {
            self.first = links.next;
        }
        if let Some(next) = links.next {
            self.views
                .get_mut(&next)
                .and_then(|view| view.ready.as_mut())
                .expect("the following ready view owns its links")
                .previous = links.previous;
        } else {
            self.last = links.previous;
        }
    }
}
