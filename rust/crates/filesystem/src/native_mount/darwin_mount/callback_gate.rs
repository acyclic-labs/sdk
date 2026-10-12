use super::*;

/// Native callbacks in flight, in two generations, so a barrier can wait
/// for every callback that began before it without holding up later ones.
pub(super) struct CallbackGate {
    pub(super) generation: AtomicU64,
    /// Callbacks in flight that entered in an even or odd generation.
    pub(super) even: AtomicU64,
    pub(super) odd: AtomicU64,
    /// Serializes barriers, so each drains a generation before the next
    /// one reuses its slot.
    pub(super) draining: Mutex<()>,
}

/// One callback's place in its generation, left when dropped.
pub(super) struct CallbackPass<'a>(&'a AtomicU64);

impl CallbackGate {
    pub(super) const fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            even: AtomicU64::new(0),
            odd: AtomicU64::new(0),
            draining: Mutex::new(()),
        }
    }

    pub(in crate::native_mount) fn slot(&self, generation: u64) -> &AtomicU64 {
        if generation.is_multiple_of(2) {
            &self.even
        } else {
            &self.odd
        }
    }

    pub(in crate::native_mount) fn enter(&self) -> CallbackPass<'_> {
        loop {
            let generation = self.generation.load(Ordering::SeqCst);
            let active = self.slot(generation);
            active.fetch_add(1, Ordering::SeqCst);
            // A barrier that began in between may already have found this
            // slot idle; join the generation after it instead.
            if self.generation.load(Ordering::SeqCst) == generation {
                return CallbackPass(active);
            }
            active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Returns once every callback that entered before this call has left.
    pub(in crate::native_mount) fn drain(&self) {
        let _draining = self.draining.lock().unwrap_or_else(PoisonError::into_inner);
        let active = self.slot(self.generation.fetch_add(1, Ordering::SeqCst));
        while active.load(Ordering::SeqCst) != 0 {
            std::thread::sleep(CALLBACK_DRAIN_POLL);
        }
    }
}

impl Drop for CallbackPass<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
