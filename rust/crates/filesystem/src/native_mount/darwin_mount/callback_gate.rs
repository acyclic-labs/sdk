use super::*;

/// Native callbacks in flight, in two generations, so a barrier can wait for every callback that began before it without holding up later ones.
pub(super) struct CallbackGate {
    pub(super) generation: AtomicU64,
    /// Callbacks in flight that entered in an even or odd generation.
    pub(super) even: AtomicU64,
    pub(super) odd: AtomicU64,
    /// Serializes barriers, so each drains a generation before the next one reuses its slot.
    pub(super) draining: Mutex<Option<u64>>,
}

/// One callback's place in its generation, left when dropped.
pub(super) struct CallbackPass<'a>(&'a AtomicU64);

impl CallbackGate {
    pub(super) const fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            even: AtomicU64::new(0),
            odd: AtomicU64::new(0),
            draining: Mutex::new(None),
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
            if self.generation.load(Ordering::SeqCst) == generation {
                return CallbackPass(active);
            }
            active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Returns once every callback that entered before this call has left.
    pub(super) fn drain(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let mut pending = self.draining.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(generation) = *pending {
            self.wait_generation(generation, work)?;
            *pending = None;
        }
        let generation = self.generation.fetch_add(1, Ordering::SeqCst);
        *pending = Some(generation);
        self.wait_generation(generation, work)?;
        *pending = None;
        Ok(())
    }

    fn wait_generation(
        &self,
        generation: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let active = self.slot(generation);
        loop {
            work.charge(scan_work(1))?;
            if active.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }
            work.charge(READ_WORK)?;
            std::thread::sleep(CALLBACK_DRAIN_POLL);
        }
    }
}

impl Drop for CallbackPass<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
