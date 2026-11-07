//! Original backing and work for process-wide native service indexes.
use acyclic_collections::IndexWork;
use std::convert::Infallible;

pub(crate) fn completed<T>(result: Result<T, Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(impossible) => match impossible {},
    }
}

#[derive(Default)]
pub(crate) struct Counts {
    #[cfg(test)]
    examined: std::cell::Cell<u64>,
    #[cfg(test)]
    allocations: std::cell::Cell<u64>,
}

impl Counts {
    pub(crate) fn observe(&self, items: u64) {
        #[cfg(test)]
        self.examined.set(self.examined.get() + items);
        #[cfg(not(test))]
        let _ = items;
    }

    #[cfg(test)]
    pub(crate) fn examined(&self) -> u64 {
        self.examined.get()
    }

    #[cfg(test)]
    pub(crate) fn allocations(&self) -> u64 {
        self.allocations.get()
    }
}

// Like native queue and registration backing, these nodes belong to the host
// service. There is no new guest admission during original cleanup.
pub(crate) struct Work<'a>(pub(crate) &'a Counts);

impl IndexWork for Work<'_> {
    type Error = Infallible;
    type Allocation = ();

    fn check(&mut self) -> Result<(), Infallible> {
        let _ = self.0;
        Ok(())
    }

    fn charge_items(&mut self, items: u64) -> Result<(), Infallible> {
        self.0.observe(items);
        Ok(())
    }

    fn claim(&mut self, _: u64) -> Result<(), Infallible> {
        #[cfg(test)]
        self.0.allocations.set(self.0.allocations.get() + 1);
        Ok(())
    }
}
