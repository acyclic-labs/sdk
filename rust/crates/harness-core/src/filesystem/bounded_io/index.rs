//! Index operations borrow original preparation work; nodes retain its custody.
use super::*;

impl acyclic_collections::IndexWork for &PreparationIo {
    type Error = Error;
    type Allocation = PreparationCustody;

    fn check(&mut self) -> Result<()> {
        self.check_cancellation()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original_native_work() {
            return original.admit(WorkCounters::UNCHARGED).map_err(Into::into);
        }
        self.preflight(WorkCounters::UNCHARGED).map_err(Into::into)
    }

    fn charge_items(&mut self, items: u64) -> Result<()> {
        self.charged(WorkCounters {
            items_examined: items,
            ..Default::default()
        })
    }

    fn claim(&mut self, bytes: u64) -> Result<PreparationCustody> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original_native_work() {
            return original
                .claim(bytes, 1, WorkCounters::UNCHARGED)
                .map(PreparationCustody::Native)
                .map_err(Into::into);
        }
        // The existing reservation retires an admitted byte claim if allocation
        // admission or custody transfer refuses before the node is constructed.
        let mut reservation = self.reserve_decode(bytes)?;
        self.allocation_attempt(bytes)?;
        reservation.take_custody()
    }
}
