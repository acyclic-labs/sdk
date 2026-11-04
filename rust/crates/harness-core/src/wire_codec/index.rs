//! Temporary index nodes borrow the original observer through their retirement.
use super::*;
use acyclic_collections::IndexWork;
use std::cell::RefCell;

pub(crate) struct BorrowedIndexWork<'a, 'b> {
    original: RefCell<&'a mut Option<&'b mut dyn HistoryIo>>,
}

impl<'a, 'b> BorrowedIndexWork<'a, 'b> {
    pub(crate) fn new(original: &'a mut Option<&'b mut dyn HistoryIo>) -> Self {
        Self {
            original: RefCell::new(original),
        }
    }

    pub(crate) fn with<T>(
        &self,
        run: impl FnOnce(&mut Option<&'b mut dyn HistoryIo>) -> Result<T>,
    ) -> Result<T> {
        run(&mut **self.original.borrow_mut())
    }
}

// A node cannot outlive this borrow, so an observer that declines ownership
// transfer can still fund shared nodes without a copied allowance or ledger.
pub(crate) struct BorrowedIndexAllocation<'a, 'b, 'c> {
    original: &'a BorrowedIndexWork<'b, 'c>,
    bytes: u64,
}

impl Drop for BorrowedIndexAllocation<'_, '_, '_> {
    fn drop(&mut self) {
        let _ = self.original.with(|original| {
            if let Some(io) = original.as_deref_mut() {
                io.release_owned(self.bytes)?;
            }
            Ok(())
        });
    }
}

impl<'a, 'b, 'c> IndexWork for &'a BorrowedIndexWork<'b, 'c> {
    type Error = Error;
    type Allocation = BorrowedIndexAllocation<'a, 'b, 'c>;

    fn check(&mut self) -> Result<()> {
        self.with(|original| {
            if let Some(io) = original.as_deref_mut() {
                io.check_cancellation()?;
            }
            Ok(())
        })
    }

    fn charge_items(&mut self, items: u64) -> Result<()> {
        self.with(|original| {
            if let Some(io) = original.as_deref_mut() {
                io.check_cancellation()?;
                io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items })?;
            }
            Ok(())
        })
    }

    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation> {
        self.with(|original| {
            let mut quota = CodecQuota::new(original, bytes)?;
            if let Some(io) = quota.work().as_deref_mut() {
                io.allocation_attempt(bytes)?;
            }
            quota.transfer();
            Ok(())
        })?;
        Ok(BorrowedIndexAllocation {
            original: *self,
            bytes,
        })
    }
}
