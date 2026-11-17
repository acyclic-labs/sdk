//! Custody that moves from the invocation that prepared a payload to the
//! store that keeps it.
//!
//! An invocation funds what it allocates while it prepares a write. Once the
//! write lands, the store keeps that payload for as long as it retains it, so
//! the charge belongs to the store and not to whichever request happened to
//! write it. Every guard allocated through one [`NativeCustody`] follows that
//! custody, so the move is one constant-time step however many guards and
//! structures the write produced.
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::*;

/// A store's own retained-byte ledger. `None` is truly unbounded.
#[derive(Debug)]
pub struct NativeCapacity {
    maximum: Option<u64>,
    held: AtomicU64,
}

impl NativeCapacity {
    /// A ledger that refuses retention past `maximum` bytes.
    #[must_use]
    pub fn new(maximum: Option<u64>) -> Self {
        Self {
            maximum,
            held: AtomicU64::new(0),
        }
    }

    /// Bytes the store currently keeps or has reserved to keep.
    pub fn held(&self) -> u64 {
        self.held.load(Ordering::Acquire)
    }

    fn charge(&self, bytes: u64) -> io::Result<()> {
        let maximum = self.maximum.unwrap_or(u64::MAX);
        self.held
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                held.checked_add(bytes).filter(|next| *next <= maximum)
            })
            .map(|_| ())
            .map_err(|_| io::ErrorKind::StorageFull.into())
    }

    fn release(&self, bytes: u64) {
        let previous = self.held.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "a store releases only what it holds");
    }
}

enum Holder {
    Invocation {
        meter: Arc<dyn NativeReadMeter>,
        owner: Arc<dyn NativeReadMeter>,
    },
    Store(Arc<NativeCapacity>),
}

struct Custody {
    holder: Holder,
    /// Bytes currently owned through this custody, including its own header.
    outstanding: u64,
    /// Invocation credit withheld but not yet recorded. A transfer waits for
    /// none, so a reservation always settles on the ledger that withheld it.
    withheld: u64,
    /// A transfer is reserved: the outstanding total is fixed until it
    /// completes or is abandoned.
    sealed: bool,
}

/// One write's payload custody: an allocation meter that charges the
/// preparing invocation until [`CustodyTransfer::complete`] hands every
/// outstanding byte to a [`NativeCapacity`].
pub struct NativeCustody {
    state: Mutex<Custody>,
}

impl NativeCustody {
    /// Opens custody under `meter`, which funds this custody's own header.
    pub fn new(meter: &Arc<dyn NativeReadMeter>) -> io::Result<Arc<Self>> {
        let header = arc_allocation_bytes::<Self>() as u64;
        meter.allocate(header)?;
        let owner = meter
            .allocation_owner()
            .unwrap_or_else(|| Arc::clone(meter));
        Ok(Arc::new(Self {
            state: Mutex::new(Custody {
                holder: Holder::Invocation {
                    meter: Arc::clone(meter),
                    owner,
                },
                outstanding: header,
                withheld: 0,
                sealed: false,
            }),
        }))
    }

    fn lock(&self) -> InlineMutexGuard<'_, Custody> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reserves `capacity` for everything this custody owns now. Nothing more
    /// can be charged here until the transfer completes or is dropped, so
    /// completion needs no further admission and cannot fail.
    pub fn reserve(
        self: &Arc<Self>,
        capacity: &Arc<NativeCapacity>,
    ) -> io::Result<CustodyTransfer> {
        let mut custody = self.lock();
        if custody.sealed || custody.withheld != 0 || matches!(custody.holder, Holder::Store(_)) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        capacity.charge(custody.outstanding)?;
        custody.sealed = true;
        Ok(CustodyTransfer {
            custody: Arc::clone(self),
            capacity: Arc::clone(capacity),
            reserved: custody.outstanding,
        })
    }

    // Applies `charge` to the current holder, refusing a sealed custody.
    fn charge(
        &self,
        bytes: u64,
        charge: impl FnOnce(&dyn NativeReadMeter) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut custody = self.lock();
        if custody.sealed {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        match &custody.holder {
            Holder::Invocation { meter, .. } => charge(meter.as_ref())?,
            Holder::Store(capacity) => capacity.charge(bytes)?,
        }
        custody.outstanding = custody
            .outstanding
            .checked_add(bytes)
            .ok_or(io::ErrorKind::InvalidInput)?;
        Ok(())
    }

    // The invocation view for work that is not retained payload.
    fn invocation(&self) -> io::Result<Arc<dyn NativeReadMeter>> {
        match &self.lock().holder {
            Holder::Invocation { meter, .. } => Ok(Arc::clone(meter)),
            Holder::Store(_) => Err(io::ErrorKind::Unsupported.into()),
        }
    }

    /// Returns a guard this custody owns to the invocation that funded it,
    /// before a transfer, so payload the caller keeps stays on its ledger.
    pub(crate) fn release_to_invocation(&self, bytes: u64) -> io::Result<Arc<dyn NativeReadMeter>> {
        let mut custody = self.lock();
        let Holder::Invocation { owner, .. } = &custody.holder else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        let owner = Arc::clone(owner);
        custody.outstanding = custody
            .outstanding
            .checked_sub(bytes)
            .ok_or(io::ErrorKind::InvalidInput)?;
        Ok(owner)
    }
}

impl Drop for NativeCustody {
    fn drop(&mut self) {
        let custody = self.lock();
        // Every guard holds this custody, so only its own header remains.
        let header = custody.outstanding;
        match &custody.holder {
            Holder::Invocation { owner, .. } => owner.release(header),
            Holder::Store(capacity) => capacity.release(header),
        }
    }
}

/// Capacity reserved for one custody's payload. Completing it moves the
/// payload's charge from the invocation to the store, with no allocation and
/// no failure. Dropping it returns the reservation and reopens the custody.
#[must_use]
pub struct CustodyTransfer {
    custody: Arc<NativeCustody>,
    capacity: Arc<NativeCapacity>,
    reserved: u64,
}

impl CustodyTransfer {
    /// Hands every outstanding byte to the store and returns it to the invocation.
    pub fn complete(mut self) {
        let mut custody = self.custody.lock();
        // Sealed: guards could only have been released since the reservation.
        let kept = custody.outstanding;
        self.capacity.release(self.reserved - kept);
        let previous = std::mem::replace(
            &mut custody.holder,
            Holder::Store(Arc::clone(&self.capacity)),
        );
        if let Holder::Invocation { owner, .. } = previous {
            owner.release(kept);
        }
        custody.sealed = false;
        self.reserved = 0;
    }
}

impl Drop for CustodyTransfer {
    fn drop(&mut self) {
        let mut custody = self.custody.lock();
        if custody.sealed {
            custody.sealed = false;
            self.capacity.release(self.reserved);
        }
    }
}

impl NativeReadMeter for NativeCustody {
    fn check(&self) -> io::Result<()> {
        match &self.lock().holder {
            Holder::Invocation { meter, .. } => meter.check(),
            Holder::Store(_) => Ok(()),
        }
    }
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
        self.invocation()?.reserve_operations(kind, count)
    }
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
        if let Ok(meter) = self.invocation() {
            meter.cancel_reserved_operations(kind, count);
        }
    }
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
        self.invocation()?.record_reserved_operation(kind)
    }
    fn admit_write_bytes(&self, maximum: usize) -> io::Result<()> {
        self.invocation()?.admit_write_bytes(maximum)
    }
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
        self.invocation()?.reserve_bytes(kind, maximum)
    }
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64) {
        if let Ok(meter) = self.invocation() {
            meter.cancel_reserved_bytes(kind, maximum);
        }
    }
    fn record_reserved_bytes(
        &self,
        kind: NativeOperationKind,
        maximum: u64,
        actual: u64,
    ) -> io::Result<()> {
        self.invocation()?
            .record_reserved_bytes(kind, maximum, actual)
    }
    fn allocate(&self, bytes: u64) -> io::Result<()> {
        self.charge(bytes, |meter| meter.allocate(bytes))
    }
    fn reserve_allocation(&self, bytes: u64) -> io::Result<()> {
        let mut custody = self.lock();
        if custody.sealed {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        match &custody.holder {
            Holder::Invocation { meter, .. } => meter.reserve_allocation(bytes)?,
            Holder::Store(capacity) => capacity.charge(bytes)?,
        }
        custody.withheld += bytes;
        Ok(())
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        let mut custody = self.lock();
        custody.withheld -= bytes;
        match &custody.holder {
            Holder::Invocation { meter, .. } => meter.cancel_reserved_allocation(bytes),
            Holder::Store(capacity) => capacity.release(bytes),
        }
    }
    fn record_reserved_allocation(&self, bytes: u64) -> io::Result<()> {
        let mut custody = self.lock();
        let result = match &custody.holder {
            Holder::Invocation { meter, .. } => meter.record_reserved_allocation(bytes),
            Holder::Store(_) => Ok(()),
        };
        if result.is_ok() {
            custody.withheld -= bytes;
            custody.outstanding += bytes;
        }
        result
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
        match &self.lock().holder {
            Holder::Invocation { meter, .. } => meter.allocation_failed(error),
            Holder::Store(_) => io::Error::new(io::ErrorKind::OutOfMemory, error),
        }
    }
    fn allocate_refcount_header(&self) -> io::Result<()> {
        self.charge(0, |meter| meter.allocate_refcount_header())
    }
    fn adopt(&self, bytes: u64) -> io::Result<()> {
        self.charge(bytes, |meter| meter.adopt(bytes))
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
        let mut custody = self.lock();
        if custody.sealed {
            return (0, Err(io::ErrorKind::InvalidInput.into()));
        }
        let (retained, result) = match &custody.holder {
            Holder::Invocation { meter, .. } => meter.record_completed_adoption(bytes),
            Holder::Store(capacity) => match capacity.charge(bytes) {
                Ok(()) => (bytes, Ok(())),
                Err(error) => (0, Err(error)),
            },
        };
        custody.outstanding += retained;
        (retained, result)
    }
    fn examine(&self, items: u64, bytes_copied: u64) -> io::Result<()> {
        self.invocation()?.examine(items, bytes_copied)
    }
    fn admit_examination(&self, items: u64, bytes_copied: u64) -> io::Result<()> {
        self.invocation()?.admit_examination(items, bytes_copied)
    }
    fn record_examination(&self, items: u64, bytes_copied: u64) -> io::Result<()> {
        self.invocation()?.record_examination(items, bytes_copied)
    }
    fn release(&self, bytes: u64) {
        let mut custody = self.lock();
        custody.outstanding -= bytes;
        match &custody.holder {
            Holder::Invocation { owner, .. } => owner.release(bytes),
            Holder::Store(capacity) => capacity.release(bytes),
        }
    }
    fn admit_bytes(&self, maximum: usize) -> io::Result<()> {
        self.invocation()?.admit_bytes(maximum)
    }
    fn admit_read(&self, maximum: usize) -> io::Result<()> {
        self.invocation()?.admit_read(maximum)
    }
    fn record_read(&self, bytes: usize) -> io::Result<()> {
        self.invocation()?.record_read(bytes)
    }
    fn admit_write(&self, maximum: usize) -> io::Result<()> {
        self.invocation()?.admit_write(maximum)
    }
    fn record_write(&self, bytes: usize) -> io::Result<()> {
        self.invocation()?.record_write(bytes)
    }
    fn admit_durability(&self) -> io::Result<()> {
        self.invocation()?.admit_durability()
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> io::Result<()> {
        self.invocation()?.admit_codec(encoded, copied, hashed)
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> io::Result<()> {
        self.invocation()?.record_codec(encoded, copied, hashed)
    }
    fn reserve_codec(&self, maximum: NativeCodecWork) -> io::Result<()> {
        self.invocation()?.reserve_codec(maximum)
    }
    fn cancel_reserved_codec(&self, unused: NativeCodecWork) {
        if let Ok(meter) = self.invocation() {
            meter.cancel_reserved_codec(unused);
        }
    }
    fn record_reserved_codec(&self, actual: NativeCodecWork) -> io::Result<()> {
        self.invocation()?.record_reserved_codec(actual)
    }
    fn reserve_examination(&self, items: u64) -> io::Result<()> {
        self.invocation()?.reserve_examination(items)
    }
    fn cancel_reserved_examination(&self, items: u64) {
        if let Ok(meter) = self.invocation() {
            meter.cancel_reserved_examination(items);
        }
    }
    fn record_reserved_examination(&self, items: u64) -> io::Result<()> {
        self.invocation()?.record_reserved_examination(items)
    }
    fn finish_read(&self, requested: usize, completed: usize) -> io::Result<()> {
        self.invocation()?.finish_read(requested, completed)
    }
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        self.invocation()?.reserve_cleanup_write()
    }
    fn cancel_cleanup_write(&self, reservation: u64) {
        if let Ok(meter) = self.invocation() {
            meter.cancel_cleanup_write(reservation);
        }
    }
    fn record_cleanup_write(&self, reservation: u64) -> io::Result<()> {
        self.invocation()?.record_cleanup_write(reservation)
    }
}
