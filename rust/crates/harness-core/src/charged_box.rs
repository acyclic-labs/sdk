//! Heap moves paid for on the allocation budget that is live where they happen.
//!
//! Boxing a value or a future keeps it out of a stack frame, but the box is a
//! real allocation. Where an allocation budget is live, the box's bytes are
//! owned custody and its allocation is one attempt on that budget, both
//! charged before the box exists. The charge is returned only after the box
//! is freed, so the budget's peak includes every live box.
//!
//! A box made where no budget is live says so: [`ChargedBox::unbudgeted`].

/// What one box holds on its budget while it is live: nothing where no
/// budget is live, else the custody that returns its bytes when dropped.
#[must_use = "a box charge returns its bytes when dropped"]
pub struct BoxCharge {
    _held: Charge,
}

/// Held only for its drop, which returns the charge.
enum Charge {
    Unbudgeted,
    #[cfg(feature = "filesystem")]
    Owned {
        _held: crate::filesystem::OwnedCharge,
    },
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native {
        _held: acyclic_fs::NativeAllocation,
    },
}

impl BoxCharge {
    /// For a box made where no allocation budget is live.
    pub(crate) const fn unbudgeted() -> Self {
        Self {
            _held: Charge::Unbudgeted,
        }
    }

    #[cfg(feature = "filesystem")]
    pub(crate) const fn owned(charge: crate::filesystem::OwnedCharge) -> Self {
        Self {
            _held: Charge::Owned { _held: charge },
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) const fn native(claim: acyclic_fs::NativeAllocation) -> Self {
        Self {
            _held: Charge::Native { _held: claim },
        }
    }

    /// Charges a box of `bytes` on `io`, or on nothing when no budget is live.
    #[cfg(feature = "filesystem")]
    pub(crate) fn on(
        io: Option<&crate::filesystem::PreparationIo>,
        bytes: u64,
    ) -> crate::Result<Self> {
        io.map_or_else(|| Ok(Self::unbudgeted()), |io| io.charge_box(bytes))
    }

    /// Charges a box of `bytes` on the original native ledger, when one is live.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn on_native(
        work: Option<&acyclic_fs::NativeMaterializationWork>,
        bytes: u64,
    ) -> crate::Result<Self> {
        let Some(work) = work else {
            return Ok(Self::unbudgeted());
        };
        work.remaining()?;
        Ok(Self::native(work.claim(
            bytes,
            u64::from(bytes != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?))
    }
}

pin_project_lite::pin_project! {
    /// A future in a box whose bytes and allocation were paid for before it
    /// was made; the charge is returned as the box is freed.
    pub struct ChargedFrame<F> {
        // Declared first: the future is dropped before its charge returns.
        #[pin]
        future: F,
        charge: BoxCharge,
    }
}

impl<F: std::future::Future> std::future::Future for ChargedFrame<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project().future.poll(cx)
    }
}

/// Boxes the future `build` makes, after charging the whole frame on `io`
/// when a budget is live. A refused frame is never built.
#[cfg(feature = "filesystem")]
pub fn charged_frame<'a, F>(
    io: Option<&crate::filesystem::PreparationIo>,
    build: impl FnOnce() -> F,
) -> crate::Result<acyclic_stream::BoxProviderFuture<'a, F::Output>>
where
    F: std::future::Future + acyclic_stream::ProviderTask + 'a,
{
    let charge = BoxCharge::on(io, std::mem::size_of::<ChargedFrame<F>>() as u64)?;
    // Charged above: the whole frame, future and charge alike.
    Ok(Box::pin(ChargedFrame {
        future: build(),
        charge,
    }))
}

/// [`charged_frame`] for a future awaited in place, which need not be a
/// provider task: a recursive call, whose `Send` cannot be proven inside its
/// own body.
#[cfg(feature = "filesystem")]
pub(crate) fn pinned_frame<F: std::future::Future>(
    io: Option<&crate::filesystem::PreparationIo>,
    build: impl FnOnce() -> F,
) -> crate::Result<std::pin::Pin<Box<ChargedFrame<F>>>> {
    let charge = BoxCharge::on(io, std::mem::size_of::<ChargedFrame<F>>() as u64)?;
    // Charged above: the whole frame, future and charge alike.
    Ok(Box::pin(ChargedFrame {
        future: build(),
        charge,
    }))
}

/// [`charged_frame`] on history work: the whole frame is charged on `work`
/// when it is live, then `build` borrows `work` to make the future.
#[cfg(feature = "filesystem")]
pub(crate) fn history_frame<'w, 'h, F>(
    work: &'w mut Option<&'h mut dyn crate::store::history::HistoryIo>,
    build: impl FnOnce(&'w mut Option<&'h mut dyn crate::store::history::HistoryIo>) -> F,
) -> crate::Result<std::pin::Pin<Box<ChargedFrame<F>>>>
where
    F: std::future::Future,
{
    let bytes = std::mem::size_of::<ChargedFrame<F>>() as u64;
    let charge = match work.as_deref_mut() {
        Some(io) => io.charge_box(bytes)?,
        None => BoxCharge::unbudgeted(),
    };
    // Charged above: the whole frame, future and charge alike.
    Ok(Box::pin(ChargedFrame {
        future: build(work),
        charge,
    }))
}

/// A box whose bytes and allocation were paid for before it was made.
pub struct ChargedBox<T> {
    // Declared first: the box is freed before its charge is returned.
    value: Box<T>,
    _charge: BoxCharge,
}

impl<T> ChargedBox<T> {
    /// Boxes `value` after charging the box on `io`, when a budget is live.
    #[cfg(feature = "filesystem")]
    pub fn new(io: Option<&crate::filesystem::PreparationIo>, value: T) -> crate::Result<Self> {
        let charge = BoxCharge::on(io, std::mem::size_of::<T>() as u64)?;
        Ok(Self {
            // Charged above: the box's bytes and attempt.
            value: Box::new(value),
            _charge: charge,
        })
    }

    /// Boxes `value` after charging the box on the history work, when present.
    #[cfg(feature = "filesystem")]
    pub(crate) fn in_history(
        value: T,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> crate::Result<Self> {
        let charge = match work.as_deref_mut() {
            Some(io) => io.charge_box(std::mem::size_of::<T>() as u64)?,
            None => BoxCharge::unbudgeted(),
        };
        Ok(Self {
            // Charged above: the box's bytes and attempt.
            value: Box::new(value),
            _charge: charge,
        })
    }

    /// Boxes `value` where no allocation budget is live.
    ///
    /// Every call is preceded by an `// Unbudgeted:` comment naming why.
    #[cfg(feature = "filesystem")]
    pub(crate) fn unbudgeted(value: T) -> Self {
        Self {
            // Unbudgeted: each caller states why at its call.
            value: Box::new(value),
            _charge: BoxCharge::unbudgeted(),
        }
    }

    /// Charges a box for a value that does not exist yet, on `io` when a
    /// budget is live; [`BoxReservation::fill`] makes the box it paid for.
    #[cfg(feature = "filesystem")]
    pub fn reserve(
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> crate::Result<BoxReservation<T>> {
        Ok(BoxReservation {
            charge: BoxCharge::on(io, std::mem::size_of::<T>() as u64)?,
            _value: std::marker::PhantomData,
        })
    }

    /// Splits the box from its charge, for moving fields out of the box.
    ///
    /// Bind the charge first (`let (_charge, boxed) = ...`): locals drop in
    /// reverse order, so the box is then freed before the charge returns.
    // Charged above: hands back the box its charge already pays for.
    pub fn into_box(self) -> (BoxCharge, Box<T>) {
        let Self { value, _charge } = self;
        (_charge, value)
    }

    /// Moves the value out; the box is freed before its charge is returned.
    pub fn into_inner(self) -> T {
        let Self { value, _charge } = self;
        let value = *value;
        drop(_charge);
        value
    }
}

impl<T> std::ops::Deref for ChargedBox<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for ChargedBox<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ChargedBox<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(formatter)
    }
}

/// A charge for exactly one future `Box<T>`, returned if it is never filled.
#[must_use = "a box reservation returns its charge when dropped"]
pub struct BoxReservation<T> {
    charge: BoxCharge,
    _value: std::marker::PhantomData<fn() -> T>,
}

impl<T> BoxReservation<T> {
    /// Makes the box this reservation paid for.
    pub fn fill(self, value: T) -> ChargedBox<T> {
        ChargedBox {
            // Charged above: the reservation paid for this box.
            value: Box::new(value),
            _charge: self.charge,
        }
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
#[path = "charged_box_tests.rs"]
mod tests;
