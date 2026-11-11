use super::{DecodeKind, DecodeWork, Funded, MaterializationDecode};
use crate::kernel::{KernelAccounting, Retained, RetainedAllocation};
use std::cell::RefCell;

struct OriginalDecodeWork<'a, 'b>(RefCell<&'a mut KernelAccounting<'b>>);

impl DecodeWork for OriginalDecodeWork<'_, '_> {
    type Allocation = RetainedAllocation;
    type Error = crate::FsError;

    fn admit(&self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.0.borrow().admit(delta)
    }
    fn spent(&self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.0.borrow_mut().record_completed(delta)
    }
    fn charge(&self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.0.borrow_mut().charge(delta)
    }
    fn claim(&self, bytes: u64, operations: u64) -> Result<Self::Allocation, Self::Error> {
        RetainedAllocation::claim(bytes, operations, &mut self.0.borrow_mut())
    }
    fn grow(&self, allocation: &mut Self::Allocation, bytes: u64) -> Result<(), Self::Error> {
        allocation.absorb(&mut self.claim(bytes, u64::from(bytes != 0))?)
    }
    fn absorb(
        &self,
        allocation: &mut Self::Allocation,
        incoming: &mut Self::Allocation,
    ) -> Result<(), Self::Error> {
        allocation.absorb(incoming)
    }
}

/// A JSON projection failure, preserving the original accounting error.
#[derive(Debug, thiserror::Error)]
pub enum MetadataDecodeError {
    /// Work, cancellation or allocation admission failed.
    #[error(transparent)]
    Work(#[from] crate::FsError),
    /// The selected bytes do not encode the requested JSON value.
    #[error(transparent)]
    Codec(#[from] serde_json::Error),
}

impl KernelAccounting<'_> {
    /// Decodes JSON with the shared funded visitor. The returned value retains
    /// its admitted allocation credit on this caller's original ledger.
    /// Unselected fields are parsed without materializing their strings or
    /// collections.
    pub fn decode_json<T: serde::de::DeserializeOwned>(
        &mut self,
        input: &[u8],
    ) -> Result<Retained<T>, MetadataDecodeError> {
        from_slice(input, self)
    }
}

/// Retains decoded metadata on the caller's original allocation ledger.
pub(crate) fn from_slice<T: serde::de::DeserializeOwned>(
    input: &[u8],
    accounting: &mut KernelAccounting<'_>,
) -> Result<Retained<T>, MetadataDecodeError> {
    accounting.check()?;
    let scratch_bytes = (input.len() as u64)
        .checked_mul(2)
        .ok_or(crate::WorkError::Overflow)
        .map_err(crate::FsError::from)?
        .max(8);
    let _scratch = RetainedAllocation::claim(scratch_bytes, 1, accounting)?;
    let work = OriginalDecodeWork(RefCell::new(accounting));
    let decode = MaterializationDecode {
        work: &work,
        allocation: RefCell::new(work.claim(0, 0)?),
        failure: RefCell::new(None),
    };
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let result = T::deserialize(Funded {
        inner: &mut deserializer,
        decode: &decode,
        kind: DecodeKind::Borrowed,
    })
    .and_then(|value| deserializer.end().map(|()| value));
    drop(deserializer);
    if let Some(error) = decode.failure.borrow_mut().take() {
        return Err(error.into());
    }
    let value = result?;
    Ok(decode.allocation.into_inner().into_value(value))
}
