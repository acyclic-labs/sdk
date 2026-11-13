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

/// A serde projection failure, preserving the original accounting error.
#[derive(Debug, thiserror::Error)]
pub enum MetadataDecodeError<E = serde_json::Error> {
    /// Work, cancellation or allocation admission failed.
    #[error(transparent)]
    Work(#[from] crate::FsError),
    /// The selected input does not encode the requested value.
    #[error(transparent)]
    Codec(E),
}

impl KernelAccounting<'_> {
    /// Runs a serde decoder through the one funded destination visitor.
    /// The codec supplies its own admitted scratch and checks its input's end;
    /// the returned value keeps the original destination allocation credit.
    /// This consumes exactly one value, not necessarily the entire input. Use
    /// `decode_json` for a complete JSON document; streaming codecs must check
    /// their own end after the value (serde's generic trait has no end method).
    pub fn deserialize<'de, T, D>(
        &mut self,
        deserializer: D,
    ) -> Result<Retained<T>, MetadataDecodeError<D::Error>>
    where
        T: serde::Deserialize<'de>,
        D: serde::Deserializer<'de>,
    {
        self.check()?;
        let work = OriginalDecodeWork(RefCell::new(self));
        let decode = MaterializationDecode {
            work: &work,
            allocation: RefCell::new(work.claim(0, 0)?),
            failure: RefCell::new(None),
        };
        let result = T::deserialize(Funded {
            inner: deserializer,
            decode: &decode,
            kind: DecodeKind::Borrowed,
        });
        if let Some(error) = decode.failure.borrow_mut().take() {
            return Err(error.into());
        }
        let value = result.map_err(MetadataDecodeError::Codec)?;
        Ok(decode.allocation.into_inner().into_value(value))
    }

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
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let value = accounting.deserialize(&mut deserializer)?;
    deserializer.end().map_err(MetadataDecodeError::Codec)?;
    Ok(value)
}
