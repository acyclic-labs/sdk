use crate::kernel::serde_decode::DecodeWork;
pub(super) use crate::kernel::serde_decode::{DecodeKind, Funded, MaterializationDecode};

impl DecodeWork for crate::NativeMaterializationWork {
    type Allocation = crate::NativeAllocation;
    type Error = crate::NativeTreeMaterializationError;

    fn admit(&self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.admit(delta)
    }
    fn spent(&self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.spent(delta)
    }
    fn charge(&self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.charge(delta)
    }
    fn claim(&self, bytes: u64, operations: u64) -> Result<Self::Allocation, Self::Error> {
        self.claim(bytes, operations, crate::WorkCounters::UNCHARGED)
    }
    fn grow(&self, allocation: &mut Self::Allocation, bytes: u64) -> Result<(), Self::Error> {
        allocation.grow(bytes)
    }
    fn absorb(
        &self,
        allocation: &mut Self::Allocation,
        incoming: &mut Self::Allocation,
    ) -> Result<(), Self::Error> {
        allocation.absorb(incoming)
    }
}
