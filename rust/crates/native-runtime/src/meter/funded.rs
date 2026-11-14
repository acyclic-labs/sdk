use super::*;

/// A stored value and the original allocation charge that pays for it.
///
/// Storage retires before its charge. The value can be borrowed, but its
/// allocation owner cannot be detached, replaced or manually refunded.
#[must_use]
pub struct Funded<T> {
    pub(crate) data: T,
    pub(super) allocation: ReadAllocation,
}

impl<T> Funded<T> {
    /// Borrows the stored value without transferring its funding.
    pub fn value(&self) -> &T {
        &self.data
    }
}

impl ReadAllocation {
    /// Moves this original charge into the value whose storage it pays for.
    pub fn store<T>(self, data: T) -> Funded<T> {
        Funded {
            data,
            allocation: self,
        }
    }

    /// Funds construction before it starts; refusal drops the original charge.
    pub fn try_store<T, E>(self, construct: impl FnOnce() -> Result<T, E>) -> Result<Funded<T>, E> {
        let data = construct()?;
        Ok(self.store(data))
    }
}
