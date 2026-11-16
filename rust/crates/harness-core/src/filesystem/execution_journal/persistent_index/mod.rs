//! Immutable, content-addressed radix pages for bounded checkpoint recovery.
use super::*;
use acyclic_stream::{OwnedBytes, StreamPath};

mod cache;
pub(super) mod node;
pub(super) use cache::NodeCache;
mod read;
mod update;
mod value;
use node::Node;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NodeRef {
    pub(super) sequence: u64,
    pub(super) digest: [u8; 32],
}

pub(super) struct Reader<'a, P: ?Sized> {
    pub(super) provider: &'a Arc<P>,
    pub(super) path: &'a StreamPath,
    pub(super) base: u64,
    pub(super) staged: &'a [OwnedBytes],
    pub(super) io: Option<&'a PreparationIo>,
    pub(super) cache: Option<&'a NodeCache>,
}

pub(super) struct Writer<'a, P: ?Sized> {
    provider: &'a Arc<P>,
    path: &'a StreamPath,
    base: u64,
    scratch: Scratch,
    // Encoded values retire before their separate payload ownership pool.
    content: Funded<()>,
    cache: NodeCache,
}

/// Reusable staging capacities retain their original ledger and no old values.
#[derive(Default)]
pub(super) struct Scratch {
    records: Funded<OwnedBytes>,
    retired: Funded<NodeRef>,
    live: Funded<bool>,
    path: Funded<update::Ancestor>,
}

impl Scratch {
    fn new(io: Option<&PreparationIo>) -> Result<Self> {
        Ok(Self {
            records: Funded::new(io)?,
            retired: Funded::new(io)?,
            live: Funded::new(io)?,
            path: Funded::new(io)?,
        })
    }

    fn bind(&mut self, io: Option<&PreparationIo>) -> Result<()> {
        self.records.bind(io)?;
        self.retired.bind(io)?;
        self.live.bind(io)?;
        self.path.bind(io)
    }

    fn clear(&mut self) {
        self.records.values.clear();
        self.retired.values.clear();
        self.live.values.clear();
        self.path.values.clear();
    }
}

/// Values retire before the pool that funds their backing storage.
pub(super) struct Funded<T> {
    pub(super) values: Vec<T>,
    io: Option<PreparationIo>,
}

impl<T> Default for Funded<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            io: None,
        }
    }
}

impl<T> Funded<T> {
    fn bind(&mut self, current: Option<&PreparationIo>) -> Result<()> {
        match (&mut self.io, current) {
            (Some(retained), Some(current)) if retained.shares_ledger(current) => {
                retained.use_current_context(current);
            }
            (None, None) => (),
            _ => return Err(invalid(current, "index staging belongs to another ledger")),
        }
        Ok(())
    }
    pub(super) fn new(io: Option<&PreparationIo>) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            io: io.map(PreparationIo::scoped).transpose()?,
        })
    }

    pub(super) fn push(&mut self, value: T) -> Result<()> {
        self.reserve_slot()?;
        self.values.push(value);
        Ok(())
    }

    fn reserve_slot(&mut self) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(1))
                .ok_or_else(|| invalid(self.io.as_ref(), "index staging capacity overflows"))?;
            crate::executor::stock_vec_reserve(&mut self.values, capacity, self.io.as_ref())?;
        }
        Ok(())
    }
}

impl<T> Drop for Funded<T> {
    fn drop(&mut self) {
        self.values = Vec::new();
        drop(self.io.take());
    }
}

fn invalid(io: Option<&PreparationIo>, message: &'static str) -> Error {
    canonical_boundary(io, crate::wire::ErrorCode::Storage, message)
}

fn direction(key: &[u8; 32], bit: u16, io: Option<&PreparationIo>) -> Result<usize> {
    let byte = key
        .get(usize::from(bit / 8))
        .ok_or_else(|| invalid(io, "execution index bit overflows"))?;
    Ok(usize::from((byte >> (7 - bit % 8)) & 1))
}

#[cfg(test)]
mod tests;
