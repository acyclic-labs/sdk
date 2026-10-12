use super::*;

mod hypothesis;
mod observation;
mod retention;

fn metadata_bytes<T>() -> Result<usize, Error> {
    std::mem::size_of::<T>()
        .checked_add(64)
        .and_then(|n| n.checked_mul(32))
        .ok_or(Error::Budget)
}

fn dependency_bytes(length: usize, capacity: usize) -> Result<usize, Error> {
    let backing = capacity
        .checked_mul(std::mem::size_of::<Dependency>())
        .ok_or(Error::Budget)?;
    let indexes = length
        .checked_mul(metadata_bytes::<Dependency>()?)
        .ok_or(Error::Budget)?;
    backing.checked_add(indexes).ok_or(Error::Budget)
}

/// Demand-scoped client kernel. Construction grants no authority and starts no work.
pub struct Client<D: Domain> {
    domain: D,
    adapter: u128,
    pub(in crate::client) limits: Limits,
    namespace: u128,
    sequence: u64,
    now: u64,
    facts: BTreeMap<D::Key, Record<D>>,
    branches: BTreeMap<BranchId, ClientHypothesis<D>>,
    by_key: BTreeMap<D::Key, BTreeSet<BranchId>>,
    by_operation: BTreeMap<D::Operation, BranchId>,
    dependents: BTreeMap<BranchId, BTreeSet<BranchId>>,
    bytes: usize,
    edges: usize,
}

impl<D: Domain> Client<D> {
    /// Construct an empty client with explicit finite positive bounds.
    pub fn new(domain: D, namespace: u128, sequence: u64, limits: Limits) -> Result<Self, Error> {
        if [
            limits.records,
            limits.branches,
            limits.edges,
            limits.bytes,
            limits.work,
            limits.visible,
        ]
        .contains(&0)
            || limits.retention == 0
            || limits.visible > limits.work
        {
            return Err(Error::Budget);
        }
        let adapter = domain.identity();
        Ok(Self {
            domain,
            adapter,
            limits,
            namespace,
            sequence,
            now: 0,
            facts: BTreeMap::new(),
            branches: BTreeMap::new(),
            by_key: BTreeMap::new(),
            by_operation: BTreeMap::new(),
            dependents: BTreeMap::new(),
            bytes: 0,
            edges: 0,
        })
    }

    /// Last allocated sequence for data-only host checkpoints.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Accounted resident bytes, records, branches and edges.
    pub fn residency(&self) -> (usize, usize, usize, usize) {
        (
            self.bytes,
            self.facts.len(),
            self.branches.len(),
            self.edges,
        )
    }
}
