use acyclic_harness::client::*;
use serde::{Deserialize, Serialize};
use std::{cell::Cell, rc::Rc, sync::Arc};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Basis {
    pub authority: u64,
    pub generation: u64,
    pub revision: u64,
    pub content: u64,
}

const DEEP_BYTES: usize =
    std::mem::size_of::<Basis>() + std::mem::size_of::<u64>() + 4 * std::mem::size_of::<usize>(); // two Arc payloads and their refcount headers

#[derive(Clone)]
pub struct Evidence {
    pub key: u64,
    pub basis: Basis,
    pub value: u64,
    pub operation: Option<(u64, OperationOutcome)>,
    pub publish: bool,
}

// A deliberately small domain for exhaustive kernel transitions, not a domain
// engine supplied to consumers. Real Harness reuse is tested separately.
pub struct Numbers {
    pub visits: Rc<Cell<usize>>,
    pub adapter: Rc<Cell<u128>>,
}

impl Default for Numbers {
    fn default() -> Self {
        Self {
            visits: Rc::new(Cell::new(0)),
            adapter: Rc::new(Cell::new(1)),
        }
    }
}

impl Domain for Numbers {
    type Key = u64;
    type Basis = Basis;
    type Operation = u64;
    type Assumption = u64;
    type Value = u64;
    type Evidence = Evidence;
    fn identity(&self) -> u128 {
        self.adapter.get()
    }
    fn validate(
        &self,
        fact: &Fact<u64, Basis, u64>,
        _: &u64,
        assumption: &u64,
        work: usize,
    ) -> Result<(usize, usize), Error> {
        if work == 0 {
            return Err(Error::Budget);
        }
        if fact.value.as_ref() != assumption {
            return Err(Error::Conflict);
        }
        self.visits.set(self.visits.get() + 1);
        Ok((DEEP_BYTES, 1))
    }
    fn observe(
        &self,
        evidence: &Evidence,
        current: Option<&Fact<u64, Basis, u64>>,
        work: usize,
    ) -> Result<Observation<u64, Basis, u64, u64>, Error> {
        if work == 0 {
            return Err(Error::Budget);
        }
        self.visits.set(self.visits.get() + 1);
        if evidence.publish {
            if evidence.basis.content != evidence.value {
                return Err(Error::Conflict);
            }
            if let Some(old) = current
                && (evidence.basis.authority != old.basis.authority
                    || evidence.basis.generation != old.basis.generation
                    || evidence.basis.revision < old.basis.revision
                    || evidence.basis.revision > old.basis.revision + 1
                    || (evidence.basis.revision == old.basis.revision
                        && evidence.basis != *old.basis))
            {
                return Err(Error::Conflict);
            }
        }
        Ok(Observation {
            fact: evidence.publish.then(|| Fact {
                key: evidence.key,
                basis: Arc::new(evidence.basis.clone()),
                value: Arc::new(evidence.value),
                bytes: DEEP_BYTES,
            }),
            operation: evidence.operation,
            work: 1,
        })
    }
    fn corresponds(
        &self,
        predicted: &u64,
        canonical: &u64,
        work: usize,
    ) -> Result<(Correspondence, usize), Error> {
        if work == 0 {
            return Err(Error::Budget);
        }
        if *canonical == u64::MAX {
            return Err(Error::Unsupported);
        }
        self.visits.set(self.visits.get() + 1);
        Ok((
            if predicted == canonical {
                Correspondence::Match
            } else {
                Correspondence::Different
            },
            1,
        ))
    }
}

pub fn limits() -> Limits {
    Limits {
        records: 16,
        branches: 16,
        edges: 32,
        bytes: 262_144,
        work: 256,
        retention: 100,
        visible: 8,
    }
}
pub fn evidence(
    key: u64,
    revision: u64,
    value: u64,
    operation: Option<(u64, OperationOutcome)>,
) -> Evidence {
    Evidence {
        key,
        basis: Basis {
            authority: 1,
            generation: 1,
            revision,
            content: value,
        },
        value,
        operation,
        publish: true,
    }
}
pub fn status(key: u64, operation: u64, outcome: OperationOutcome) -> Evidence {
    Evidence {
        publish: false,
        ..evidence(key, 0, 0, Some((operation, outcome)))
    }
}
pub fn request(
    key: u64,
    revision: u64,
    original: u64,
    predicted: u64,
    operation: Option<u64>,
    dependencies: Vec<BranchId>,
) -> Begin<u64, Basis, u64, u64, u64> {
    Begin {
        key,
        basis: Arc::new(evidence(key, revision, original, None).basis),
        operation,
        assumption: original,
        predicted: Arc::new(predicted),
        dependencies: dependencies
            .into_iter()
            .map(Dependency::prediction)
            .collect(),
        expires: 50,
    }
}
