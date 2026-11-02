//! A fixed-size, owner-written spend balance. The admitting host supplies the
//! original funding; a balance does not issue a grant or authorize execution.
//! Stage debits in the owner's existing state transaction and publish them with
//! the step. Discarded candidates do not spend; committed debits never expire.
use acyclic_stream::CommitId;
use bytes::Bytes;

const FORMAT: &[u8] = b"\xffAES\x01";
pub(crate) const MAX_RECORD_LEN: usize = FORMAT.len() + 2 + 32 + 8;

/// The amount this agent can still spend. `None` is explicitly unbounded;
/// absence of an escrow record is a missing prerequisite, never unbounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Escrow {
    remaining: Option<u64>,
    branch: Option<CommitId>,
}

/// Refusal before any balance is changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The original host has not supplied this owner's funding.
    #[error("actor escrow has no admitted funding")]
    Missing,
    /// Initial funding cannot replace an existing balance or committed debit.
    #[error("actor escrow is already funded")]
    AlreadyFunded,
    /// Inherited funding belongs to the original branch, not its copy.
    #[error("actor escrow belongs to another branch")]
    ForeignBranch,
    /// The finite balance cannot fund this debit.
    #[error("actor escrow is exhausted")]
    Exhausted,
    /// The record does not have the exact supported format and length.
    #[error("invalid actor escrow record")]
    InvalidRecord,
}

impl Escrow {
    /// Retain funding admitted by the original host. This value conveys no
    /// authority; parent suballocation and the child's creation must publish
    /// atomically before the child can use it.
    pub const fn new(remaining: Option<u64>) -> Self {
        Self {
            remaining,
            branch: None,
        }
    }

    /// Remaining admitted units; zero and unbounded have distinct encodings.
    pub const fn remaining(self) -> Option<u64> {
        self.remaining
    }

    /// Debit one candidate. The owner's original step commit makes it durable.
    /// Exhaustion leaves the original value intact. No clock, lease or refund
    /// can undo this debit after publication.
    pub fn debit(&mut self, units: u64) -> Result<(), Error> {
        if let Some(remaining) = self.remaining {
            self.remaining = Some(remaining.checked_sub(units).ok_or(Error::Exhausted)?);
        }
        Ok(())
    }

    /// Move `amount` of this balance into new funding for another actor (a
    /// creation or branch overlay in the same commit). Conservation: a finite
    /// balance loses exactly what the allocation holds, so the two together
    /// never exceed the original; an unbounded balance can fund anything; a
    /// finite one cannot fund an unbounded allocation. A refusal changes
    /// nothing. The allocation belongs to whichever branch its overlay starts.
    pub fn allocate(&mut self, amount: Option<u64>) -> Result<Self, Error> {
        match (self.remaining, amount) {
            (None, _) => {}
            (Some(_), None) => return Err(Error::Exhausted),
            (Some(remaining), Some(amount)) => {
                self.remaining = Some(remaining.checked_sub(amount).ok_or(Error::Exhausted)?);
            }
        }
        Ok(Self::new(amount))
    }

    /// Encode one bounded record, independent of other agents and history.
    pub fn encode(self) -> Bytes {
        let mut record = Vec::with_capacity(
            FORMAT.len() + 2 + self.branch.map_or(0, |_| 32) + self.remaining.map_or(0, |_| 8),
        );
        record.extend_from_slice(FORMAT);
        match self.branch {
            None => record.push(0),
            Some(branch) => {
                record.push(1);
                record.extend_from_slice(branch.as_bytes());
            }
        }
        match self.remaining {
            None => record.push(0),
            Some(remaining) => {
                record.push(1);
                record.extend_from_slice(&remaining.to_be_bytes());
            }
        }
        record.into()
    }

    /// Validate the boundary once, without a second encoding or heap value.
    pub fn decode(record: &[u8]) -> Result<Self, Error> {
        let (branch, balance) = match record.strip_prefix(FORMAT) {
            Some([0, balance @ ..]) => (None, balance),
            Some([1, rest @ ..]) if rest.len() >= 32 => {
                let (identity, balance) = rest.split_at(32);
                let identity = identity.try_into().map_err(|_| Error::InvalidRecord)?;
                (Some(CommitId::from_bytes(identity)), balance)
            }
            _ => return Err(Error::InvalidRecord),
        };
        let mut escrow = match balance {
            [0] => Self::new(None),
            [1, a, b, c, d, e, f, g, h] => {
                Self::new(Some(u64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h])))
            }
            _ => return Err(Error::InvalidRecord),
        };
        escrow.branch = branch;
        Ok(escrow)
    }

    pub(crate) fn in_branch(mut self, branch: Option<CommitId>) -> Self {
        self.branch = branch;
        self
    }

    pub(crate) fn require_branch(self, branch: Option<CommitId>) -> Result<Self, Error> {
        if self.branch != branch {
            return Err(Error::ForeignBranch);
        }
        Ok(self)
    }
}

#[cfg(kani)]
mod proofs {
    use super::{Error, Escrow};

    /// No double spend: an allocation never creates units. For every
    /// balance and request, a finite source keeps `remaining - amount` and
    /// the allocation holds `amount`; a refusal leaves the source unchanged.
    #[kani::proof]
    fn allocation_conserves_funding() {
        let remaining: Option<u64> = kani::any();
        let amount: Option<u64> = kani::any();
        let mut source = Escrow::new(remaining);
        let before = source;
        match source.allocate(amount) {
            Ok(allocated) => {
                assert_eq!(allocated.remaining(), amount);
                assert_eq!(allocated.branch, None);
                match (before.remaining(), source.remaining(), amount) {
                    (None, None, _) => {}
                    (Some(original), Some(left), Some(moved)) => {
                        assert_eq!(u128::from(left) + u128::from(moved), u128::from(original));
                    }
                    _ => panic!("finite funding became unbounded or funded an unbounded child"),
                }
            }
            Err(error) => {
                assert_eq!(error, Error::Exhausted);
                assert_eq!(source, before);
            }
        }
    }
}
