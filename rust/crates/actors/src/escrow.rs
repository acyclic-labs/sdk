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

    /// Transfer funding into a call's immutable hold. Join the returned hold
    /// with this owner's debit in one request commit. A finite balance cannot
    /// fund an unbounded hold. Neither cancellation nor an unknown outcome
    /// credits this balance; the original executor host reconciles spend.
    pub fn hold(&mut self, units: Option<u64>) -> Result<Self, Error> {
        match units {
            Some(units) => self.debit(units)?,
            None if self.remaining.is_some() => return Err(Error::Exhausted),
            None => {}
        }
        Ok(Self {
            remaining: units,
            branch: self.branch,
        })
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
