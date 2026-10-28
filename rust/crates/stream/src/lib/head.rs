/// One atomic observation of a path's accepted history and durable prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathHead {
    /// Next accepted sequence.
    pub tail: u64,
    /// Last accepted record, absent for an empty path.
    pub last: Option<Record>,
    /// Confirmed durable prefix; zero makes no persistence claim.
    pub durable: u64,
    /// Immediate inherited source at the fork's immutable cut.
    pub origin: Option<ForkOrigin>,
}

impl PathHead {
    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        if self.durable > self.tail
            || match &self.last {
                Some(record) => record.sequence.checked_add(1) != Some(self.tail),
                None => self.tail != 0,
            }
            || self.origin.as_ref().is_some_and(|origin| origin.forked_at > self.tail)
        {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}

/// The source of this path's most recent enclosing subtree fork.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkOrigin {
    /// Source path, including this descendant's relative suffix.
    pub source: StreamPath,
    /// Exclusive inherited source prefix, unaffected by later appends.
    pub forked_at: u64,
    /// The fork publication, rather than the last inherited record's commit.
    pub commit_id: CommitId,
}
