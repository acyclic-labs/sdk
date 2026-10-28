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

/// Named observations and an optional hierarchy page at one authority cut.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadsRequest {
    /// Exact paths, in response order. An absent path has a `None` observation.
    pub paths: Vec<StreamPath>,
    /// Optional direct-child page, including its existing continuation fence.
    pub children: Option<ChildrenPageRequest>,
    /// Nonzero total result budget: named paths plus returned children.
    /// Named paths must fit; the remaining budget bounds the child page.
    pub limit: u32,
    /// Minimum applied authority cut, checked before the snapshot is captured.
    pub at_least: RevisionCut,
}

impl HeadsRequest {
    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        let named = u32::try_from(self.paths.len()).map_err(|_| StreamError::InvalidArgument)?;
        if self.limit == 0 || named > self.limit || (self.children.is_some() && named == self.limit)
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(())
    }
}

/// A coherent view. No component was observed before or after `snapshot`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Heads {
    /// Exact authority revision shared by heads, absence and the hierarchy page.
    pub snapshot: Revision,
    /// One observation for every requested path, in request order.
    pub heads: Vec<Option<PathHead>>,
    /// Optional page at the same snapshot, with its own hierarchy continuation.
    pub children: Option<ChildrenPage>,
}

impl Heads {
    pub(crate) fn validate_for(&self, request: &HeadsRequest) -> Result<(), StreamError> {
        let child_count = self.children.as_ref().map_or(0, |page| page.children.len());
        if self.heads.len() != request.paths.len()
            || self.children.is_some() != request.children.is_some()
            || child_count
                > request
                    .children
                    .as_ref()
                    .map_or(0, |page| page.limit as usize)
            || self.heads.len().saturating_add(child_count) > request.limit as usize
            || !request.at_least.proven_by(Some(&self.snapshot))
        {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}

impl PathHead {
    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        if self.durable > self.tail
            || match &self.last {
                Some(record) => record.sequence.checked_add(1) != Some(self.tail),
                None => self.tail != 0,
            }
            || self
                .origin
                .as_ref()
                .is_some_and(|origin| origin.forked_at > self.tail)
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
