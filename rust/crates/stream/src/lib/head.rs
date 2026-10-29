/// Ordered observations with inline storage for a single path. Larger views grow
/// with the caller's budget; the inline capacity is not a result limit.
pub type HeadSet<T> = smallvec::SmallVec<[T; 1]>;

/// One atomic observation of a path's accepted history and durable prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathHead {
    /// Explicit irreversible-prefix proof, independent of crash persistence.
    /// Present only in response to a requested `durable_through` confirmation.
    pub retained_through: Option<u64>,
    /// Next accepted sequence.
    pub tail: u64,
    /// Last accepted record when history is requested, absent for an empty path.
    pub last: Option<Record>,
    /// Confirmed durable prefix; zero makes no persistence claim.
    pub durable: u64,
    /// Immediate inherited source when history is requested, at its immutable cut.
    pub origin: Option<ForkOrigin>,
}

/// Data required for each named path at the shared snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HeadProjection {
    /// Accepted/durable offsets, the last record and inherited fork origin.
    #[default]
    History,
    /// Accepted/durable offsets only; last record and origin are omitted.
    Watermarks,
    /// Accepted offset only; no durability metadata is consulted.
    Tail,
}

/// One original record appended by a retained publication, independently of
/// the path's current head or ancestry. Inherited fork records belong to their
/// original publication and destination, not to the inheriting path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedRecord {
    /// Original publication identity.
    pub commit_id: CommitId,
    /// Original append destination.
    pub path: StreamPath,
    /// Original sequence assigned at that destination.
    pub sequence: u64,
}

/// An original publication observation bound to its selected destination,
/// including when no record exists at the requested position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedRecordObservation {
    /// Original append destination, echoed in selection order.
    pub path: StreamPath,
    /// Original record, or absence without proof of nonexecution.
    pub record: Option<Record>,
}

/// Named observations and an optional hierarchy page at one authority cut.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadsRequest {
    /// Original retained records, in response order. A missing publication or
    /// position yields `None`, never proof that a command did not execute.
    /// Every selected path requires read authorization for the whole view.
    pub retained: HeadSet<RetainedRecord>,
    /// Requests an irreversible-prefix proof through this sequence for each
    /// named path. A deferred backend seals the captured prefix only when the
    /// requested sequence is accepted and not already confirmed.
    pub durable_through: Option<u64>,
    /// Selects history details or an offset-only observation.
    pub projection: HeadProjection,
    /// Exact paths, in response order. An absent path has a `None` observation.
    pub paths: HeadSet<StreamPath>,
    /// Optional direct-child page, including its existing continuation fence.
    pub children: Option<ChildrenPageRequest>,
    /// Nonzero total result budget: named paths, retained selections and children.
    /// Explicit selections must fit; the remainder bounds the child page.
    pub limit: u32,
    /// Minimum applied authority cut, checked before the snapshot is captured.
    pub at_least: RevisionCut,
}

impl HeadsRequest {
    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        let named = self.selection_count()?;
        if self.limit == 0
            || named > self.limit
            || (self.children.is_some() && named == self.limit)
            || (self.projection == HeadProjection::Tail && self.durable_through.is_some())
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(())
    }

    pub(crate) fn selection_count(&self) -> Result<u32, StreamError> {
        self.paths
            .len()
            .checked_add(self.retained.len())
            .and_then(|count| u32::try_from(count).ok())
            .ok_or(StreamError::InvalidArgument)
    }
}

/// A coherent view. No component was observed before or after `snapshot`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Heads {
    /// Original retained records in selection order at this snapshot. Returned
    /// bytes, sequence, publication identity and time are the original facts;
    /// they do not certify that the current path still contains that prefix.
    pub retained: HeadSet<RetainedRecordObservation>,
    /// Exact projection used for every named path.
    pub projection: HeadProjection,
    /// Exact authority revision shared by heads, absence and the hierarchy page.
    pub snapshot: Revision,
    /// One observation for every requested path, in request order.
    pub heads: HeadSet<Option<PathHead>>,
    /// Optional page at the same snapshot, with its own hierarchy continuation.
    pub children: Option<ChildrenPage>,
}

impl Heads {
    pub(crate) fn check_size(&self) -> Result<(), StreamError> {
        if crate::wire_codec::requests::heads_len(self) > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok(())
    }

    pub(crate) fn validate_for(&self, request: &HeadsRequest) -> Result<(), StreamError> {
        let child_count = self.children.as_ref().map_or(0, |page| page.children.len());
        if self.projection != request.projection
            || self.heads.len() != request.paths.len()
            || self.retained.len() != request.retained.len()
            || self.children.is_some() != request.children.is_some()
            || child_count
                > request
                    .children
                    .as_ref()
                    .map_or(0, |page| page.limit as usize)
            || self
                .heads
                .len()
                .saturating_add(self.retained.len())
                .saturating_add(child_count)
                > request.limit as usize
            || !request.at_least.proven_by(Some(&self.snapshot))
        {
            return Err(StreamError::InvalidResponse);
        }
        for (observation, selected) in self.retained.iter().zip(&request.retained) {
            if observation.path != selected.path
                || observation.record.as_ref().is_some_and(|record| {
                    record.commit_id != selected.commit_id || record.sequence != selected.sequence
                })
            {
                return Err(StreamError::InvalidResponse);
            }
        }
        for head in self.heads.iter().flatten() {
            head.validate_projection(self.projection)?;
            if head.retained_through.is_some() && head.retained_through != request.durable_through {
                return Err(StreamError::InvalidResponse);
            }
        }
        Ok(())
    }
}

impl PathHead {
    pub(crate) fn validate_projection(
        &self,
        projection: HeadProjection,
    ) -> Result<(), StreamError> {
        if self
            .retained_through
            .is_some_and(|through| through > self.tail)
            || (projection == HeadProjection::Tail && self.retained_through.is_some())
        {
            return Err(StreamError::InvalidResponse);
        }
        match projection {
            HeadProjection::History => self.validate(),
            HeadProjection::Tail if self.durable != 0 => Err(StreamError::InvalidResponse),
            HeadProjection::Tail | HeadProjection::Watermarks
                if self.last.is_none() && self.origin.is_none() && self.durable <= self.tail =>
            {
                Ok(())
            }
            HeadProjection::Tail | HeadProjection::Watermarks => Err(StreamError::InvalidResponse),
        }
    }

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
