//! Bounded literal search over the original stream namespace and records.
//! No index or background task: each continuation retains an immutable namespace
//! cut and a depth-first frontier, and uses the caller's authorized provider.

use crate::{
    ChildrenPageRequest, HeadProjection, Heads, HeadsRequest, ReadRequest, Record, Revision,
    RevisionCut, StreamError, StreamPath, StreamProvider,
};
use futures::TryStreamExt as _;
use std::num::NonZeroU32;

#[derive(Clone, Debug)]
struct Frame {
    path: StreamPath,
    sequence: u64,
    tail: Option<u64>,
    after: Option<StreamPath>,
}

/// Immutable continuation for one literal subtree search. Cloning/replaying a
/// cursor does not consume results. Its memory grows with depth, never history
/// length or the number of agents. A cursor is not an authorization capability.
#[derive(Clone, Debug)]
pub struct Cursor {
    pattern: Vec<u8>,
    at: Option<Revision>,
    frontier: Vec<Frame>,
}

#[cfg(feature = "serde")]
mod codec;

/// An addressable original record, without a second copy of its provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Match {
    /// Selected path, which can differ from the inherited record's origin.
    pub path: StreamPath,
    /// Original sequence, publication, origin and bytes.
    pub record: Record,
}

/// Results through one bounded work frontier. An empty page with `next` is
/// progress, not exhaustion; a sparse query never scans to its next match.
#[derive(Clone, Debug)]
pub struct Page {
    /// Matches in depth-first path order, then ascending record sequence.
    pub matches: Vec<Match>,
    /// Record inspections plus namespace observations charged to this page.
    pub work: u32,
    /// Continuation at the original cut, absent only when traversal is complete.
    pub next: Option<Cursor>,
}

impl Cursor {
    /// Original subtree and literal selected by this continuation.
    pub fn selection(&self) -> Option<(&StreamPath, &[u8])> {
        self.frontier
            .first()
            .map(|root| (&root.path, self.pattern.as_slice()))
    }
    /// Select a subtree and literal bytes. Construction performs no IO. Empty
    /// patterns select every record; there are no implicit result limits.
    pub fn new(root: StreamPath, pattern: Vec<u8>) -> Self {
        Self {
            pattern,
            at: None,
            frontier: vec![Frame {
                path: root,
                sequence: 0,
                tail: None,
                after: None,
            }],
        }
    }

    /// Read one bounded page through the original provider. Every named read
    /// and child page retains that provider's current authorization. No cached
    /// result or cursor can widen its grants. Concurrent appends and newly
    /// created children are excluded by the opening namespace cut.
    ///
    /// Cancellation and errors leave this cursor unchanged, so no partial
    /// result is acknowledged or lost. Unsupported historical cuts refuse;
    /// they are never replaced with a newer observation.
    pub async fn page<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        budget: NonZeroU32,
    ) -> Result<Page, StreamError> {
        crate::memory::validate_limit(budget.get())?;
        let mut cursor = self.clone();
        let mut matches = Vec::new();
        let mut work = 0;
        while work < budget.get() {
            let Some(frame) = cursor.frontier.last_mut() else {
                break;
            };
            if frame.tail.is_none() {
                let observed =
                    observe(provider, cursor.at.clone(), Some(frame.path.clone()), None).await?;
                cursor.at = Some(observed.snapshot);
                frame.tail = Some(
                    observed
                        .heads
                        .first()
                        .and_then(Option::as_ref)
                        .map_or(0, |h| h.tail),
                );
                work += 1;
                continue;
            }
            let tail = frame.tail.ok_or(StreamError::InvalidResponse)?;
            if frame.sequence < tail {
                let count = u64::from(budget.get() - work).min(tail - frame.sequence) as u32;
                let at = cursor.at.clone().ok_or(StreamError::InvalidResponse)?;
                matches.extend(scan(provider, frame, at, &cursor.pattern, count).await?);
                work += count;
                continue;
            }
            let observed = observe(
                provider,
                cursor.at.clone(),
                None,
                Some(ChildrenPageRequest {
                    tail_stream: None,
                    parent: Some(frame.path.clone()),
                    after: frame.after.clone(),
                    offset: 0,
                    hierarchy_version: None,
                    limit: 1,
                }),
            )
            .await?;
            let children = observed.children.ok_or(StreamError::InvalidResponse)?;
            work += 1;
            if let Some(child) = children.children.into_iter().next() {
                frame.after = Some(child.path.clone());
                cursor.frontier.push(Frame {
                    path: child.path,
                    sequence: 0,
                    tail: None,
                    after: None,
                });
            } else if let Some(after) = children.next_after {
                // Authorization can hide every child in a provider page.
                frame.after = Some(after);
            } else {
                cursor.frontier.pop();
            }
        }
        Ok(Page {
            matches,
            work,
            next: (!cursor.frontier.is_empty()).then_some(cursor),
        })
    }
}

async fn scan<P: StreamProvider + ?Sized>(
    provider: &P,
    frame: &mut Frame,
    at: Revision,
    pattern: &[u8],
    count: u32,
) -> Result<Vec<Match>, StreamError> {
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: frame.path.clone(),
            from: frame.sequence,
            limit: count,
            at_least: RevisionCut::of(at),
        })
        .await?;
    let mut matches = Vec::new();
    for _ in 0..count {
        let record = records.try_next().await?.ok_or(StreamError::Unavailable)?;
        if record.sequence != frame.sequence {
            return Err(StreamError::InvalidResponse);
        }
        frame.sequence += 1;
        if pattern.is_empty()
            || record
                .value
                .as_slice()
                .windows(pattern.len())
                .any(|bytes| bytes == pattern)
        {
            matches.push(Match {
                path: frame.path.clone(),
                record,
            });
        }
    }
    Ok(matches)
}

async fn observe<P: StreamProvider + ?Sized>(
    provider: &P,
    at: Option<Revision>,
    path: Option<StreamPath>,
    children: Option<ChildrenPageRequest>,
) -> Result<Heads, StreamError> {
    let request = HeadsRequest {
        at,
        retained: Default::default(),
        durable_through: None,
        projection: HeadProjection::Tail,
        paths: path.into_iter().collect(),
        children,
        limit: 1,
        at_least: Default::default(),
    };
    let observed = provider.heads(request.clone()).await?;
    observed.validate_for(&request)?;
    Ok(observed)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
