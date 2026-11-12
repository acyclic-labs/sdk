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
    content: Option<Vec<u8>>,
}

/// Immutable continuation for one literal subtree search. Cloning/replaying a
/// cursor does not consume results. Its memory grows with depth, never history
/// length or the number of agents. A cursor is not an authorization capability.
#[derive(Clone, Debug)]
pub struct Cursor {
    pattern: Vec<u8>,
    at: Option<Revision>,
    frontier: Vec<Frame>,
    matcher: Option<[u8; 32]>,
}

#[cfg(feature = "serde")]
mod codec;

/// An addressable original record, without a second copy of its provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Match<T = ()> {
    /// Selected path, which can differ from the inherited record's origin.
    pub path: StreamPath,
    /// Original sequence, publication, origin and bytes.
    pub record: Record,
    /// Exact linked source selected by the matcher, absent for record bytes.
    pub source: Option<T>,
}

/// Results through one bounded work frontier. An empty page with `next` is
/// progress, not exhaustion; a sparse query never scans to its next match.
#[derive(Clone, Debug)]
pub struct Page<T = ()> {
    /// Matches in depth-first path order, then ascending record sequence.
    pub matches: Vec<Match<T>>,
    /// Record inspections plus namespace observations charged to this page.
    pub work: u32,
    /// Continuation at the original cut, absent only when traversal is complete.
    pub next: Option<Cursor>,
}

/// One bounded inspection of an original record or its linked content.
pub struct Step<T> {
    /// Whether the literal occurred in this original source.
    pub matched: bool,
    /// Exact linked source identity, when the hit was outside the record bytes.
    pub source: Option<T>,
    /// Opaque progress within the same record; never an index of derived data.
    pub next: Option<Vec<u8>>,
}

/// Shared adapters can resolve original linked files without a second traversal
/// or index. Each step must bound its reads and retain source authorization.
pub trait Matcher: crate::ProviderPlatform {
    /// Address callers can open through the original content reader.
    type Source: crate::ProviderTask;
    /// Original typed source failure.
    type Error: From<StreamError> + crate::ProviderTask;
    /// Versioned matching semantics, bound into every continuation.
    fn identity(&self) -> [u8; 32];
    /// One bounded step. A continuation must advance; a match ends this record.
    fn step<'a>(
        &'a self,
        record: &'a Record,
        pattern: &'a [u8],
        after: Option<&'a [u8]>,
    ) -> impl std::future::Future<Output = Result<Step<Self::Source>, Self::Error>>
    + crate::ProviderTask
    + 'a;
}

/// The same literal byte predicate used by record and linked-content adapters.
pub fn contains(bytes: &[u8], pattern: &[u8]) -> bool {
    find(bytes, pattern).is_some()
}

/// First literal occurrence, shared by record and file-range adapters.
pub fn find(bytes: &[u8], pattern: &[u8]) -> Option<usize> {
    if pattern.is_empty() {
        Some(0)
    } else {
        bytes
            .windows(pattern.len())
            .position(|bytes| bytes == pattern)
    }
}

struct Raw;
impl Matcher for Raw {
    type Source = ();
    type Error = StreamError;
    fn identity(&self) -> [u8; 32] {
        [0; 32]
    }
    fn step<'a>(
        &'a self,
        record: &'a Record,
        pattern: &'a [u8],
        _: Option<&'a [u8]>,
    ) -> impl std::future::Future<Output = Result<Step<()>, StreamError>> + crate::ProviderTask + 'a
    {
        std::future::ready(Ok(Step {
            matched: contains(record.value.as_slice(), pattern),
            source: None,
            next: None,
        }))
    }
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
            matcher: None,
            frontier: vec![Frame {
                path: root,
                sequence: 0,
                tail: None,
                after: None,
                content: None,
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
        self.page_with(provider, budget, &Raw).await
    }

    /// The same cursor and paging mechanism over authorized linked content.
    /// A step counts against the caller's budget even when it yields no hit.
    pub async fn page_with<P: StreamProvider + ?Sized, M: Matcher>(
        &self,
        provider: &P,
        budget: NonZeroU32,
        matcher: &M,
    ) -> Result<Page<M::Source>, M::Error> {
        if self
            .matcher
            .is_some_and(|identity| identity != matcher.identity())
        {
            return Err(StreamError::InvalidArgument.into());
        }
        let mut cursor = self.clone();
        cursor.matcher = Some(matcher.identity());
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
                let count = (budget.get() - work).min(crate::MAX_ITEMS as u32);
                let at = cursor.at.clone().ok_or(StreamError::InvalidResponse)?;
                let batch = scan(provider, frame, at, &cursor.pattern, count, matcher).await?;
                matches.extend(batch.matches);
                work += batch.work;
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
                    content: None,
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

struct Scan<T> {
    matches: Vec<Match<T>>,
    work: u32,
}

async fn scan<P: StreamProvider + ?Sized, M: Matcher>(
    provider: &P,
    frame: &mut Frame,
    at: Revision,
    pattern: &[u8],
    count: u32,
    matcher: &M,
) -> Result<Scan<M::Source>, M::Error> {
    let tail = frame.tail.ok_or(StreamError::InvalidResponse)?;
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: frame.path.clone(),
            from: frame.sequence,
            limit: u64::from(count).min(tail - frame.sequence) as u32,
            at_least: RevisionCut::of(at),
        })
        .await?;
    let mut matches = Vec::new();
    let mut work = 0;
    while work < count && frame.sequence < tail {
        let record = records.try_next().await?.ok_or(StreamError::Unavailable)?;
        if record.sequence != frame.sequence {
            return Err(StreamError::InvalidResponse.into());
        }
        loop {
            let step = matcher
                .step(&record, pattern, frame.content.as_deref())
                .await?;
            work += 1;
            if step.matched {
                matches.push(Match {
                    path: frame.path.clone(),
                    record,
                    source: step.source,
                });
                frame.sequence += 1;
                frame.content = None;
                break;
            }
            if step.next.is_some() && step.next == frame.content {
                return Err(StreamError::InvalidResponse.into());
            }
            frame.content = step.next;
            if frame.content.is_none() {
                frame.sequence += 1;
                break;
            }
            if work == count {
                return Ok(Scan { matches, work });
            }
        }
    }
    Ok(Scan { matches, work })
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
