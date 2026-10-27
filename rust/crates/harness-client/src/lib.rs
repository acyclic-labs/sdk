//! Bounded view demand and provider-owned pages shared by client adapters.

mod connection;
pub use connection::{
    CREDENTIAL_ENV, ConnectionConfigError, ConnectionOptions, DEFAULT_ENDPOINT, ENDPOINT_ENV,
    resolve_connection_options,
};

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod control;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use control::{
    AgentBinding, AgentCall, AgentControls, AgentExchangeError, PreparedAgentInvocation,
};

#[cfg(any(feature = "grpc", feature = "wasm"))]
mod command;
#[cfg(any(feature = "grpc", feature = "wasm"))]
pub use command::{
    AgentCommand, AgentPageRequest, CommandError, MAX_COMMAND_BYTES as MAX_AGENT_COMMAND_BYTES,
    PreparedCommand,
};
#[cfg(any(feature = "grpc", feature = "wasm"))]
mod reply;
#[cfg(any(feature = "grpc", feature = "wasm"))]
pub use reply::{
    AgentControlFailure, AgentReply, ChildAgent, MAX_AGENT_REPLY_BYTES, PendingApproval,
    decode_agent_reply, encode_agent_reply,
};

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod agents;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use acyclic_harness_core::agent::{
    AgentId, AgentPath, AgentRecord, ConversationEntry, Delivered, Error as AgentError, IntentId,
    Phase, StatusRecord,
};
#[cfg(any(feature = "grpc", feature = "wasm"))]
pub use acyclic_harness_core::{
    interaction::{ApprovalBinding, Interaction},
    model::ModelContent,
};
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use agents::{AgentClient, AgentClientError, AgentEvent, AgentSelection};
mod demand;
pub use demand::{DemandError, DemandPolicy, ViewDemand};

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod session;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use session::{WireSession, WireSessionError};

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod stream_view;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use stream_view::{StreamView, StreamViewPolicy};

use std::{
    collections::{BTreeSet, VecDeque},
    sync::Arc,
};

/// The provider retains its complete page, including its cut and backing custody.
/// `retained_bytes` includes allocated entries, cursors and shared backing bytes;
/// adapters use their existing admission charge rather than estimating JSON size.
pub trait WindowPage {
    /// An entry owned by the page's backing allocation.
    type Item;
    /// Stable, unique identity within the query cut.
    type Key: Ord + ?Sized;
    /// Opaque exclusive edge, bound to this query and cut.
    type Cursor: Eq;
    /// Provider identity for the retained query cut.
    type Generation: Eq;
    /// Borrows the ordered entries without materializing another array.
    fn items(&self) -> &[Self::Item];
    /// Borrows an entry's stable identity.
    fn key(item: &Self::Item) -> &Self::Key;
    /// Exclusive anchor before the first entry.
    fn before(&self) -> &Arc<Self::Cursor>;
    /// Exclusive anchor after the last entry.
    fn after(&self) -> &Arc<Self::Cursor>;
    /// Identity shared by every page in one window.
    fn generation(&self) -> &Self::Generation;
    /// Whether the provider can return an earlier page at this cut.
    fn has_more_before(&self) -> bool;
    /// Whether the provider can return a later page at this cut.
    fn has_more_after(&self) -> bool;
    /// Complete admitted backing charge, retained until this page is dropped.
    fn retained_bytes(&self) -> u64;
}

/// Which exclusive edge to extend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageDirection {
    /// Fetch preceding entries.
    Before,
    /// Fetch following entries.
    After,
}

/// Hard ceilings prevent a caller from turning a page window into full history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageWindowPolicy {
    pages: usize,
    items: usize,
    bytes: u64,
}

impl PageWindowPolicy {
    /// Validate finite page, entry and backing-byte budgets.
    ///
    /// # Errors
    /// Returns `Capacity` for zero budgets or values above the hard ceilings.
    pub fn new(pages: usize, items: usize, bytes: u64) -> Result<Self, PageWindowError> {
        if pages == 0
            || pages > 64
            || items == 0
            || items > 4096
            || bytes == 0
            || bytes > 16 * 1024 * 1024
        {
            return Err(PageWindowError::Capacity);
        }
        Ok(Self {
            pages,
            items,
            bytes,
        })
    }
}

impl Default for PageWindowPolicy {
    fn default() -> Self {
        Self {
            pages: 4,
            items: 512,
            bytes: 4 * 1024 * 1024,
        }
    }
}

/// Typed refusal; rejected pages never change the current window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageWindowError {
    /// The requested policy or admitted page exceeds a finite budget.
    Capacity,
    /// The sole physical fetch has not settled.
    Busy,
    /// The response capability belongs to another window.
    ForeignRequest,
    /// The supplied response capability was already settled or never opened.
    SettledRequest,
    /// A reset fenced the settled response.
    StaleResponse,
    /// The page belongs to another query generation.
    GenerationChanged,
    /// Exclusive provider pages repeat an entry identity.
    DuplicateKey,
    /// A nonterminal page leaves its requested edge unchanged.
    NoProgress,
    /// The revision counter cannot advance without wrapping.
    Exhausted,
}

impl std::fmt::Display for PageWindowError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(match self {
            Self::Capacity => "page window capacity exceeded",
            Self::Busy => "page fetch has not settled",
            Self::ForeignRequest => "page request belongs to another window",
            Self::SettledRequest => "page request is absent or already settled",
            Self::StaleResponse => "page response predates the reset",
            Self::GenerationChanged => "page generation changed",
            Self::DuplicateKey => "exclusive pages contain a duplicate key",
            Self::NoProgress => "nonterminal page did not advance its cursor",
            Self::Exhausted => "page window revision exhausted",
        })
    }
}

impl std::error::Error for PageWindowError {}

/// A linear response capability. The I/O owner retains this until its fetch has
/// physically settled, including cancellation; reset does not release custody.
pub struct PageRequest<C> {
    owner: Arc<()>,
    revision: u64,
    direction: PageDirection,
    cursor: Option<Arc<C>>,
}

impl<C> PageRequest<C> {
    /// Edge selected by the one mutation owner.
    #[must_use]
    pub fn direction(&self) -> PageDirection {
        self.direction
    }
    /// Exact exclusive provider cursor; absent for initial discovery.
    #[must_use]
    pub fn cursor(&self) -> Option<&C> {
        self.cursor.as_deref()
    }
}

/// Pure paging state. The session owns I/O and credit; this owns admitted pages.
/// Unchanged reads borrow entries and allocate nothing. Mutation visits only the
/// finite retained window. No entry, cursor or payload is copied on eviction.
pub struct PageWindow<P: WindowPage> {
    policy: PageWindowPolicy,
    pages: VecDeque<P>,
    items: usize,
    bytes: u64,
    before: bool,
    after: bool,
    revision: u64,
    in_flight: bool,
    identity: Arc<()>,
}

impl<P: WindowPage> PageWindow<P> {
    /// Construct an empty window under an already validated policy.
    #[must_use]
    pub fn new(policy: PageWindowPolicy) -> Self {
        Self {
            policy,
            pages: VecDeque::with_capacity(policy.pages),
            items: 0,
            bytes: 0,
            before: true,
            after: true,
            revision: 0,
            in_flight: false,
            identity: Arc::new(()),
        }
    }

    /// Iterate retained entries by reference, without copying the window.
    pub fn items(&self) -> impl Iterator<Item = &P::Item> {
        self.pages.iter().flat_map(|page| page.items())
    }

    /// Number of retained entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items
    }
    /// Whether no entries are currently retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items == 0
    }
    /// Number of complete provider pages retaining custody.
    #[must_use]
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }
    /// Backing-byte charges of those pages; session/queue overhead is separate.
    #[must_use]
    pub fn retained_page_bytes(&self) -> u64 {
        self.bytes
    }
    /// Cache exported immutable arrays by this revision at the WASM boundary.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }
    /// Whether the selected edge has undisplayed entries at this cut.
    #[must_use]
    pub fn has_more(&self, direction: PageDirection) -> bool {
        match direction {
            PageDirection::Before => self.before,
            PageDirection::After => self.after,
        }
    }

    fn validate(&self, page: &P, include_current: bool) -> Result<(), PageWindowError> {
        if page.items().len() > self.policy.items || page.retained_bytes() > self.policy.bytes {
            return Err(PageWindowError::Capacity);
        }
        let mut keys = BTreeSet::new();
        if include_current {
            for item in self.items() {
                keys.insert(P::key(item));
            }
        }
        for item in page.items() {
            if !keys.insert(P::key(item)) {
                return Err(PageWindowError::DuplicateKey);
            }
        }
        Ok(())
    }

    /// Replace atomically, fencing the outstanding response without admitting a
    /// second producer. The old fetch must settle before begin can succeed again.
    ///
    /// # Errors
    /// Refuses an oversized/duplicate page or exhausted revision counter.
    pub fn reset(&mut self, page: P) -> Result<(), PageWindowError> {
        self.validate(&page, false)?;
        let next = self
            .revision
            .checked_add(1)
            .ok_or(PageWindowError::Exhausted)?;
        self.items = page.items().len();
        self.bytes = page.retained_bytes();
        self.before = page.has_more_before();
        self.after = page.has_more_after();
        self.pages.clear();
        self.pages.push_back(page);
        self.revision = next;
        Ok(())
    }

    /// Acquire the sole response capability, or return `None` at a closed edge.
    ///
    /// # Errors
    /// Returns `Busy` while the previous fetch still owns its capability.
    pub fn begin(
        &mut self,
        direction: PageDirection,
    ) -> Result<Option<PageRequest<P::Cursor>>, PageWindowError> {
        if self.in_flight {
            return Err(PageWindowError::Busy);
        }
        if !self.has_more(direction) {
            return Ok(None);
        }
        let cursor = match direction {
            PageDirection::Before => self.pages.front().map(|page| Arc::clone(page.before())),
            PageDirection::After => self.pages.back().map(|page| Arc::clone(page.after())),
        };
        self.in_flight = true;
        Ok(Some(PageRequest {
            owner: Arc::clone(&self.identity),
            revision: self.revision,
            direction,
            cursor,
        }))
    }

    /// Call on a settled I/O error or physically drained cancellation too. This
    /// consumes the linear request without changing pages or their revision.
    ///
    /// # Errors
    /// Refuses foreign/stale requests, invalid provider pages and revision wrap.
    pub fn finish(
        &mut self,
        slot: &mut Option<PageRequest<P::Cursor>>,
        page: Option<P>,
    ) -> Result<(), PageWindowError> {
        let request = match slot.take() {
            Some(request) if Arc::ptr_eq(&self.identity, &request.owner) => request,
            Some(foreign) => {
                *slot = Some(foreign);
                return Err(PageWindowError::ForeignRequest);
            }
            None => return Err(PageWindowError::SettledRequest),
        };
        self.in_flight = false;
        if request.revision != self.revision {
            return Err(PageWindowError::StaleResponse);
        }
        let Some(page) = page else {
            return Ok(());
        };
        self.validate(&page, true)?;
        if self
            .pages
            .front()
            .is_some_and(|old| old.generation() != page.generation())
        {
            return Err(PageWindowError::GenerationChanged);
        }
        let (edge, more) = match request.direction {
            PageDirection::Before => (page.before(), page.has_more_before()),
            PageDirection::After => (page.after(), page.has_more_after()),
        };
        if more && request.cursor.as_ref().is_some_and(|cursor| cursor == edge) {
            return Err(PageWindowError::NoProgress);
        }
        let next = self
            .revision
            .checked_add(1)
            .ok_or(PageWindowError::Exhausted)?;
        // An empty terminal response closes the queried edge without displacing
        // an admitted data page. Its cursor is never needed at the closed edge.
        if page.items().is_empty() && !more && !self.pages.is_empty() {
            match request.direction {
                PageDirection::Before => self.before = false,
                PageDirection::After => self.after = false,
            }
            self.revision = next;
            return Ok(());
        }
        let incoming_items = page.items().len();
        let incoming_bytes = page.retained_bytes();
        // Evict before inserting, so the deque never grows past its page cap.
        let mut evicted = false;
        while self.pages.len() >= self.policy.pages
            || self.items > self.policy.items - incoming_items
            || self.bytes > self.policy.bytes - incoming_bytes
        {
            let old = match request.direction {
                PageDirection::Before => self.pages.pop_back(),
                PageDirection::After => self.pages.pop_front(),
            };
            let Some(old) = old else {
                return Err(PageWindowError::Capacity);
            };
            self.items -= old.items().len();
            self.bytes -= old.retained_bytes();
            evicted = true;
        }
        if self.pages.is_empty() {
            self.before = page.has_more_before();
            self.after = page.has_more_after();
        }
        match request.direction {
            PageDirection::Before => {
                self.before = more;
                if evicted {
                    self.after = true;
                }
                self.pages.push_front(page);
            }
            PageDirection::After => {
                self.after = more;
                if evicted {
                    self.before = true;
                }
                self.pages.push_back(page);
            }
        }
        self.items += incoming_items;
        self.bytes += incoming_bytes;
        self.revision = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
