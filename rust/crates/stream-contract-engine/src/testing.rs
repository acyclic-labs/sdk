//! Public request accounting shared by primitive conformance backends.
//!
//! Counts describe calls at the wrapped provider boundary, including failed
//! calls. They do not infer physical HTTP requests hidden inside a backend.
//! Backend adapters should instrument that boundary separately when qualifying
//! transport traffic. Clones share a ledger; use fresh wrappers for concurrent
//! workloads whose budgets must be measured independently.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

mod provider;
mod sample;
pub use sample::{Sample, SampleStart};

/// Request classes used by every primitive's conformance budgets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum RequestClass {
    /// Payload reads, follows and mutations.
    Data,
    /// Metadata and placement inspection.
    Metadata,
    /// Namespace enumeration and pagination.
    List,
    /// Existence, tail and bounds probes.
    Head,
    /// Ownership or lease management.
    Lease,
    /// Credential acquisition or refresh.
    Token,
}

impl RequestClass {
    /// A heads request's class: a pure child page enumerates a namespace,
    /// every other heads request probes existence and tails.
    pub fn of_heads(request: &crate::HeadsRequest) -> Self {
        if request.paths.is_empty() && request.children.is_some() {
            Self::List
        } else {
            Self::Head
        }
    }
}

/// An immutable ledger snapshot, or the difference between two snapshots.
/// A component equal to `u64::MAX` has lost precision and stays unknown in
/// subsequent deltas. These values never certify hidden physical work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Requests([u64; 6]);

impl Requests {
    fn count(self, class: RequestClass) -> u64 {
        let [data, metadata, list, head, lease, token] = self.0;
        match class {
            RequestClass::Data => data,
            RequestClass::Metadata => metadata,
            RequestClass::List => list,
            RequestClass::Head => head,
            RequestClass::Lease => lease,
            RequestClass::Token => token,
        }
    }

    /// Calls that carry payload data; `u64::MAX` denotes lost precision.
    pub fn data(self) -> u64 {
        self.count(RequestClass::Data)
    }

    /// Every non-payload request, irrespective of what the caller requested.
    /// For requested HEAD/LIST semantics use [`Self::auxiliary`] instead.
    /// `u64::MAX` means the aggregate is unknown, never a wrapped zero.
    pub fn control(self) -> u64 {
        self.auxiliary(Some(RequestClass::Data)).unwrap_or(u64::MAX)
    }

    /// Calls outside the original operation's explicitly requested class.
    /// `None` means missing requested semantics or lost counter precision.
    /// The requested component remains in [`Self::class`]: surplus calls of
    /// that same class still violate its budget and cannot disappear here.
    /// This partitions this provider boundary, not unobserved transport work.
    pub fn auxiliary(self, requested: Option<RequestClass>) -> Option<u64> {
        let requested = requested? as usize;
        self.0
            .iter()
            .enumerate()
            .try_fold(0_u64, |sum, (class, &count)| {
                if count == u64::MAX {
                    None
                } else if class == requested {
                    Some(sum)
                } else {
                    sum.checked_add(count)
                }
            })
    }

    /// Calls in one specific class; `u64::MAX` denotes lost precision.
    pub fn class(self, class: RequestClass) -> u64 {
        self.count(class)
    }

    /// Requests issued since an earlier snapshot of this same ledger.
    ///
    /// # Panics
    /// Panics if any counter precedes the earlier snapshot. Take both snapshots
    /// in order from the same ledger. Lost precision propagates through deltas.
    pub fn since(self, earlier: Self) -> Self {
        let mut delta = self;
        for (current, previous) in delta.0.iter_mut().zip(earlier.0) {
            if *current == u64::MAX || previous == u64::MAX {
                *current = u64::MAX;
                continue;
            }
            assert!(
                *current >= previous,
                "request ledger moved backwards; take both snapshots from the same wrapper"
            );
            *current -= previous;
        }
        delta
    }
}

/// A clonable ledger handle retained when a counted provider is type erased.
#[derive(Clone)]
pub struct RequestLedger(pub Arc<[AtomicU64; 6]>);

impl RequestLedger {
    /// Starts an independent empty ledger.
    pub fn new() -> Self {
        Self(Arc::new(std::array::from_fn(|_| AtomicU64::new(0))))
    }

    /// Records one dispatched call in its contract class.
    pub fn record(&self, class: RequestClass) {
        let [data, metadata, list, head, lease, token] = self.0.as_ref();
        let counter = match class {
            RequestClass::Data => data,
            RequestClass::Metadata => metadata,
            RequestClass::List => list,
            RequestClass::Head => head,
            RequestClass::Lease => lease,
            RequestClass::Token => token,
        };
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            count.checked_add(1)
        });
    }

    /// Reads the ledger without resetting other handles.
    pub fn requests(&self) -> Requests {
        let [data, metadata, list, head, lease, token] = self.0.as_ref();
        Requests([
            data.load(Ordering::Relaxed),
            metadata.load(Ordering::Relaxed),
            list.load(Ordering::Relaxed),
            head.load(Ordering::Relaxed),
            lease.load(Ordering::Relaxed),
            token.load(Ordering::Relaxed),
        ])
    }
}

impl Default for RequestLedger {
    fn default() -> Self {
        Self::new()
    }
}

/// A provider and its request ledger. Primitive adapters implement their public
/// provider trait on this same wrapper, preserving the provider's error types.
pub struct Counted<P> {
    inner: P,
    requests: RequestLedger,
}

impl<P> Counted<P> {
    /// Starts an independent ledger around a provider.
    pub fn new(inner: P) -> Self {
        Self::with_ledger(inner, RequestLedger::new())
    }

    /// Composes several primitive providers into one measured dispatch boundary.
    /// Keep unrelated or concurrent workloads on independent ledgers.
    pub fn with_ledger(inner: P, requests: RequestLedger) -> Self {
        Self { inner, requests }
    }

    /// The wrapped provider, for implementing a forwarding adapter.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// Records one request at the backend's actual dispatch boundary.
    pub fn record(&self, class: RequestClass) {
        self.requests.record(class);
    }

    /// Reads this ledger without resetting counters shared by other handles.
    pub fn requests(&self) -> Requests {
        self.requests.requests()
    }

    /// Retains accounting after moving this provider into a trait object.
    pub fn ledger(&self) -> RequestLedger {
        self.requests.clone()
    }
}

impl<P: Clone> Clone for Counted<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            requests: self.requests.clone(),
        }
    }
}

/// Evaluates an operation once and checks its request delta, returning its result.
/// Await async operations inside the expression:
/// `assert_requests!(provider, provider.append(request).await, control = 0, data <= 1)`.
#[macro_export]
macro_rules! assert_requests {
    ($provider:expr, $operation:expr, $($class:ident $comparison:tt $limit:expr),+ $(,)?) => {{
        let ledger = &$provider;
        let before = ledger.requests();
        let result = $operation;
        let requests = ledger.requests().since(before);
        $($crate::assert_requests!(@check requests, $class $comparison $limit);)+
        result
    }};
    (@check $requests:ident, $class:ident = $limit:expr) => {
        assert_eq!($requests.$class(), $limit,
            "request budget exceeded for {}: {:?}", stringify!($class), $requests);
    };
    (@check $requests:ident, $class:ident <= $limit:expr) => {
        assert!($requests.$class() <= $limit,
            "request budget exceeded for {}: {:?}", stringify!($class), $requests);
    };
}
