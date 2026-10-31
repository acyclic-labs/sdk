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

/// An immutable ledger snapshot, or the difference between two snapshots.
/// A component equal to `u64::MAX` has lost precision and stays unknown in
/// subsequent deltas. These values never certify hidden physical work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Requests([u64; 6]);

impl Requests {
    /// Calls that carry payload data; `u64::MAX` denotes lost precision.
    pub fn data(self) -> u64 {
        self.0[RequestClass::Data as usize]
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
        self.0[class as usize]
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
pub struct RequestLedger(Arc<[AtomicU64; 6]>);

impl RequestLedger {
    /// Starts an independent empty ledger.
    pub fn new() -> Self {
        Self(Arc::new(std::array::from_fn(|_| AtomicU64::new(0))))
    }

    /// Records one dispatched call in its contract class.
    pub fn record(&self, class: RequestClass) {
        let _ =
            self.0[class as usize].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_add(1)
            });
    }

    /// Reads the ledger without resetting other handles.
    pub fn requests(&self) -> Requests {
        Requests(std::array::from_fn(|index| {
            self.0[index].load(Ordering::Relaxed)
        }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn erased_provider_preserves_records_failures_and_request_budgets() {
        use crate::{
            AppendOutcome, AppendRequest, MemoryStream, ReadRequest, StreamError, StreamPath,
            StreamProvider, StreamProviderExt as _,
        };
        use futures::TryStreamExt as _;

        let counted = Counted::new(MemoryStream::default());
        let ledger = counted.ledger();
        let provider: Box<dyn StreamProvider> = Box::new(counted);
        let path = StreamPath::new("request-budget").unwrap();
        let result = crate::assert_requests!(
            ledger,
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![crate::OwnedBytes::from_static(b"value").into()],
                    if_tail: Some(0),
                    idempotency_key: crate::IdempotencyKey::new(crate::OwnedBytes::from_static(
                        b"request-budget"
                    ),)
                    .unwrap(),
                })
                .await,
            control = 0,
            data <= 1
        )
        .unwrap();
        assert!(matches!(result, AppendOutcome::Committed(_)));
        let records = crate::assert_requests!(
            ledger,
            async {
                provider
                    .read(ReadRequest {
                        key: None,
                        path,
                        from: 0,
                        limit: 2,
                        at_least: Default::default(),
                    })
                    .await?
                    .try_collect::<Vec<_>>()
                    .await
            }
            .await,
            control = 0,
            data <= 1
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref(), b"value");
        let absent = StreamPath::new("absent").unwrap();
        let before = ledger.requests();
        assert_eq!(
            crate::assert_requests!(ledger, provider.tail(absent).await, control = 1, data = 0),
            Err(StreamError::NotFound)
        );
        let requested = ledger.requests().since(before);
        assert_eq!(requested.class(RequestClass::Head), 1);
        assert_eq!(requested.auxiliary(Some(RequestClass::Head)), Some(0));

        // Listing is the requested data operation here. Its independent result
        // oracle stays intact; a method-class name cannot turn it into a probe.
        let before = ledger.requests();
        let list = || crate::ChildrenPageRequest {
            tail_stream: None,
            parent: None,
            limit: 8,
            after: None,
            hierarchy_version: None,
        };
        let page = provider.children_page(list()).await.unwrap();
        assert_eq!(page.children.len(), 1);
        assert_eq!(
            page.children[0].path,
            StreamPath::new("request-budget").unwrap()
        );
        let requested = ledger.requests().since(before);
        assert_eq!(requested.class(RequestClass::List), 1);
        assert_eq!(requested.auxiliary(Some(RequestClass::List)), Some(0));
        assert_eq!(requested.auxiliary(None), None);
        assert_eq!(
            requested.control(),
            1,
            "raw method classification is not requested semantics"
        );

        // Actual extra provider calls must remain visible, including one of
        // the same requested class and a failed auxiliary HEAD.
        provider.children_page(list()).await.unwrap();
        assert_eq!(
            provider.tail(StreamPath::new("absent").unwrap()).await,
            Err(StreamError::NotFound)
        );
        let extra = ledger.requests().since(before);
        assert_eq!(extra.class(RequestClass::List), 2);
        assert_eq!(extra.auxiliary(Some(RequestClass::List)), Some(1));

        // Bounded arithmetic state control, not a fabricated native fixture.
        let overflow = RequestLedger::new();
        overflow.0[RequestClass::Token as usize].store(u64::MAX - 1, Ordering::Relaxed);
        let before = overflow.requests();
        overflow.record(RequestClass::Token);
        let full = overflow.requests();
        overflow.record(RequestClass::Token);
        assert_eq!(overflow.requests().class(RequestClass::Token), u64::MAX);
        assert_eq!(full.since(before).auxiliary(Some(RequestClass::Data)), None);
        assert_eq!(
            overflow
                .requests()
                .since(full)
                .auxiliary(Some(RequestClass::Data)),
            None
        );
        assert_ne!(overflow.requests().since(full).control(), 0);
    }

    #[test]
    fn budgets_use_the_operation_delta_and_return_its_result() {
        let provider = Counted::new(());
        provider.record(RequestClass::Token);
        let value = crate::assert_requests!(
            provider,
            {
                provider.record(RequestClass::Data);
                42
            },
            control = 0,
            data <= 1
        );
        assert_eq!(value, 42);
        assert_eq!(provider.requests().class(RequestClass::Token), 1);
    }

    #[test]
    fn cloned_handles_share_all_request_classes() {
        let provider = Counted::new(());
        let clone = provider.clone();
        for class in [
            RequestClass::Metadata,
            RequestClass::List,
            RequestClass::Head,
            RequestClass::Lease,
            RequestClass::Token,
        ] {
            clone.record(class);
            assert_eq!(provider.requests().class(class), 1);
        }
        assert_eq!(provider.requests().control(), 5);
    }

    #[test]
    #[should_panic(expected = "request budget exceeded for control")]
    fn negative_control_rejects_a_hidden_head_request() {
        let provider = Counted::new(());
        crate::assert_requests!(
            provider,
            {
                provider.record(RequestClass::Data);
                provider.record(RequestClass::Head);
            },
            control = 0,
            data <= 1
        );
    }
}
