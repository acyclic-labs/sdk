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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Requests([u64; 6]);

impl Requests {
    /// Calls that carry payload data.
    pub fn data(self) -> u64 {
        self.0[RequestClass::Data as usize]
    }

    /// Every metadata, list, head, lease or token request.
    pub fn control(self) -> u64 {
        self.0[1..].iter().sum()
    }

    /// Calls in one specific class.
    pub fn class(self, class: RequestClass) -> u64 {
        self.0[class as usize]
    }

    /// Requests issued since an earlier snapshot of this same ledger.
    pub fn since(self, earlier: Self) -> Self {
        Self(std::array::from_fn(|index| {
            self.0[index]
                .checked_sub(earlier.0[index])
                .expect("request ledger moved backwards; take both snapshots from the same wrapper")
        }))
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
        self.0[class as usize].fetch_add(1, Ordering::Relaxed);
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
            StreamProvider,
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
        assert_eq!(
            crate::assert_requests!(ledger, provider.tail(absent).await, control = 1, data = 0),
            Err(StreamError::NotFound)
        );
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
