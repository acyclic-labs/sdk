//! Trace admission reuses the primitive's existing subscription registry.
use super::{Observed, Recorder, Request};
use crate::{
    Stream, StreamError, StreamPath, StreamProvider,
    interest::{Interest, Interests},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct State<P: ?Sized> {
    provider: Arc<P>,
    entries: Interests<StreamPath, Arc<Recorder<P>>>,
    leases: AtomicUsize,
}

/// Host-shared admitted trace interest, proportional only to followed sources.
/// No agent entry or producer clock exists until its first subscription.
pub struct Registry<P: ?Sized>(Arc<State<P>>);

impl<P: ?Sized> Clone for Registry<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("trace subscription count exceeds its representation")]
struct LeaseOverflow;

#[derive(Debug, thiserror::Error)]
#[error("trace interest owner already has a different original source")]
struct SourceMismatch;

impl<P: StreamProvider + ?Sized> Registry<P> {
    /// One registry for the host's original provider, never one per agent.
    pub fn new(provider: Arc<P>) -> Self {
        Self(Arc::new(State {
            provider,
            entries: Interests::new(),
            leases: AtomicUsize::new(0),
        }))
    }

    /// Admit the original follow lifetime. Overlapping subscriptions share the
    /// same recorder, clock domain and span sequence through the last drop.
    pub fn subscribe(
        &self,
        owner: StreamPath,
        source: StreamPath,
    ) -> Result<Lease<P>, StreamError> {
        let entry = self.0.entries.subscribe_with(owner, || {
            Arc::new(Recorder::new(
                Stream {
                    provider: self.0.provider.clone(),
                    path: source.clone(),
                    fork_receipt: None,
                },
                crate::new_identity(),
            ))
        })?;
        if entry.value().stream.path() != &source {
            return Err(StreamError::InvalidArgument.with_source(SourceMismatch));
        }
        self.0
            .leases
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| StreamError::Capacity.with_source(LeaseOverflow))?;
        Ok(Lease {
            registry: self.clone(),
            _entry: entry,
        })
    }

    /// Select already-admitted interest; this grants no access and opens no
    /// subscription. With no followers this is one atomic load, without a map
    /// lookup, allocation, clock read or IO.
    pub fn recorder(&self, owner: &str) -> Result<Option<Arc<Recorder<P>>>, StreamError> {
        if self.0.leases.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }
        self.0.entries.get(owner)
    }

    /// Select demand at the original request boundary. A failed diagnostic
    /// lookup cannot replace the operation's original output or cause.
    pub async fn observe<T, E>(
        &self,
        owner: &str,
        request: Request,
        work: impl std::future::Future<Output = Result<T, E>>,
        bytes: impl FnOnce(&T) -> Option<u64>,
    ) -> Observed<T, E, P> {
        match self.recorder(owner) {
            Ok(recorder) => Recorder::observe(recorder.as_ref(), request, work, bytes).await,
            Err(error) => Observed {
                result: work.await,
                observation: Err(error),
            },
        }
    }

    /// Same original demand, with no diagnostic wait after dispatch admission.
    pub async fn handoff<T, E>(
        &self,
        owner: &str,
        request: Request,
        work: impl std::future::Future<Output = Result<T, E>>,
    ) -> Observed<T, E, P> {
        match self.recorder(owner) {
            Ok(recorder) => Recorder::handoff(recorder.as_ref(), request, work).await,
            Err(error) => Observed {
                result: work.await,
                observation: Err(error),
            },
        }
    }
}

/// Original subscription custody. Dropping the last lease retires the existing
/// registry entry; already-admitted spans keep their own completion ownership.
pub struct Lease<P: ?Sized> {
    registry: Registry<P>,
    _entry: Interest<StreamPath, Arc<Recorder<P>>>,
}

impl<P: ?Sized> Drop for Lease<P> {
    fn drop(&mut self) {
        self.registry.0.leases.fetch_sub(1, Ordering::AcqRel);
    }
}
