//! Caller-funded provenance on the original subscriber registry.
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;
use tracing::{Id, Span};

trait Identity: Send + Sync {
    fn key(&self) -> &dyn Any;
    fn root(&self) -> &Id;
}

struct Admitted<K> {
    key: K,
    root: Id,
    _credit: OwnedSemaphorePermit,
}

impl<K: Copy + Send + Sync + 'static> Identity for Admitted<K> {
    fn key(&self) -> &dyn Any {
        &self.key
    }
    fn root(&self) -> &Id {
        &self.root
    }
}

/// Original admission identity shared with descendants and credited output.
/// Only the original operation owner supplies the key: use its exact opaque
/// authority, operation, generation and physical-owner IDs, never paths,
/// credentials or reconstructed descriptor identities. No ID is a metric label.
///
/// This token retains its caller's diagnostic credit, not the producer or its
/// authority. It neither completes work nor certifies physical coverage.
#[derive(Clone)]
pub struct Binding(Arc<dyn Identity>);

impl Binding {
    /// Attach once to the original span before starting its children. A nested
    /// admission can replace inherited provenance on its own new root. The key
    /// is fixed-size and cannot own a producer. One allocation is funded by the
    /// caller's permit; all copies share it through final diagnostic retirement.
    /// Operation roots reuse their existing collector storage. A non-operation
    /// root also needs the registry's ordinary extension storage for its token.
    pub fn attach<K: Copy + Send + Sync + 'static>(
        span: &Span,
        key: K,
        credit: OwnedSemaphorePermit,
    ) -> Result<Self, BindingError> {
        crate::operations::attach_binding(span, |root| {
            Self(Arc::new(Admitted {
                key,
                root,
                _credit: credit,
            }))
        })
    }

    /// Recover the original owner's exact typed IDs without decoding a field map.
    pub fn key<K: Copy + 'static>(&self) -> Option<&K> {
        self.0.key().downcast_ref()
    }

    /// Original root ID in its registry. Use the token, rather than this scalar,
    /// to distinguish bindings from different registries or recycled span IDs.
    pub fn root_id(&self) -> &Id {
        self.0.root()
    }

    /// Whether both handles came from the same original admission.
    pub fn same_operation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Join a sample only through its original propagated token. Missing
    /// provenance is false; neither descriptors nor parent IDs fill that gap.
    pub fn matches(&self, sample: &crate::Sample<'_>) -> bool {
        sample
            .binding
            .is_some_and(|other| self.same_operation(other))
    }
}

impl fmt::Debug for Binding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Binding").finish_non_exhaustive()
    }
}

/// The original span could not accept a new admission binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingError {
    /// The original span is disabled or has retired.
    SpanUnavailable,
    /// The subscriber has no shared tracing registry.
    RegistryUnavailable,
    /// This span already has its own explicit admission binding.
    AlreadyBound,
    /// The operation has already published its terminal sample.
    Completed,
}

impl fmt::Display for BindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SpanUnavailable => "binding requires the original live enabled span",
            Self::RegistryUnavailable => "binding requires the shared tracing registry",
            Self::AlreadyBound => "the original span already has an admission binding",
            Self::Completed => "bind the original operation before terminal completion",
        })
    }
}
impl std::error::Error for BindingError {}
