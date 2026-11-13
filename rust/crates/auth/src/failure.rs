//! Clone-safe ownership of an original local error, independent of its public category.
use std::{error::Error, sync::Arc};

/// A category together with its original concrete local cause.
///
/// Clones share the cause without requiring it to implement `Clone`. Equality
/// means the same retained failure identity, not equal rendered error messages.
#[derive(Debug)]
pub struct Failure<C>(Arc<Retained<C>>);

#[derive(Debug)]
struct Retained<C> {
    category: C,
    source: Box<dyn Error + Send + Sync>,
}

impl<C> Failure<C> {
    /// Retain the original error without rendering or replacing it.
    #[must_use]
    pub fn new(category: C, source: impl Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(Retained {
            category,
            source: Box::new(source),
        }))
    }

    /// The selected category or context; diagnostic ownership does not change it.
    #[must_use]
    pub fn category(&self) -> &C {
        &self.0.category
    }
}

impl<C> Clone for Failure<C> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<C> PartialEq for Failure<C> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl<C> Eq for Failure<C> {}

impl<C: std::fmt::Display> std::fmt::Display for Failure<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0.category, self.0.source)
    }
}

impl<C: std::fmt::Debug + std::fmt::Display> Error for Failure<C> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.source.as_ref())
    }
}
