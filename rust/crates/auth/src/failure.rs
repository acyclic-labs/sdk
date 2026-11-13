//! Clone-safe ownership of an original local error, independent of its public category.
use std::{error::Error, sync::Arc};

/// A category together with its original concrete local cause.
///
/// Clones share the cause without requiring it to implement `Clone`. Equality
/// compares classification and original cause identity, never rendered text.
#[derive(Clone, Debug)]
pub struct Failure<C> {
    classification: Box<C>,
    source: Arc<dyn Error + Send + Sync>,
}

impl<C> Failure<C> {
    /// Retain the original error without rendering or replacing it.
    #[must_use]
    pub fn new(category: C, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            classification: Box::new(category),
            source: Arc::new(source),
        }
    }

    /// Share an already erased source without another error wrapper. Converting
    /// its Box to Arc preserves the concrete vtable and downcasting target.
    #[must_use]
    pub fn from_boxed(category: C, source: Box<dyn Error + Send + Sync + 'static>) -> Self {
        Self {
            classification: Box::new(category),
            source: Arc::from(source),
        }
    }

    /// The selected category or context; diagnostic ownership does not change it.
    #[must_use]
    pub const fn classification(&self) -> &C {
        &self.classification
    }
}

impl<C: PartialEq> PartialEq for Failure<C> {
    fn eq(&self, other: &Self) -> bool {
        self.classification == other.classification && Arc::ptr_eq(&self.source, &other.source)
    }
}
impl<C: Eq> Eq for Failure<C> {}

impl<C: std::fmt::Display> std::fmt::Display for Failure<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.classification, self.source)
    }
}

impl<C: std::fmt::Debug + std::fmt::Display> Error for Failure<C> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}
