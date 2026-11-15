//! Shared links whose auto traits are named instead of derived.
//!
//! The original-work graph is cyclic: a work context links views and allowances, each of
//! those holds an allocation that holds a work context again, and the demand ledger under
//! every context points back up at the view and allowance it is scoped to. Deriving `Send`
//! or `Sync` for any owner of a ledger walked those back-edges into the whole graph again,
//! and with the owners a crate composes on top the walk overflowed (E0275). A [`Link`] or
//! [`WeakLink`] holds its target behind a `dyn` whose bound names `Send + Sync`, so the
//! query stops at the link. The target's own auto traits are still checked, once, where the
//! link is made, and never re-derived through the graph.
use std::sync::{Arc, Weak};

/// What a link erases its target to: the target, with `Send + Sync` named.
pub(crate) trait Linked<T>: Send + Sync {
    fn target(&self) -> &T;
    fn into_arc(self: Arc<Self>) -> Arc<T>;
}
impl<T: Send + Sync> Linked<T> for T {
    #[inline]
    fn target(&self) -> &T {
        self
    }
    #[inline]
    fn into_arc(self: Arc<Self>) -> Arc<T> {
        self
    }
}

/// A shared owner of `T` that is `Send + Sync` by its named bound.
pub(crate) struct Link<T: 'static>(Arc<dyn Linked<T>>);
impl<T: Send + Sync + 'static> From<Arc<T>> for Link<T> {
    #[inline]
    fn from(target: Arc<T>) -> Self {
        Self(target)
    }
}
impl<T: std::fmt::Debug + 'static> std::fmt::Debug for Link<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        T::fmt(self, formatter)
    }
}
impl<T: 'static> std::ops::Deref for Link<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        <dyn Linked<T> as Linked<T>>::target(&*self.0)
    }
}

/// A non-owning [`Link`]: a `Weak` whose target's auto traits are named the same way.
pub(crate) struct WeakLink<T: 'static>(Weak<dyn Linked<T>>);
impl<T: Send + Sync + 'static> From<Weak<T>> for WeakLink<T> {
    #[inline]
    fn from(target: Weak<T>) -> Self {
        Self(target)
    }
}
impl<T: 'static> WeakLink<T> {
    /// The target, if any owner still holds it.
    #[inline]
    pub(crate) fn upgrade(&self) -> Option<Arc<T>> {
        self.0.upgrade().map(<dyn Linked<T> as Linked<T>>::into_arc)
    }
}
