//! Spawn admission shared by the serialized transition and its proofs.
//! Child cardinality and nesting are not admission conditions.

/// A live parent may create a child; an already-open spawn is idempotent even
/// while the parent archives. A closed parent cannot create a new child.
pub(crate) const fn admits(closed: bool, existing: bool) -> bool {
    !closed || existing
}
