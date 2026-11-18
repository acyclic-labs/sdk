//! Caller-selected refusal classification and work events for JSON syntax.
use crate::JsonInputError;

/// A type-level adapter; caller state stays in its context or work callback.
pub trait Policy: Default {
    /// The caller's original typed failure, propagated without conversion.
    type Error;
    /// Context used when classifying an input refusal.
    type Context: Copy;
    /// The work event emitted before admitting an original item.
    type Charge;
    /// Classify invalid JSON syntax.
    fn input(kind: JsonInputError, context: Self::Context) -> Self::Error;
    /// Classify a declared structural or byte bound.
    fn boundary(reason: &'static str, context: Self::Context) -> Self::Error;
    /// Classify overflow of an internal counter.
    fn overflow(reason: &'static str) -> Self::Error;
    /// One original item, before increasing its admitted count.
    fn item() -> Self::Charge;
}

/// An unfunded syntax failure, independent of a runtime's wire classification.
#[derive(Debug, thiserror::Error)]
pub enum SyntaxError {
    /// Invalid JSON grammar or nesting.
    #[error(transparent)]
    Input(JsonInputError),
    /// A declared bound or an overflowing counter.
    #[error("{0}")]
    Boundary(&'static str),
}

/// Unfunded parsing with the caller's explicit bounds and no hidden allowance.
#[derive(Default)]
pub struct Unfunded;

impl Policy for Unfunded {
    type Error = SyntaxError;
    type Context = ();
    type Charge = ();

    fn input(kind: JsonInputError, (): ()) -> Self::Error {
        SyntaxError::Input(kind)
    }
    fn boundary(reason: &'static str, (): ()) -> Self::Error {
        SyntaxError::Boundary(reason)
    }
    fn overflow(reason: &'static str) -> Self::Error {
        SyntaxError::Boundary(reason)
    }
    fn item() {}
}
