#![deny(unsafe_code)]
//! JSON admission contracts shared by the harness codecs and their producers.
//! Schemas describe opaque values; traversal, work and custody stay with callers.

#[doc(hidden)]
pub mod schema;
pub use schema::JsonValueSchema;
mod literal;
pub use literal::deserialize as deserialize_literal;

/// The existing producer depth contract, also used by saved-record readers
/// and literal model payloads.
pub const RECORD_MAXIMUM_DEPTH: u32 = 256;
/// How much deeper a selected model context nests a recorded tool call's
/// members than the saved record does. The record nests metadata one level
/// down; the context, its messages, the message and its tool-call part nest it
/// four. Admission subtracts this from [`RECORD_MAXIMUM_DEPTH`], so every
/// admitted record also projects and replays within the decoder's depth.
pub const PROJECTION_NESTING_DEPTH: u32 = 3;
/// Fixed JSON input failures, distinct from exhausted work allowances.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JsonInputError {
    /// A closing container has no matching opener.
    #[error("invalid JSON nesting")]
    Nesting,
    /// The source ends inside a string or container.
    #[error("incomplete JSON document")]
    Incomplete,
    /// A value exceeds the fixed nesting limit.
    #[error("JSON nesting limit exceeded")]
    Depth,
    /// The document has a shape its typed decode rejects.
    #[error("JSON shape is outside its typed layout")]
    Shape,
}

/// Optional byte and node policies with nesting admission for one document.
#[derive(Clone, Copy, Debug)]
pub struct JsonBounds {
    /// Optional UTF-8 input/output byte policy; `None` adds no byte ceiling.
    pub maximum_bytes: Option<u64>,
    /// Optional values and keys ceiling, including replaced duplicate entries.
    pub maximum_items: Option<u64>,
    /// Zero-based nesting depth.
    pub maximum_depth: u32,
}
