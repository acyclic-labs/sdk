//! A model request is a frozen [`Header`], independent item segments and a [`Tail`].
//!
//! Admission owns JSON validation and copying. Rendering borrows the admitted
//! bytes, has no I/O except the caller's writer, and never looks at another item
//! to choose a segment. Persist [`WireItem`] in the conversation stream; do not
//! reconstruct it from display events. Epoch selection and paging belong to
//! the stream reader, so this crate never owns or scans conversation history.
#![deny(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::disallowed_macros
)]

mod item;
mod render;
pub mod strict;

pub use item::{Family, ItemKind, Provenance, WireItem};
pub use render::{Header, RenderError, Tail, render, render_summary, seg};

/// Changing rendered bytes requires a new renderer version and corpus review.
pub const RENDERER_VERSION: u32 = 1;

#[cfg(test)]
mod tests;
