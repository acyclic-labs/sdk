//! A model request is a frozen [`Header`], independent item segments and a [`Tail`].
//! This crate is the only provider request encoder: root fields have one fixed
//! order ([`Fields`]), and per-call controls follow the conversation.
//!
//! Admission owns JSON validation and copying. Rendering borrows the admitted
//! bytes and has no I/O except the caller's writer. Ordinary rendering selects
//! each segment independently; Gemini summaries retain one visible item to
//! extend a final user turn. Persist [`WireItem`] in the conversation stream; do not
//! reconstruct it from display events. Epoch selection and paging belong to
//! the stream reader, so this crate never owns or scans conversation history.
#![deny(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::disallowed_macros
)]

pub mod exchanges;
mod fields;
mod item;
mod notice;
mod profile;
mod render;
mod selection;
mod sink;
pub mod strict;
mod tools;

pub use fields::{Field, Fields};
pub use item::{Family, ItemKind, Provenance, WireItem};
pub use notice::{Notice, PendingNotices, notice_block};
pub use profile::{Cache, Effort, Reasoning};
pub use render::{
    Header, RenderError, Segment, Tail, gemini_summary, json_error, render, render_summary, seg,
    summary_artifact,
};
pub use tools::{SummaryHeader, ToolDeclaration, ToolDeclarations};

/// Changing rendered bytes requires a new renderer version and corpus review.
pub const RENDERER_VERSION: u32 = 5;

#[cfg(test)]
mod notice_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tools_tests;
