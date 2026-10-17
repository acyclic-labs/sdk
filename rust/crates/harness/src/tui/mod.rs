//! Shared widgets and TaskFileViews presentation for native clients.
pub use acyclic_harness_tui::*;
#[cfg(feature = "terminal")]
mod render;
#[cfg(feature = "terminal")]
pub use render::ClientRenderer;

#[cfg(feature = "terminal")]
mod agents;
#[cfg(feature = "terminal")]
pub use agents::AgentPanel;

#[cfg(feature = "terminal")]
mod diff;
#[cfg(feature = "terminal")]
pub use diff::diff_lines;

#[cfg(feature = "terminal")]
mod help;

pub(crate) mod catalog;
