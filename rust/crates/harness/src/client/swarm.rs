//! File-derived swarm demand and canonical update data.
use super::ClientInterests;
use crate::task_volume::HeadSnapshot;
use acyclic_stream::{StreamPath, wire};
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod driver;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use driver::SwarmClient;

/// Admission bounds, independent of how many agents are currently rendered.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct SwarmClientLimits {
    /// Whole-swarm discovery and status capacity.
    pub agents: usize,
    /// Combined visible and prefetched agents.
    pub selected: usize,
    /// Combined layout and open-view document bytes.
    pub layout_bytes: usize,
    /// Maximum command/workspace files in the config query, independent of agents.
    pub config_files: usize,
    /// Maximum logical files in each selected snapshot.
    pub snapshot_files: usize,
}

/// Inputs for the shared ClientViews fold; no renderer or second wire schema.
pub enum SwarmUpdate {
    /// Authoritative membership retired this agent after settling its effects.
    Retired {
        /// Canonical former member; its volume history remains readable.
        agent: StreamPath,
    },
    /// An agent discovered by the root's snapshot or live children follow.
    Agent {
        /// Canonical child path, even before its first status publication.
        agent: StreamPath,
    },
    /// Rendering demand changed; prefetch remains followed without rendering.
    Layout(ClientInterests),
    /// Exact initial heads; subsequent content starts at this snapshot's cursor.
    Snapshot {
        /// Selected agent identity.
        agent: StreamPath,
        /// Zero-copy materialized files at C.
        snapshot: HeadSnapshot,
    },
    /// Platform-delivered status or full-fidelity content.
    Data {
        /// Source agent.
        agent: StreamPath,
        /// True for the always-on latest-value status subscription.
        overview: bool,
        /// The exact platform data, including cursor and durability watermarks.
        data: wire::SessionData,
    },
    /// Standalone durability progress, even when no later content arrives.
    Watermark {
        /// Source agent.
        agent: StreamPath,
        /// Whether the source is the always-on status follow.
        overview: bool,
        /// Exact pushed provider watermarks; this does not consume another record.
        watermark: wire::SessionWatermark,
    },
}
