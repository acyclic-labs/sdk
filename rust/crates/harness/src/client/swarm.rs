//! File-derived swarm demand and canonical update data.
use super::{ClientInterests, ClientViewLimits};
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

/// Build one status publication on the swarm's shared `status` stream.
/// Publish an agent's first status before (or with) its parent-volume
/// membership: admitting the first member opens the shared status follow.
/// The transport key is the agent path, a zero byte, and the producer's opaque
/// key. This keeps conflation local to one agent/key and preserves attention.
pub fn status_publication(
    root: &StreamPath,
    agent: &StreamPath,
    mut value: wire::SessionLatestValue,
) -> crate::Result<acyclic_stream::AppendRequest> {
    use prost::Message as _;
    let demand = ClientInterests::from_layout(root.clone(), [], [], 0)?;
    if agent == root || demand.tier(agent).is_none() || value.key.is_empty() {
        return Err(crate::Error::Invalid(
            "invalid swarm status identity".into(),
        ));
    }
    let mut key = Vec::with_capacity(agent.as_str().len() + 1 + value.key.len());
    key.extend_from_slice(agent.as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(&value.key);
    value.key = key.into();
    Ok(acyclic_stream::AppendRequest {
        path: StreamPath::new(format!("{root}/status"))
            .map_err(|error| crate::Error::Invalid(error.to_string()))?,
        records: vec![value.encode_to_vec().into()],
        if_tail: None,
        idempotency_key: None,
    })
}

#[cfg(any(
    test,
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn status_agent(data: &mut wire::SessionData) -> crate::Result<StreamPath> {
    let Some(wire::session_data::Item::LatestValue(value)) = &mut data.item else {
        return Err(crate::Error::Invalid(
            "status received a non-value frame".into(),
        ));
    };
    let at = value
        .key
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| crate::Error::Invalid("status agent missing".into()))?;
    let agent = std::str::from_utf8(value.key.get(..at).unwrap_or_default())
        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
    let agent = StreamPath::new(agent).map_err(|error| crate::Error::Invalid(error.to_string()))?;
    value.key = value.key.slice(at + 1..);
    if value.key.is_empty() {
        return Err(crate::Error::Invalid("status key missing".into()));
    }
    Ok(agent)
}

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

impl SwarmClientLimits {
    /// The one admission rule for swarm bounds, independent of the transport
    /// that later follows them: positive capacities, selection within the
    /// swarm, and renderer views at least as wide as admission.
    pub fn validate(&self, views: &ClientViewLimits) -> crate::Result<()> {
        if self.agents == 0
            || self.selected > self.agents
            || self.layout_bytes == 0
            || self.config_files == 0
            || self.snapshot_files == 0
            || views.agents < self.agents
            || views.selected < self.selected
        {
            return Err(crate::Error::Invalid("invalid swarm client bounds".into()));
        }
        Ok(())
    }
}

/// Inputs for the shared ClientViews fold; no renderer or second wire schema.
pub enum SwarmUpdate {
    /// Progress of the shared status stream, applied once for every status view.
    StatusWatermark(wire::SessionWatermark),
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

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod status_tests {
    use super::*;
    use prost::Message as _;
    #[test]
    fn transport_identity_preserves_opaque_keys_payloads_and_attention() {
        let root = StreamPath::new("swarm").unwrap();
        let mut keys = std::collections::BTreeSet::new();
        for name in ["swarm/a", "swarm/b"] {
            for key in [b"status".as_slice(), b"x\0y".as_slice(), &[255, 128, 0]] {
                for attention in [false, true] {
                    let agent = StreamPath::new(name).unwrap();
                    let original = wire::SessionLatestValue {
                        key: key.to_vec().into(),
                        value: vec![0, 255, 128].into(),
                        attention,
                    };
                    let request = status_publication(&root, &agent, original.clone()).unwrap();
                    assert_eq!(request.path.as_str(), "swarm/status");
                    let value = wire::SessionLatestValue::decode(request.records[0].value.as_ref())
                        .unwrap();
                    if !attention {
                        assert!(keys.insert(value.key.clone()));
                    }
                    let mut data = wire::SessionData {
                        item: Some(wire::session_data::Item::LatestValue(value)),
                        ..Default::default()
                    };
                    assert_eq!(status_agent(&mut data).unwrap(), agent);
                    assert_eq!(
                        data.item,
                        Some(wire::session_data::Item::LatestValue(original))
                    );
                }
            }
        }
        for agent in ["swarm", "other/a"] {
            assert!(
                status_publication(
                    &root,
                    &StreamPath::new(agent).unwrap(),
                    wire::SessionLatestValue {
                        key: b"status".to_vec().into(),
                        ..Default::default()
                    }
                )
                .is_err()
            );
        }
        for key in [
            b"status".as_slice(),
            b"swarm/a\0".as_slice(),
            b"\xff\0key".as_slice(),
        ] {
            let mut data = wire::SessionData {
                item: Some(wire::session_data::Item::LatestValue(
                    wire::SessionLatestValue {
                        key: key.to_vec().into(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            };
            assert!(status_agent(&mut data).is_err());
        }
    }
}
