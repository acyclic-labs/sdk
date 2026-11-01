//! Peer authority is a capability over canonical agent paths, independent of
//! discovery. Discover authorized subtrees using Streams children pages.

use super::AgentPath;

/// Explicit extra authority beyond an agent's parent and open children.
/// `agent.send:<path>` grants one inbox; `agent.send-subtree:<path>` grants
/// that inbox and its descendants. Unrelated string prefixes never match.
#[must_use]
pub fn peer_send_allowed(grants: &crate::Capabilities, recipient: &AgentPath) -> bool {
    if grants.contains(&format!("agent.send:{recipient}")) {
        return true;
    }
    // Indexed membership for the canonical ancestry, never a scan over the
    // caller's grants or over any agent roster. One buffer serves every prefix.
    let mut label = format!("agent.send-subtree:{recipient}");
    loop {
        if grants.contains(&label) {
            return true;
        }
        let Some((parent, _)) = label.rsplit_once("/children/") else {
            return false;
        };
        label.truncate(parent.len());
    }
}

/// Boundary chosen by the sender. Both modes append to the original inbox.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MessageDelivery {
    /// Include at the next model boundary, without cancelling active tools.
    #[default]
    Steer,
    /// Start a subsequent turn after the current turn settles.
    FollowUp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_grants_use_canonical_ancestry_and_attenuate() -> Result<(), super::super::Error> {
        let root = AgentPath::root(&super::super::AgentId::from_bytes([1; 16]));
        let child = root.child(&super::super::AgentId::from_bytes([2; 16]))?;
        let other = AgentPath::root(&super::super::AgentId::from_bytes([3; 16]));
        let exact = crate::Capabilities::new([format!("agent.send:{root}")]);
        let subtree = crate::Capabilities::new([format!("agent.send-subtree:{root}")]);
        assert!(peer_send_allowed(&exact, &root));
        assert!(!peer_send_allowed(&exact, &child));
        assert!(peer_send_allowed(&subtree, &child));
        assert!(!peer_send_allowed(&subtree, &other));
        assert!(!peer_send_allowed(&crate::Capabilities::default(), &root));
        Ok(())
    }
}
