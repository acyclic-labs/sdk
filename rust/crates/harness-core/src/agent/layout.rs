//! Canonical agent subtree layout inside the `harness/` namespace consistency domain.
//!
//! ```text
//! harness/agents/<root>                   inbox (the subtree's root stream)
//! harness/agents/<root>/conversation      conversation, inherited by children at the fork cut
//! harness/agents/<root>/closed            exists once the agent is archived
//! harness/agents/<root>/children/<id>     child subtree, same shape
//! harness/actors/<actor>                  A4 actor state (`Store` root), outside the subtree
//! harness/status/<id>                      status records, outside the subtree
//! ```
//!
//! Status lives outside the subtree so the commit that creates an agent (a spawn
//! fork may write nothing inside its destination) also writes its first status:
//! every agent a `children` page lists already has one.

use super::Error;
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Root of every agent subtree.
pub const AGENTS: &str = "harness/agents";
/// Root of every agent's actor state, a sibling of [`AGENTS`] in the same domain.
pub const ACTORS: &str = "harness/actors";
/// Root of every agent volume authority stream, in the same domain.
pub const VOLUMES: &str = "harness/volumes";
/// Root of every agent's status stream, keyed by its globally unique id.
pub const STATUSES: &str = "harness/status";
/// Relative names a child does not inherit from its parent.
pub const NOT_INHERITED: [&str; 2] = [CHILDREN, CLOSED];
/// Deepest admitted nesting; bounds every path and every lineage walk.
pub const MAX_DEPTH: usize = 64;

const CONVERSATION: &str = "conversation";
const CLOSED: &str = "closed";
const CHILDREN: &str = "children";
const ID_HEX: usize = 32;

/// 128-bit agent identity, canonical lowercase hex. It is also the actor id.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentId(String);

impl AgentId {
    /// Builds an id from its 16 bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let mut text = String::with_capacity(ID_HEX);
        for byte in bytes {
            for nibble in [byte >> 4, byte & 0x0f] {
                text.extend(char::from_digit(u32::from(nibble), 16));
            }
        }
        Self(text)
    }

    /// The id's 16 bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        let digits = self.0.as_bytes().chunks_exact(2);
        for (byte, pair) in bytes.iter_mut().zip(digits) {
            let digit = |c: &u8| char::from(*c).to_digit(16).unwrap_or_default() as u8;
            for c in pair {
                *byte = (*byte << 4) | digit(c);
            }
        }
        bytes
    }

    /// Derives a child id from the parent and the spawning intent, so a retried
    /// spawn names the same child.
    #[must_use]
    pub fn derive(parent: &AgentId, intent: &str) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.child-id.v1");
        hasher.update(parent.0.as_bytes());
        hasher.update(&[0]);
        hasher.update(intent.as_bytes());
        let mut bytes = [0; 16];
        hasher.finalize_xof().fill(&mut bytes);
        Self::from_bytes(bytes)
    }

    /// The root a portable (event or browser) host creates for `intent`, so a
    /// retried create names the same agent. An Actor-hosted agent's intent is
    /// its actor id: the platform derives that actor's agent here, too.
    #[must_use]
    pub fn hosted_root(intent: &str) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.browser.agent.v1");
        hasher.update(intent.as_bytes());
        hasher.update(&[0]);
        let mut bytes = [0; 16];
        hasher.finalize_xof().fill(&mut bytes);
        Self::from_bytes(bytes)
    }

    /// Canonical text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(value: &str) -> Result<(), Error> {
        if value.len() == ID_HEX
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(())
        } else {
            Err(Error::Path("agent id is not 32 lowercase hex digits"))
        }
    }
}

impl TryFrom<String> for AgentId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        Self::validate(&value)?;
        Ok(Self(value))
    }
}

impl From<AgentId> for String {
    fn from(value: AgentId) -> Self {
        value.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The canonical path of an agent's root (inbox) stream; the agent's address.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentPath {
    text: String,
    depth: usize,
}

impl AgentPath {
    /// A top-level agent.
    #[must_use]
    pub fn root(id: &AgentId) -> Self {
        Self {
            text: format!("{AGENTS}/{id}"),
            depth: 0,
        }
    }

    /// The child `id` nested under this agent.
    pub fn child(&self, id: &AgentId) -> Result<Self, Error> {
        if self.depth + 1 > MAX_DEPTH {
            return Err(Error::Path("agent nesting exceeds MAX_DEPTH"));
        }
        Ok(Self {
            text: format!("{}/{CHILDREN}/{id}", self.text),
            depth: self.depth + 1,
        })
    }

    /// The parent agent, `None` for a root.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        if self.depth == 0 {
            return None;
        }
        let (rest, _) = self.text.rsplit_once('/')?;
        let (parent, _) = rest.rsplit_once('/')?;
        Some(Self {
            text: parent.to_owned(),
            depth: self.depth - 1,
        })
    }

    /// Whether `agent` is this canonical agent or one of its descendants.
    /// Compares the complete ancestry once without allocating parent paths.
    /// This checks routing containment, not the caller's current authority.
    #[must_use]
    pub fn contains(&self, agent: &Self) -> bool {
        agent
            .text
            .strip_prefix(self.text.as_str())
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("/children/"))
    }

    /// The agent's own id, the last path segment.
    #[must_use]
    pub fn id(&self) -> AgentId {
        AgentId(
            self.text
                .rsplit_once('/')
                .map_or(self.text.as_str(), |(_, id)| id)
                .to_owned(),
        )
    }

    /// Nesting depth; zero for a root.
    #[must_use]
    pub const fn depth(&self) -> usize {
        self.depth
    }

    /// Canonical text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The inbox, which is the subtree's root stream.
    pub fn inbox(&self) -> Result<StreamPath, Error> {
        stream(&self.text)
    }

    /// Conversation stream.
    pub fn conversation(&self) -> Result<StreamPath, Error> {
        stream(&format!("{}/{CONVERSATION}", self.text))
    }

    /// Status stream.
    pub fn status(&self) -> Result<StreamPath, Error> {
        stream(&format!("{STATUSES}/{}", self.id()))
    }

    /// Close marker stream; its existence refuses every later inbox append.
    pub fn closed(&self) -> Result<StreamPath, Error> {
        stream(&format!("{}/{CLOSED}", self.text))
    }

    /// Parent of the child subtrees, the `children_page` parent.
    pub fn children(&self) -> Result<StreamPath, Error> {
        stream(&format!("{}/{CHILDREN}", self.text))
    }
}

impl TryFrom<String> for AgentPath {
    type Error = Error;

    fn try_from(text: String) -> Result<Self, Error> {
        let rest = text
            .strip_prefix(AGENTS)
            .and_then(|rest| rest.strip_prefix('/'))
            .ok_or(Error::Path("agent path is outside harness/agents"))?;
        let mut segments = rest.split('/');
        let mut depth = 0;
        AgentId::validate(segments.next().unwrap_or_default())?;
        while let Some(segment) = segments.next() {
            if segment != CHILDREN {
                return Err(Error::Path("agent path has a non-children segment"));
            }
            AgentId::validate(segments.next().unwrap_or_default())?;
            depth += 1;
            if depth > MAX_DEPTH {
                return Err(Error::Path("agent nesting exceeds MAX_DEPTH"));
            }
        }
        Ok(Self { text, depth })
    }
}

impl From<AgentPath> for String {
    fn from(value: AgentPath) -> Self {
        value.text
    }
}

impl fmt::Display for AgentPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.text)
    }
}

/// Relative names excluded from a child's inherited subtree.
pub fn not_inherited() -> Result<Vec<StreamPath>, Error> {
    NOT_INHERITED.iter().map(|name| stream(name)).collect()
}

fn stream(text: &str) -> Result<StreamPath, Error> {
    StreamPath::new(text).map_err(Error::Stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;

    #[test]
    fn canonical_subtree_admission_borrows_ancestry_without_parent_copies() -> Result<(), Error> {
        let root =
            AgentPath::root(&AgentId::from_bytes([1; 16])).child(&AgentId::from_bytes([2; 16]))?;
        let foreign =
            AgentPath::root(&AgentId::from_bytes([9; 16])).child(&AgentId::from_bytes([2; 16]))?;
        // A leaf-id-only check would admit the same id under a foreign root.
        assert_eq!(root.id(), foreign.id());
        assert!(!root.contains(&foreign));
        assert!(!root.contains(&root.parent().expect("nested root")));
        for malformed in [
            format!("{root}0"),
            format!("{root}/children/{}", "A".repeat(ID_HEX)),
            format!("{root}/children/"),
            format!("{root}/status"),
        ] {
            assert!(AgentPath::try_from(malformed).is_err());
        }
        for depth in [1, 8, MAX_DEPTH] {
            let mut descendant = root.clone();
            for _ in root.depth()..depth {
                descendant = descendant.child(&AgentId::from_bytes([3; 16]))?;
            }
            let text = descendant.as_str().to_owned();
            let original = text.as_ptr();
            let mut parsed = None;
            let parsing = acyclic_test_support::measure(|| {
                parsed = Some(AgentPath::try_from(text));
            });
            let parsed = parsed.expect("parser ran")?;
            assert_eq!(parsed, descendant);
            assert_eq!(parsed.as_str().as_ptr(), original);
            assert_eq!(parsing.count_total, 0);
            // The former parser allocated an owned id for each ancestor.
            let owned_ids = acyclic_test_support::measure(|| {
                for id in parsed.as_str().split('/').skip(2).step_by(2) {
                    black_box(AgentId::try_from(id.to_owned()).expect("canonical id"));
                }
            });
            assert_eq!(owned_ids.count_total, depth as u64 + 1);
            for calls in [1, 100, 1000] {
                let borrowed = acyclic_test_support::measure(|| {
                    for _ in 0..calls {
                        assert!(black_box(&root).contains(black_box(&parsed)));
                        assert!(!root.contains(black_box(&foreign)));
                    }
                });
                assert_eq!(borrowed.count_total, 0);
                // Run the former parent-clone walk against the same ancestry.
                let parents = acyclic_test_support::measure(|| {
                    for _ in 0..calls {
                        let mut cursor = Some(black_box(&parsed).clone());
                        while let Some(path) = cursor {
                            if path == root {
                                break;
                            }
                            cursor = path.parent();
                        }
                    }
                });
                assert_eq!(parents.count_total, calls * depth as u64);
            }
        }
        Ok(())
    }
}
