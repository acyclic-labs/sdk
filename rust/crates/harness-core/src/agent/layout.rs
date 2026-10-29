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

    /// Canonical text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AgentId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        if value.len() == ID_HEX
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(value))
        } else {
            Err(Error::Path("agent id is not 32 lowercase hex digits"))
        }
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
        Ok(Self {
            text: format!("{}/{CHILDREN}/{id}", self.text),
            depth: self
                .depth
                .checked_add(1)
                .ok_or(Error::Path("agent depth overflows this platform"))?,
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
        AgentId::try_from(segments.next().unwrap_or_default().to_owned())?;
        while let Some(segment) = segments.next() {
            if segment != CHILDREN {
                return Err(Error::Path("agent path has a non-children segment"));
            }
            AgentId::try_from(segments.next().unwrap_or_default().to_owned())?;
            depth += 1;
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
