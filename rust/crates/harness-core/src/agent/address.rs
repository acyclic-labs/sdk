//! Agent addresses retain qualification until the owning host resolves them.
use super::{AgentId, AgentPath, Error};
use acyclic_actors::addressing::ActorReference;
use serde::{Deserialize, Serialize};

/// An address relative to a host, or a fully qualified peer root. Neither form
/// grants authority. Qualification must survive receipts, waits and retries.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(try_from = "String")]
pub struct AgentAddress {
    path: AgentPath,
    actor: Option<ActorReference>,
}

impl Serialize for AgentAddress {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.actor {
            Some(actor) => {
                serializer.serialize_str(&acyclic_auth::encode_component(&actor.encode()))
            }
            None => serializer.serialize_str(self.path.as_str()),
        }
    }
}

impl AgentAddress {
    /// Retain a canonical account/namespace/root identity, without a credential.
    pub fn qualified(actor: ActorReference) -> Result<Self, Error> {
        Ok(Self {
            path: AgentPath::root(&AgentId::try_from(actor.id().to_owned())?),
            actor: Some(actor),
        })
    }

    /// Physical subtree inside its owning provider; this discards no stored
    /// qualifier, and must not be used to resolve a peer on another host.
    pub fn path(&self) -> &AgentPath {
        &self.path
    }

    /// Explicitly retained identity, if this address was already qualified.
    pub fn qualified_actor(&self) -> Option<&ActorReference> {
        self.actor.as_ref()
    }

    /// Resolve a relative address against the original host. A qualified peer
    /// always keeps its account and namespace, including a foreign namespace.
    pub fn actor(&self, within: &ActorReference) -> Result<ActorReference, Error> {
        match &self.actor {
            Some(actor) => Ok(actor.clone()),
            None => ActorReference::new(
                within.account().as_bytes(),
                within.namespace().as_str().as_bytes(),
                self.path.id().as_str().as_bytes(),
            )
            .map_err(|_| Error::Record("invalid agent address")),
        }
    }
}

impl From<AgentPath> for AgentAddress {
    fn from(path: AgentPath) -> Self {
        Self { path, actor: None }
    }
}

impl From<AgentAddress> for String {
    fn from(address: AgentAddress) -> Self {
        match address.actor {
            Some(actor) => acyclic_auth::encode_component(&actor.encode()),
            None => address.path.to_string(),
        }
    }
}

impl TryFrom<String> for AgentAddress {
    type Error = Error;
    fn try_from(value: String) -> Result<Self, Error> {
        if value.starts_with(super::layout::AGENTS) {
            return AgentPath::try_from(value).map(Into::into);
        }
        let bytes = acyclic_auth::decode_component(&value)
            .map_err(|_| Error::Record("invalid qualified agent address"))?;
        let actor = ActorReference::decode(&bytes)
            .map_err(|_| Error::Record("invalid qualified agent address"))?;
        Self::qualified(actor)
    }
}
