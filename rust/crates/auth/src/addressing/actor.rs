//! Plain namespace-scoped actor references. These bytes convey no authority.
use super::{AccountId, IdentityError, field};

/// Reserved projection of existing account/id actors, without rewriting IDs.
pub const DEFAULT_ACTOR_NAMESPACE: &str = "__default";

// A pair cannot start with this header: its account would be empty. The tag
// identifies this grammar and the byte version permits rejecting future forms.
const HEADER: &[u8] = b"\0\0\0\0\0\0\0\0acyclic.actor\x01";

fn valid_name(bytes: &[u8]) -> bool {
    (1..=128).contains(&bytes.len())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(byte))
}

/// The live actor id grammar, with exact case and no Unicode normalization.
pub fn validate_actor_id(bytes: &[u8]) -> Result<(), IdentityError> {
    if valid_name(bytes) {
        Ok(())
    } else {
        Err(IdentityError::InvalidActorId)
    }
}

/// Validated class/namespace name. It is not a DNS label.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NamespaceId(String);

impl NamespaceId {
    /// Parse an exact namespace spelling, including the reserved legacy scope.
    pub fn new(bytes: &[u8]) -> Result<Self, IdentityError> {
        if !valid_name(bytes) {
            return Err(IdentityError::InvalidNamespace);
        }
        let text = std::str::from_utf8(bytes).map_err(|_| IdentityError::InvalidNamespace)?;
        Ok(Self(text.to_owned()))
    }
    /// Admit an explicitly deployed namespace, excluding the legacy scope.
    pub fn for_deployment(bytes: &[u8]) -> Result<Self, IdentityError> {
        let value = Self::new(bytes)?;
        if value.as_str() == DEFAULT_ACTOR_NAMESPACE {
            return Err(IdentityError::ReservedNamespace);
        }
        Ok(value)
    }
    /// Resolve an absent legacy wire field to its reserved namespace.
    pub fn from_legacy_field(value: Option<&str>) -> Result<Self, IdentityError> {
        Self::new(value.unwrap_or(DEFAULT_ACTOR_NAMESPACE).as_bytes())
    }
    /// Exact spelling for logs and existing string fields.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The literal actor address `(account, namespace, id)`, without a capability.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActorReference {
    account: AccountId,
    namespace: NamespaceId,
    id: String,
}

impl ActorReference {
    /// Validate the three fields; this neither creates nor authorizes an actor.
    pub fn new(account: &[u8], namespace: &[u8], id: &[u8]) -> Result<Self, IdentityError> {
        let account = AccountId::new(account)?;
        let namespace = NamespaceId::new(namespace)?;
        validate_actor_id(id)?;
        let id = std::str::from_utf8(id)
            .map_err(|_| IdentityError::InvalidActorId)?
            .to_owned();
        Ok(Self {
            account,
            namespace,
            id,
        })
    }
    /// Wrap existing fields with an explicitly absent or present namespace.
    pub fn from_legacy_fields(
        account: &[u8],
        namespace: Option<&str>,
        id: &str,
    ) -> Result<Self, IdentityError> {
        let namespace = NamespaceId::from_legacy_field(namespace)?;
        Self::new(account, namespace.as_str().as_bytes(), id.as_bytes())
    }
    /// Account bytes, unchanged.
    pub const fn account(&self) -> &AccountId {
        &self.account
    }
    /// Namespace spelling, unchanged.
    pub const fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }
    /// Plain actor id, unchanged.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Recover the original field values without adding authority.
    pub fn into_parts(self) -> (AccountId, NamespaceId, String) {
        (self.account, self.namespace, self.id)
    }
    /// Disjoint versioned framing, then three u64-BE length-prefixed fields.
    pub fn encode(&self) -> Vec<u8> {
        let mut output = HEADER.to_vec();
        for bytes in [
            self.account.as_bytes(),
            self.namespace.as_str().as_bytes(),
            self.id.as_bytes(),
        ] {
            output.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
            output.extend_from_slice(bytes);
        }
        output
    }
    /// Decode exactly one canonical reference, rejecting unknown versions.
    pub fn decode(bytes: &[u8]) -> Result<Self, IdentityError> {
        let mut bytes = bytes.strip_prefix(HEADER).ok_or(IdentityError::Malformed)?;
        let account = field(&mut bytes)?;
        let namespace = field(&mut bytes)?;
        let id = field(&mut bytes)?;
        if !bytes.is_empty() {
            return Err(IdentityError::Malformed);
        }
        Self::new(account, namespace, id)
    }
}

/// Validate advisory placement; jurisdiction remains a namespace deployment property.
pub fn validate_location_hint(value: &str) -> Result<(), IdentityError> {
    if matches!(
        value,
        "wnam" | "enam" | "sam" | "weur" | "eeur" | "apac" | "oc" | "afr" | "me"
    ) {
        Ok(())
    } else {
        Err(IdentityError::InvalidLocationHint)
    }
}
