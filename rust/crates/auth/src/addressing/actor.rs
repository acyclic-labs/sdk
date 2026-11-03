//! Plain namespace-scoped actor references. These bytes convey no authority.
use super::{AccountId, IdentityError, field};

/// Reserved projection of existing account/id actors, without rewriting IDs.
pub const DEFAULT_ACTOR_NAMESPACE: &str = "__default";

// Actor and resource domains are disjoint. The tag identifies this grammar
// and the byte version permits rejecting future forms.
pub(super) const HEADER: &[u8] = b"\0\0\0\0\0\0\0\0acyclic.actor\x01";

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
    fn validated_text(bytes: &[u8]) -> Result<&str, IdentityError> {
        if !valid_name(bytes) {
            return Err(IdentityError::InvalidNamespace);
        }
        std::str::from_utf8(bytes).map_err(|_| IdentityError::InvalidNamespace)
    }

    /// Parse an exact namespace spelling, including the reserved legacy scope.
    pub fn new(bytes: &[u8]) -> Result<Self, IdentityError> {
        Ok(Self(Self::validated_text(bytes)?.to_owned()))
    }
    /// Validate an explicitly deployed namespace without allocating its spelling.
    pub fn validate_deployment(bytes: &[u8]) -> Result<&str, IdentityError> {
        let value = Self::validated_text(bytes)?;
        if value == DEFAULT_ACTOR_NAMESPACE {
            return Err(IdentityError::ReservedNamespace);
        }
        Ok(value)
    }
    /// Admit an explicitly deployed namespace, excluding the legacy scope.
    pub fn for_deployment(bytes: &[u8]) -> Result<Self, IdentityError> {
        Ok(Self(Self::validate_deployment(bytes)?.to_owned()))
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
        let mut bytes = crate::format::header(
            bytes,
            b"\0\0\0\0\0\0\0\0acyclic.actor",
            b"\x01",
            || IdentityError::Malformed,
            || IdentityError::UnsupportedVersion,
        )?;
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

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    #[kani::unwind(64)]
    fn bounded_actor_triples_round_trip_and_refuse_resource_encodings() {
        use crate::addressing::{ActorReference, IdentityError, ResourceIdentity, testing};
        let account: [u8; 4] = kani::any();
        let namespace: [u8; 4] = kani::any();
        let id: [u8; 4] = kani::any();
        let lengths: [usize; 3] = kani::any();
        for length in lengths {
            kani::assume((1..=4).contains(&length));
        }
        let account = &account[..lengths[0]];
        let namespace = &namespace[..lengths[1]];
        let id = &id[..lengths[2]];
        let Ok(value) = ActorReference::new(account, namespace, id) else {
            return;
        };
        let encoded = value.encode();
        assert_eq!(ActorReference::decode(&encoded), Ok(value));
        testing::framing(&encoded, ActorReference::decode);
        let resource = ResourceIdentity::new(account, id).unwrap().encode();
        testing::framing(&resource, ResourceIdentity::decode);
        assert!(ActorReference::decode(&resource).is_err());
        assert!(ResourceIdentity::decode(&encoded).is_err());
        let version = encoded.len() - (3 * 8 + account.len() + namespace.len() + id.len()) - 1;
        let other: u8 = kani::any();
        kani::assume(other != 1);
        let mut refused = encoded;
        refused[version] = other;
        refused.truncate(version + 1);
        assert_eq!(
            ActorReference::decode(&refused),
            Err(IdentityError::UnsupportedVersion)
        );
    }
}
