//! Resource identity is exactly (account, id). Both components are opaque,
//! nonempty byte strings. No kind, digest, generation or normalization is added.
//! Wire and storage use the same two u64 big-endian length-prefixed fields.
//! Decoding is a left inverse of encoding, so this representation is injective.

mod dns;
pub use dns::{CanonicalHost, DnsError, validate_dns_label};

/// Invalid identity component or canonical encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IdentityError {
    /// An account cannot be absent.
    #[error("account is empty")]
    EmptyAccount,
    /// An id cannot be absent.
    #[error("id is empty")]
    EmptyId,
    /// Truncation, invalid lengths or trailing bytes.
    #[error("malformed identity encoding")]
    Malformed,
}

impl IdentityError {
    /// Stable error code for language bindings.
    pub const fn code(self) -> &'static str {
        match self {
            Self::EmptyAccount => "empty_account",
            Self::EmptyId => "empty_id",
            Self::Malformed => "malformed_identity",
        }
    }
}

/// Validated opaque account id. Bytes are retained exactly, including non-UTF-8.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AccountId(Vec<u8>);

impl AccountId {
    /// Validate a nonempty opaque account, without normalization.
    pub fn new(bytes: &[u8]) -> Result<Self, IdentityError> {
        validate_account(bytes)?;
        Ok(Self(bytes.to_vec()))
    }
    /// Exact original bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Validated opaque resource id; product-specific creation rules still apply.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceId(Vec<u8>);

impl ResourceId {
    /// Validate a nonempty opaque id, without normalization.
    pub fn new(bytes: &[u8]) -> Result<Self, IdentityError> {
        validate_id(bytes)?;
        Ok(Self(bytes.to_vec()))
    }
    /// Exact original bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The literal globally scoped pair. Resource kind is deliberately absent.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceIdentity {
    account: AccountId,
    id: ResourceId,
}

impl ResourceIdentity {
    /// Validate the two opaque components. This does not create a resource.
    pub fn new(account: &[u8], id: &[u8]) -> Result<Self, IdentityError> {
        validate_account(account)?;
        validate_id(id)?;
        Ok(Self {
            account: AccountId(account.to_vec()),
            id: ResourceId(id.to_vec()),
        })
    }
    /// Construct from independently validated components.
    pub fn from_parts(account: AccountId, id: ResourceId) -> Self {
        Self { account, id }
    }
    /// Account component.
    pub const fn account(&self) -> &AccountId {
        &self.account
    }
    /// Resource id component.
    pub const fn id(&self) -> &ResourceId {
        &self.id
    }
    /// Exact canonical wire/storage size.
    pub fn encoded_len(&self) -> usize {
        16 + self.account.0.len() + self.id.0.len()
    }
    /// Canonical wire and storage encoding: u64-BE account length, account bytes,
    /// u64-BE id length, id bytes. There are no other fields.
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.encoded_len());
        for field in [self.account.as_bytes(), self.id.as_bytes()] {
            output.extend_from_slice(&(field.len() as u64).to_be_bytes());
            output.extend_from_slice(field);
        }
        output
    }
    /// Decode exactly one canonical pair. Framing is checked before allocation.
    pub fn decode(mut bytes: &[u8]) -> Result<Self, IdentityError> {
        let account = field(&mut bytes)?;
        validate_account(account)?;
        let id = field(&mut bytes)?;
        validate_id(id)?;
        if !bytes.is_empty() {
            return Err(IdentityError::Malformed);
        }
        Self::new(account, id)
    }
    /// Direct-account DNS form: `<id>.<account>.<domain>`. All labels must
    /// already be lowercase LDH; no conversion, escaping or hashing occurs.
    pub fn hostname(&self, domain: &str) -> Result<CanonicalHost, DnsError> {
        validate_dns_label(self.id.as_bytes())?;
        validate_dns_label(self.account.as_bytes())?;
        if domain.len() > 253 || self.id.0.len() + self.account.0.len() + domain.len() + 2 > 253 {
            return Err(DnsError::HostTooLong);
        }
        let id = std::str::from_utf8(self.id.as_bytes()).map_err(|_| DnsError::InvalidCharacter)?;
        let account =
            std::str::from_utf8(self.account.as_bytes()).map_err(|_| DnsError::InvalidCharacter)?;
        CanonicalHost::parse(&format!("{id}.{account}.{domain}"))
    }
}

fn validate_account(bytes: &[u8]) -> Result<(), IdentityError> {
    if bytes.is_empty() {
        return Err(IdentityError::EmptyAccount);
    }
    Ok(())
}
fn validate_id(bytes: &[u8]) -> Result<(), IdentityError> {
    if bytes.is_empty() {
        return Err(IdentityError::EmptyId);
    }
    Ok(())
}
fn field<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], IdentityError> {
    let (prefix, rest) = bytes.split_at_checked(8).ok_or(IdentityError::Malformed)?;
    let length = u64::from_be_bytes(prefix.try_into().map_err(|_| IdentityError::Malformed)?);
    let length = usize::try_from(length).map_err(|_| IdentityError::Malformed)?;
    let (value, rest) = rest
        .split_at_checked(length)
        .ok_or(IdentityError::Malformed)?;
    *bytes = rest;
    Ok(value)
}

#[cfg(test)]
mod tests;
