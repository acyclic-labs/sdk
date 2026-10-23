//! Versioned account-relative resource addresses. This codec allocates no names,
//! creates no resources and carries no incarnation or generation information.
//! Names and account ids are opaque bytes; no normalization is performed.
//!
//! Each of the five fields has a u64 big-endian byte length. The parser is a
//! left inverse of the encoder: equal transcripts therefore imply equal
//! (account, kind, name) tuples. BLAKE3 provides collision resistance for the
//! resulting 256-bit address; it does not make a mathematical injectivity claim
//! about a finite digest space.
//!
//! BLAKE3 matches Objects' content digests. Worker code's protocol-mandated
//! SHA-256 checksum, capability signatures and request replay digests retain
//! their existing algorithms and domains.

mod dns;
pub use dns::{CanonicalHost, DnsError, validate_dns_label};

/// Domain separator; the next framed field is the format version.
pub const DOMAIN: &[u8] = b"acyclic.resource.address";
/// Address transcript format version.
pub const VERSION: u8 = 1;
/// Maximum name length, in bytes (including non-UTF-8 bytes).
pub const MAX_NAME_BYTES: usize = 4096;
/// Maximum opaque account identifier length, in bytes.
pub const MAX_ACCOUNT_BYTES: usize = 4096;

/// Stable wire codes. Never renumber or reuse a code.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ResourceKind {
    /// Objects bucket.
    Bucket = 1,
    /// Stream, including its complete account-relative path.
    Stream = 2,
    /// Actor; existing actor lifecycle semantics remain with its provider.
    Actor = 3,
    /// Stateless Worker.
    Worker = 4,
    /// Filesystem workspace.
    FsWorkspace = 5,
    /// Machine.
    Machine = 6,
}

impl TryFrom<u8> for ResourceKind {
    type Error = AddressError;
    fn try_from(code: u8) -> Result<Self, Self::Error> {
        match code {
            1 => Ok(Self::Bucket),
            2 => Ok(Self::Stream),
            3 => Ok(Self::Actor),
            4 => Ok(Self::Worker),
            5 => Ok(Self::FsWorkspace),
            6 => Ok(Self::Machine),
            _ => Err(AddressError::UnknownKind(code)),
        }
    }
}

/// Malformed address input or noncanonical transcript.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AddressError {
    /// Account ids have a bounded representation.
    #[error("account exceeds 4096 bytes")]
    AccountTooLong,
    /// Names have a bounded representation.
    #[error("name exceeds 4096 bytes")]
    NameTooLong,
    /// Unknown codes fail closed.
    #[error("unknown resource kind {0}")]
    UnknownKind(u8),
    /// Only the explicitly supported transcript version is accepted.
    #[error("unsupported address version")]
    UnsupportedVersion,
    /// Truncation, incorrect lengths, domain or trailing bytes.
    #[error("malformed address transcript")]
    Malformed,
    /// Hexadecimal addresses have exactly one lowercase presentation.
    #[error("address must contain exactly 64 lowercase hexadecimal digits")]
    InvalidDigest,
}

/// Canonical 256-bit BLAKE3 address, independent of resource incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceAddress([u8; 32]);

impl ResourceAddress {
    /// Restore an opaque digest received from a trusted resource reference.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Derive an address from exact bytes. Empty names are codec values, even
    /// where an individual product's create API rejects them.
    pub fn new(account: &[u8], kind: ResourceKind, name: &[u8]) -> Result<Self, AddressError> {
        let resource = ResourceName::new(account, kind, name)?;
        Ok(resource.address())
    }

    /// Raw 256-bit digest.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for ResourceAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for ResourceAddress {
    type Err = AddressError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64 {
            return Err(AddressError::InvalidDigest);
        }
        let digit = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(AddressError::InvalidDigest),
        };
        let mut bytes = [0; 32];
        for (slot, &[high, low]) in bytes.iter_mut().zip(value.as_bytes().as_chunks::<2>().0) {
            *slot = digit(high)? * 16 + digit(low)?;
        }
        Ok(Self(bytes))
    }
}

/// Borrowed validated tuple; construction and parsing share the same bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceName<'a> {
    account: &'a [u8],
    kind: ResourceKind,
    name: &'a [u8],
}

impl<'a> ResourceName<'a> {
    /// Validate bounds without changing either byte string.
    pub fn new(
        account: &'a [u8],
        kind: ResourceKind,
        name: &'a [u8],
    ) -> Result<Self, AddressError> {
        if account.len() > MAX_ACCOUNT_BYTES {
            return Err(AddressError::AccountTooLong);
        }
        if name.len() > MAX_NAME_BYTES {
            return Err(AddressError::NameTooLong);
        }
        Ok(Self {
            account,
            kind,
            name,
        })
    }

    /// Exact account bytes.
    pub const fn account(&self) -> &'a [u8] {
        self.account
    }
    /// Stable resource kind.
    pub const fn kind(&self) -> ResourceKind {
        self.kind
    }
    /// Exact name bytes.
    pub const fn name(&self) -> &'a [u8] {
        self.name
    }

    /// Exact transcript size, including every field's eight-byte length.
    pub fn encoded_len(&self) -> usize {
        5 * 8 + DOMAIN.len() + 1 + self.account.len() + 1 + self.name.len()
    }

    // All five fields use u64 big-endian byte lengths, including fixed fields.
    fn fields(&self, mut emit: impl FnMut(&[u8])) {
        let version = [VERSION];
        let kind = [self.kind as u8];
        for field in [
            DOMAIN,
            version.as_slice(),
            self.account,
            kind.as_slice(),
            self.name,
        ] {
            let length = field.len() as u64;
            emit(&length.to_be_bytes());
            emit(field);
        }
    }

    /// Canonical preimage: five u64-BE-length-prefixed fields in order:
    /// domain, one-byte version, account, one-byte kind, name.
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.encoded_len());
        self.fields(|bytes| output.extend_from_slice(bytes));
        output
    }

    /// Hash the canonical transcript without a temporary allocation.
    pub fn address(&self) -> ResourceAddress {
        let mut hash = blake3::Hasher::new();
        self.fields(|bytes| {
            hash.update(bytes);
        });
        ResourceAddress(*hash.finalize().as_bytes())
    }

    /// Decode exactly one transcript. Alternate framing and trailing data fail.
    pub fn parse(mut bytes: &'a [u8]) -> Result<Self, AddressError> {
        if field(&mut bytes)? != DOMAIN {
            return Err(AddressError::Malformed);
        }
        if field(&mut bytes)? != [VERSION] {
            return Err(AddressError::UnsupportedVersion);
        }
        let account = field(&mut bytes)?;
        let [code] = field(&mut bytes)? else {
            return Err(AddressError::Malformed);
        };
        let kind = ResourceKind::try_from(*code)?;
        let name = field(&mut bytes)?;
        if !bytes.is_empty() {
            return Err(AddressError::Malformed);
        }
        Self::new(account, kind, name)
    }
}

fn field<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], AddressError> {
    let (prefix, rest) = bytes.split_at_checked(8).ok_or(AddressError::Malformed)?;
    let length = u64::from_be_bytes(prefix.try_into().map_err(|_| AddressError::Malformed)?);
    let length = usize::try_from(length).map_err(|_| AddressError::Malformed)?;
    let (value, rest) = rest
        .split_at_checked(length)
        .ok_or(AddressError::Malformed)?;
    *bytes = rest;
    Ok(value)
}

#[cfg(test)]
mod tests;
