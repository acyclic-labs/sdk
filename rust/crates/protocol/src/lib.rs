//! Exact protocol identity and capability admission, independent of wire encoding.
use std::collections::BTreeSet;

/// Why a peer cannot use the service's compiled contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error<'a> {
    /// No protocol identity was supplied.
    MissingIdentity,
    /// The supplied identity is empty or differs from the compiled contract.
    IdentityMismatch,
    /// A required capability is malformed or absent from the advertised set.
    UnsupportedCapability {
        /// Required capability name.
        name: &'a str,
        /// Required exact version.
        version: &'a str,
    },
}

impl std::fmt::Display for Error<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingIdentity => formatter.write_str("protocol identity is missing"),
            Self::IdentityMismatch => formatter.write_str("protocol identity mismatch"),
            Self::UnsupportedCapability { name, version } => {
                write!(formatter, "unsupported capability {name}@{version}")
            }
        }
    }
}
impl std::error::Error for Error<'_> {}

/// Validate the peer's nonempty identity against the compiled contract.
///
/// # Errors
/// Refuses an absent, empty or different protocol identity.
pub fn validate_identity<'a>(
    actual: Option<(&'a str, &'a str)>,
    expected: (&str, &str),
) -> Result<(), Error<'a>> {
    let actual = actual.ok_or(Error::MissingIdentity)?;
    if actual.0.is_empty() || actual.1.is_empty() || actual != expected {
        return Err(Error::IdentityMismatch);
    }
    Ok(())
}

/// Admit an exact compiled identity and every required capability version.
/// Service adapters supply borrowed wire fields and their advertised set.
/// Capability membership is indexed only when the request requires it.
///
/// # Errors
/// Refuses missing or incompatible identity and malformed or unsupported requirements.
pub fn negotiate<'a>(
    actual: Option<(&'a str, &'a str)>,
    expected: (&str, &str),
    required: impl IntoIterator<Item = (&'a str, &'a str)>,
    supported: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<(), Error<'a>> {
    validate_identity(actual, expected)?;
    let mut required = required.into_iter().peekable();
    if required.peek().is_none() {
        return Ok(());
    }
    let available: BTreeSet<_> = supported.into_iter().collect();
    for (name, version) in required {
        if name.is_empty() || version.is_empty() || !available.contains(&(name, version)) {
            return Err(Error::UnsupportedCapability { name, version });
        }
    }
    Ok(())
}
