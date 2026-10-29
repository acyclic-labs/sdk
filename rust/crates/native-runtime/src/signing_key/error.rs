//! Preserve the failing custody operation and native status without secret blobs.
use std::{error::Error, fmt, io};

/// A custody refusal. Native statuses retain their numeric OS identity.
#[derive(Debug)]
pub enum CustodyCause {
    /// Interaction is required; the OS does not distinguish a locked keychain
    /// from an unavailable authorization prompt with this status.
    InteractionRequired(i64),
    /// The OS reports that its selected store is absent.
    StoreAbsent(i64),
    /// No credential-bearing user session exists.
    NoUserSession(i64),
    /// Access to the selected store was refused.
    AccessDenied(Option<i64>),
    /// The OS cryptographic provider is unavailable.
    CryptoUnavailable(i64),
    /// The OS reports insufficient resources for the selected store operation.
    ResourceExhausted(i64),
    /// An exact OS status outside the named categories.
    NativeStatus(i64),
    /// Non-secret backend detail when no numeric status is exposed.
    Backend(String),
    /// The original reference is absent from the selected store.
    MissingEntry,
    /// The stored value does not match its reference or encoding.
    InvalidStoredSeed,
    /// The owner's explicit custody selection is invalid.
    InvalidPolicy,
    /// The backend refused the entry's attributes.
    InvalidEntry,
    /// Multiple entries match the exact reference.
    AmbiguousEntry,
}

/// A typed, non-secret error retained across native authority bootstrap.
#[derive(Debug)]
pub struct CustodyFailure {
    /// Failed admission or bootstrap step.
    pub operation: &'static str,
    /// Typed non-secret cause, including native status when available.
    pub cause: CustodyCause,
}

impl fmt::Display for CustodyFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            output,
            "signing custody step={} cause={:?}; recovery: ",
            self.operation, self.cause
        )?;
        match self.cause {
            CustodyCause::InvalidPolicy => output.write_str("select ACYCLIC_SIGNING_CUSTODY=native or private-file; use an absolute ACYCLIC_SIGNING_CUSTODY_DIRECTORY only with private-file"),
            CustodyCause::MissingEntry | CustodyCause::InvalidStoredSeed => output.write_str("restore the original selected signing store and reopen the original state directory"),
            _ => output.write_str("restore access to the selected signing store and reopen the original state directory; for a new private-file identity explicitly set ACYCLIC_SIGNING_CUSTODY=private-file"),
        }
    }
}

impl Error for CustodyFailure {}

pub(super) fn failure(operation: &'static str, cause: CustodyCause) -> io::Error {
    let kind = match cause {
        CustodyCause::MissingEntry => io::ErrorKind::NotFound,
        CustodyCause::InvalidPolicy => io::ErrorKind::InvalidInput,
        CustodyCause::InvalidStoredSeed
        | CustodyCause::InvalidEntry
        | CustodyCause::AmbiguousEntry => io::ErrorKind::InvalidData,
        CustodyCause::AccessDenied(_)
        | CustodyCause::InteractionRequired(_)
        | CustodyCause::NoUserSession(_) => io::ErrorKind::PermissionDenied,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, CustodyFailure { operation, cause })
}

fn native(source: &(dyn Error + Send + Sync + 'static)) -> CustodyCause {
    #[cfg(windows)]
    if let Some(error) = source.downcast_ref::<keyring::windows::Error>() {
        return match error.0 {
            5 => CustodyCause::AccessDenied(Some(error.0.into())),
            8 | 14 => CustodyCause::ResourceExhausted(error.0.into()),
            1312 => CustodyCause::NoUserSession(error.0.into()),
            50 | 120 | 0x8009_000b | 0x8009_0016 | 0x8009_0020 | 0x8009_0030 => {
                CustodyCause::CryptoUnavailable(error.0.into())
            }
            status => CustodyCause::NativeStatus(status.into()),
        };
    }
    #[cfg(target_vendor = "apple")]
    if let Some(error) = source.downcast_ref::<security_framework::base::Error>() {
        let status = i64::from(error.code());
        return match status {
            -25308 | -25315 => CustodyCause::InteractionRequired(status),
            -25291 | -25294 | -25307 => CustodyCause::StoreAbsent(status),
            -67729 => CustodyCause::NoUserSession(status),
            -25293 | -25292 | -128 => CustodyCause::AccessDenied(Some(status)),
            _ => CustodyCause::NativeStatus(status),
        };
    }
    if let Some(error) = source.downcast_ref::<io::Error>() {
        if error.kind() == io::ErrorKind::PermissionDenied {
            return CustodyCause::AccessDenied(error.raw_os_error().map(i64::from));
        }
        if let Some(status) = error.raw_os_error() {
            return CustodyCause::NativeStatus(status.into());
        }
    }
    CustodyCause::Backend(source.to_string())
}

pub(super) fn backend(operation: &'static str, error: keyring::Error) -> io::Error {
    let cause = match error {
        keyring::Error::NoEntry => CustodyCause::MissingEntry,
        keyring::Error::NoStorageAccess(source) | keyring::Error::PlatformFailure(source) => {
            native(source.as_ref())
        }
        // BadEncoding owns credential bytes. Never format or retain those bytes.
        keyring::Error::BadEncoding(bytes) => {
            let _cleared = zeroize::Zeroizing::new(bytes);
            CustodyCause::InvalidStoredSeed
        }
        keyring::Error::Ambiguous(_) => CustodyCause::AmbiguousEntry,
        _ => CustodyCause::InvalidEntry,
    };
    failure(operation, cause)
}
