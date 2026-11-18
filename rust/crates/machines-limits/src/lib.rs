//! Shared Machines OCI syntax and pagination constants.

/// Canonical immutable OCI digest separator.
pub const MANAGED_OCI_REFERENCE_SEPARATOR: &str = "@sha256:";
/// Canonical hexadecimal character count for a SHA-256 digest.
pub const MANAGED_OCI_DIGEST_HEX_LENGTH: usize = 64;
/// Hexadecimal characters accepted by the canonical OCI parser.
pub const MANAGED_OCI_HEX_DIGITS: &str = "0123456789abcdefABCDEF";
/// Character used to identify the all-zero digest rejected by the parser.
pub const MANAGED_OCI_ZERO_DIGIT: char = '0';

/// Maximum machines returned in one page.
pub const MAX_PAGE_SIZE: u32 = 256;

/// Maximum events returned in one page.
pub const MAX_EVENT_PAGE_SIZE: u32 = 1_024;
