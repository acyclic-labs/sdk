//! Integration-test fixtures, each loaded once for the library's unit tests.
#[path = "../tests/common/request_hardening.rs"]
pub(crate) mod hardening;
#[path = "../tests/common/limits.rs"]
mod limits;
#[path = "../tests/common/signed.rs"]
pub(crate) mod signed;
// The signed fixture owns the claims root; request hardening reads it as `common`.
use signed::claims as common;
