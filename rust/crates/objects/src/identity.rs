//! Fresh store and observation identities share one entropy boundary.
use crate::{Error, wire::ErrorCode};
use acyclic_auth::refusal::Cause;

pub(crate) fn fresh() -> Result<[u8; 16], Error> {
    let mut bytes = [0; 16];
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
        return Ok(bytes);
    }
    getrandom::fill(&mut bytes).map_err(|_| {
        Error::caused(
            ErrorCode::Unavailable,
            Cause::new("objects.store", "entropy_unavailable"),
        )
    })?;
    Ok(bytes)
}
