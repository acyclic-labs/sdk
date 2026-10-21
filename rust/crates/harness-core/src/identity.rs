//! All fresh harness UUIDs cross this boundary; persisted IDs are never reminted.
#[doc(hidden)]
pub fn fresh_uuid() -> uuid::Uuid {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return uuid::Builder::from_random_bytes(bytes).into_uuid();
        }
    }
    uuid::Uuid::new_v4()
}

/// Fills persistent local owner keys through the same entropy boundary as IDs.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn fill_random(bytes: &mut [u8]) -> Result<(), getrandom::Error> {
    #[cfg(feature = "test-support")]
    if acyclic_native_runtime::testing::fill_seeded(bytes) {
        return Ok(());
    }
    getrandom::fill(bytes)
}
