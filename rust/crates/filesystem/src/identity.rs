//! Fresh filesystem UUIDs share the active simulation's entropy source.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(test, not(target_arch = "wasm32"), windows)
))]
pub(crate) fn fresh_uuid() -> uuid::Uuid {
    #[cfg(any(test, feature = "test-support"))]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return uuid::Builder::from_random_bytes(bytes).into_uuid();
        }
    }
    uuid::Uuid::new_v4()
}
