//! Fresh filesystem UUIDs share the active simulation's entropy source.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(test, feature = "native-watch", windows)
))]
pub(crate) fn fresh_uuid() -> uuid::Uuid {
    #[cfg(feature = "test-support")]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return uuid::Builder::from_random_bytes(bytes).into_uuid();
        }
    }
    uuid::Uuid::new_v4()
}

pub(crate) fn fresh_time_uuid() -> uuid::Uuid {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if let Some(millis) = acyclic_native_runtime::testing::unix_millis()
            && acyclic_native_runtime::testing::fill_seeded(&mut bytes)
        {
            bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
            bytes[6] = (bytes[6] & 0x0f) | 0x70;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            return uuid::Uuid::from_bytes(bytes);
        }
    }
    uuid::Uuid::now_v7()
}
