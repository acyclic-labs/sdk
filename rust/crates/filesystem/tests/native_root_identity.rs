//! Stable identity encoding shared by native filesystem adapters.

#![cfg(not(target_arch = "wasm32"))]

mod watch {
    mod tests {
        use acyclic_fs_native_contract::NativeRootIdentity;

        #[test]
        fn native_root_identity_encoding_is_exact_and_round_trips() {
            let identity = NativeRootIdentity {
                device: 0x0102_0304_0506_0708,
                object: 0x1112_1314_1516_1718,
            };
            let bytes = identity.to_bytes();
            let (device, object) = bytes.split_at(8);
            assert_eq!(device, &0x0102_0304_0506_0708_u64.to_le_bytes());
            assert_eq!(object, &0x1112_1314_1516_1718_u64.to_le_bytes());
            assert_eq!(NativeRootIdentity::from_bytes(bytes), identity);
        }
    }
}
