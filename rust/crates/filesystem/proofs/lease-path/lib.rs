#![deny(unsafe_code)]

#[path = "../../src/operation_window/lease_path/encoding.rs"]
mod encoding;

#[cfg(kani)]
mod proofs {
    use super::encoding;

    #[kani::proof]
    #[kani::unwind(21)]
    fn every_u64_expiry_has_an_admitted_length() {
        let expiry: u64 = kani::any();
        let length = encoding::length(expiry);
        assert!(length <= encoding::MAX_ENCODED_BYTES);
        assert!(length >= encoding::MAX_ENCODED_BYTES - 19);
    }

    #[kani::proof_for_contract(encoding::write_identity)]
    #[kani::unwind(17)]
    fn every_identity_fits_its_fixed_hex_component() {
        let identity: [u8; 16] = kani::any();
        let mut text: [u8; 32] = kani::any();
        encoding::write_identity(identity, &mut text, kani::any());
    }

    // Composition: the first proof establishes this precondition for every u64.
    // This proof covers every length in that range independently of the expiry,
    // a stronger domain than just the corresponding canonical lengths.
    #[kani::proof]
    #[kani::unwind(21)]
    #[kani::stub_verified(encoding::write_identity)]
    fn every_lease_path_fits_its_exact_buffer() {
        let expiry: u64 = kani::any();
        let workspace: [u8; 16] = kani::any();
        let lease: [u8; 16] = kani::any();
        let digits: u8 = kani::any();
        kani::assume(digits >= 1 && digits <= 20);
        let length = encoding::MAX_ENCODED_BYTES - 20 + usize::from(digits);
        let mut text = [0; encoding::MAX_ENCODED_BYTES];
        encoding::write(workspace, lease, expiry, length, &mut text);
    }

    #[kani::proof]
    #[kani::should_panic]
    #[kani::unwind(21)]
    #[kani::stub_verified(encoding::write_identity)]
    fn maximum_expiry_overflows_a_116_byte_buffer() {
        let mut text = [0; 116];
        encoding::write_buffer(
            [0; 16],
            [0; 16],
            u64::MAX,
            encoding::length(u64::MAX),
            &mut text,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::encoding;

    #[test]
    fn maximum_expiry_uses_all_117_bytes() {
        let mut text = [0; encoding::MAX_ENCODED_BYTES];
        let length = encoding::length(u64::MAX);
        encoding::write([0; 16], [0; 16], u64::MAX, length, &mut text);
        assert_eq!(length, 117);
        assert_eq!(
            std::str::from_utf8(&text).unwrap(),
            format!(
                "fs/operation-windows-v1/{}/leases/{}/{}",
                "0".repeat(32),
                "0".repeat(32),
                u64::MAX
            )
        );
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn the_production_formatter_rejects_116_bytes_at_maximum_expiry() {
        let mut text = [0; 116];
        encoding::write_buffer(
            [0; 16],
            [0; 16],
            u64::MAX,
            encoding::length(u64::MAX),
            &mut text,
        );
    }
}
