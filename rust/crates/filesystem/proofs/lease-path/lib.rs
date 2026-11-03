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
    fn every_identity_has_canonical_hex_bytes() {
        let identity: [u8; 16] = kani::any();
        let mut text: [u8; 32] = kani::any();
        encoding::write_identity(identity, &mut text, kani::any());
        let byte: usize = kani::any();
        kani::assume(byte < identity.len());
        let alphabet = b"0123456789abcdef";
        assert_eq!(text[2 * byte], alphabet[usize::from(identity[byte] >> 4)]);
        assert_eq!(
            text[2 * byte + 1],
            alphabet[usize::from(identity[byte] & 15)]
        );
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
    fn production_formatter_preserves_identities_expiry_and_exact_write_range() {
        // The independent standard formatter covers every identity byte value
        // at every position and every decimal-width boundary. Vary positions
        // and the two identities separately so permutations cannot disappear.
        let mut expiries = vec![0, u64::MAX];
        for exponent in 0..20 {
            let boundary = 10_u64.pow(exponent);
            expiries.extend([boundary - 1, boundary, boundary + 1]);
        }
        for value in 0..=u8::MAX {
            let workspace = std::array::from_fn(|i| value.wrapping_add(i as u8));
            let lease = std::array::from_fn(|i| value.wrapping_sub(17 * i as u8));
            let hex = |identity: [u8; 16]| {
                identity
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            };
            for &expiry in &expiries {
                let expected = format!(
                    "fs/operation-windows-v1/{}/leases/{}/{expiry}",
                    hex(workspace),
                    hex(lease)
                );
                let mut text = [0xa5; encoding::MAX_ENCODED_BYTES];
                let length = encoding::length(expiry);
                encoding::write(workspace, lease, expiry, length, &mut text);
                assert_eq!(length, expected.len());
                assert_eq!(&text[..length], expected.as_bytes());
                assert!(text[length..].iter().all(|&byte| byte == 0xa5));
            }
        }
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
