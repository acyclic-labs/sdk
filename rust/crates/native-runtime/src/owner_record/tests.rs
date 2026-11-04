use super::*;

#[cfg(windows)]
#[test]
fn negative_control_plaintext_with_a_fresh_digest_cannot_replace_dpapi() {
    let plain = b"synthetic signing key";
    let record = [MAGIC.as_slice(), blake3::hash(plain).as_bytes(), plain].concat();
    assert!(
        decode_with::<(), io::Error>(&record, 65_536, None, |_| panic!(
            "plaintext fallback accepted"
        ))
        .is_err()
    );
}

#[test]
fn envelope_bounds_are_checked_before_protection_or_caller_allocation() {
    for maximum in 0..40 {
        assert!(
            encode_with::<(), io::Error>(b"", maximum, None, |_| panic!(
                "unadmitted envelope copied"
            ))
            .is_err()
        );
    }
}
