use super::*;

fn encode(plain: &[u8]) -> io::Result<Vec<u8>> {
    encode_with(plain, 65_536, None, |parts| Ok(parts.concat()))
}

#[test]
fn protected_record_round_trips_through_the_single_codec() -> io::Result<()> {
    let plain = b"synthetic owner key and unicode payload";
    let record = encode(plain)?;
    let decoded = decode_with(&record, 65_536, None, |bytes| {
        Ok::<_, io::Error>(bytes.to_vec())
    })?;
    assert_eq!(decoded, plain);
    #[cfg(windows)]
    assert!(!record.windows(plain.len()).any(|window| window == plain));
    Ok(())
}

#[test]
fn every_corrupted_byte_and_truncated_prefix_is_rejected_before_decode() -> io::Result<()> {
    let record = encode(b"synthetic owner")?;
    for index in 0..record.len() {
        let mut changed = record.clone();
        *changed.get_mut(index).ok_or(io::ErrorKind::InvalidData)? ^= 1;
        assert!(
            decode_with::<(), io::Error>(&changed, 65_536, None, |_| panic!(
                "corrupt record decoded"
            ))
            .is_err()
        );
        assert!(
            decode_with::<(), io::Error>(
                record.get(..index).ok_or(io::ErrorKind::InvalidData)?,
                65_536,
                None,
                |_| panic!("truncated record decoded")
            )
            .is_err()
        );
    }
    assert!(
        decode_with::<(), io::Error>(&record, 1, None, |_| panic!("oversize record decoded"))
            .is_err()
    );
    Ok(())
}

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
fn negative_control_a_digestless_reader_accepts_corruption() -> io::Result<()> {
    let mut record = encode(b"synthetic owner")?;
    *record.get_mut(8).ok_or(io::ErrorKind::InvalidData)? ^= 1;
    assert!(decode_with::<(), io::Error>(&record, 65_536, None, |_| Ok(())).is_err());
    // Removing envelope authentication admits the same corrupted record through
    // the actual platform decoder, including DPAPI on Windows.
    let decoded = protection::unprotect_with_copy(
        record.get(40..).ok_or(io::ErrorKind::InvalidData)?,
        65_536,
        |bytes| Ok::<_, io::Error>(bytes.to_vec()),
    )?;
    assert_eq!(decoded, b"synthetic owner");
    Ok(())
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
