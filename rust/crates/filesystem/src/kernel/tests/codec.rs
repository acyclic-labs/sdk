use super::{CanonicalDecodeError, Decoder, Encoder};

#[test]
fn exact_capacity_accepts_only_the_complete_header_boundary() {
    let domain = b"domain";
    let minimum = domain.len() + 2;

    assert_eq!(
        Encoder::with_exact_capacity(domain, 7, minimum).map(Encoder::finish),
        Ok([domain.as_slice(), &7_u16.to_le_bytes()].concat())
    );

    assert!(matches!(
        Encoder::with_exact_capacity(domain, 7, minimum - 1),
        Err(CanonicalDecodeError::LengthOverflow)
    ));
}

#[test]
fn decoder_rejects_version_and_bounded_field_overclaims_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let domain = b"domain";
    let mut wrong_version = Encoder::new(domain, 8);
    wrong_version.u8(0);
    assert_eq!(
        Decoder::new(&wrong_version.finish(), domain, 7, u64::MAX).err(),
        Some(CanonicalDecodeError::UnsupportedVersion(8))
    );

    let mut oversized = Encoder::new(domain, 7);
    oversized.u32(5);
    oversized.fixed(b"value");
    let encoded = oversized.finish();
    let mut copying = Decoder::new(&encoded, domain, 7, u64::MAX)?;
    assert_eq!(
        copying.bounded_bytes(4),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 5,
            maximum: 4,
        })
    );
    let mut skipping = Decoder::new(&encoded, domain, 7, u64::MAX)?;
    assert_eq!(
        skipping.skip_bounded_bytes(4),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 5,
            maximum: 4,
        })
    );
    Ok(())
}
