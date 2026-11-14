//! Admission for fixed, versioned binary format headers.

/// Borrow a payload only after its domain and version have been admitted.
///
/// Fixed headers require constant work, no allocation and no payload copy.
/// Errors are constructed only on refusal, before any payload interpretation.
/// Callers provide their boundary's typed errors rather than translating text.
///
/// Another domain, or input that ends inside the expected header, is
/// `invalid`: truncation is malformed input, not a version choice. A complete
/// header naming another version is `unsupported`.
pub fn header<'a, E>(
    encoded: &'a [u8],
    domain: &[u8],
    version: &[u8],
    invalid: impl FnOnce() -> E,
    unsupported: impl FnOnce() -> E,
) -> Result<&'a [u8], E> {
    match encoded.strip_prefix(domain) {
        Some(rest) => match rest.strip_prefix(version) {
            Some(payload) => Ok(payload),
            None if rest.len() < version.len() => Err(invalid()),
            None => Err(unsupported()),
        },
        None => Err(invalid()),
    }
}

/// Admit a decimal version using the same numeric value as the source writer.
/// The version and its terminator are rendered in fixed stack storage; this
/// neither allocates nor copies the payload. This avoids a second hand-written
/// version spelling drifting independently from its persisted numeric value.
pub fn decimal_header<'a, E>(
    encoded: &'a [u8],
    domain: &[u8],
    version: u32,
    terminator: u8,
    invalid: impl FnOnce() -> E,
    unsupported: impl FnOnce() -> E,
) -> Result<&'a [u8], E> {
    // Ten decimal digits represent every u32; the final byte terminates it.
    let mut rendered = [0; 11];
    rendered[10] = terminator;
    let mut start = 10;
    let mut remaining = version;
    loop {
        start -= 1;
        rendered[start] = b'0' + (remaining % 10) as u8;
        remaining /= 10;
        if remaining == 0 {
            break;
        }
    }
    header(encoded, domain, &rendered[start..], invalid, unsupported)
}

#[cfg(test)]
mod tests {
    proptest::proptest! {
        #[test]
        fn numeric_version_admission_borrows_the_original_payload(version: u32) {
            let header = format!("format:{version},");
            let encoded = format!("{header}original payload long enough for every version");
            let mut admitted = None;
            let allocation = acyclic_test_support::measure(|| {
                admitted = Some(super::decimal_header(encoded.as_bytes(), b"format:", version, b',', || false, || true));
            });
            assert_eq!(allocation.count_total, 0);
            let payload = admitted.unwrap().unwrap();
            assert_eq!(payload.as_ptr(), encoded.as_bytes()[header.len()..].as_ptr());
            assert_eq!(super::decimal_header(encoded.as_bytes(), b"format:", version.wrapping_add(1), b',', || false, || true), Err(true));
            for cut in 0..header.len() {
                assert_eq!(super::decimal_header(&encoded.as_bytes()[..cut], b"format:", version, b',', || false, || true), Err(false));
            }
        }
    }
}
