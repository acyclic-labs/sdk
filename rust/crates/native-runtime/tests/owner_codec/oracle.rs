//! Owner envelopes through the public native writer, reader and OS protection.
use acyclic_native_runtime::owner_record::{decode_with, encode_with};
use std::io;

const MAXIMUM: u64 = 65_536;

fn encode(plain: &[u8]) -> io::Result<Vec<u8>> {
    encode_with(plain, MAXIMUM, None, |parts| Ok(parts.concat()))
}

fn refused(bytes: &[u8], maximum: u64) {
    assert!(
        decode_with::<(), io::Error>(bytes, maximum, None, |_| panic!("corrupt record decoded"))
            .is_err()
    );
}

/// Arbitrary wire plus checksum-valid current-owner outputs. Every fuzz input
/// uses bounded writer work; no prefix or corruption scan is in this hot path.
pub fn check(bytes: &[u8]) {
    let _ = decode_with::<_, io::Error>(bytes, MAXIMUM, None, |plain| Ok(plain.to_vec()));
    // Native owner payloads are nonempty. Arbitrary wire (including empty)
    // is decoded above; generated payloads retain that production premise.
    let mut plain = vec![1];
    plain.extend_from_slice(&bytes[..bytes.len().min(4095)]);
    let mut record = encode(&plain).expect("original owner writer refused its bounded input");
    let decoded = decode_with::<_, io::Error>(&record, MAXIMUM, None, |plain| Ok(plain.to_vec()))
        .expect("original owner output did not decode");
    assert_eq!(decoded, plain, "owner payload changed");

    let choice = usize::from(u16::from_le_bytes(std::array::from_fn(|i| {
        bytes.get(i).copied().unwrap_or(0)
    })));
    let changed = choice % record.len();
    record[changed] ^= 1;
    refused(&record, MAXIMUM);
    record[changed] ^= 1;
    refused(&record[..choice % record.len()], MAXIMUM);
    refused(&record, record.len() as u64 - 1);

    let maximum = (choice % 40) as u64;
    assert!(
        encode_with::<(), io::Error>(b"", maximum, None, |_| {
            panic!("unadmitted envelope copied")
        })
        .is_err()
    );
    let error = decode_with::<(), io::Error>(&record, MAXIMUM, None, |_| {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "original owner copy refused",
        ))
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(error.to_string(), "original owner copy refused");
}

/// Exhaust retained seed boundaries only during ordinary replay and capture.
#[cfg(not(fuzzing))]
pub fn check_retained(bytes: &[u8]) {
    check(bytes);
    for index in 0..bytes.len() {
        let mut changed = bytes.to_vec();
        changed[index] ^= 1;
        refused(&changed, MAXIMUM);
        refused(&bytes[..index], MAXIMUM);
    }
}

/// These are deliberately nonsecret SDK inputs, never real owner credentials.
#[cfg(not(fuzzing))]
pub fn seeds() -> io::Result<Vec<(&'static str, Vec<u8>)>> {
    [
        ("minimal", b"\0".as_slice()),
        ("binary", b"\0\xff\n\x80\x01".as_slice()),
        (
            "text",
            b"SDK-owned nonsecret owner-envelope corpus".as_slice(),
        ),
    ]
    .into_iter()
    .map(|(name, plain)| Ok((name, encode(plain)?)))
    .collect()
}
