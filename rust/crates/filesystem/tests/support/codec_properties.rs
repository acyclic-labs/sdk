use acyclic_fs::kernel::CanonicalDecodeError;
use proptest::prelude::*;

pub(super) type Change = (usize, u8, usize, Vec<u8>);

pub(super) fn change() -> impl Strategy<Value = Change> {
    (
        any::<usize>(),
        any::<u8>(),
        0_usize..3,
        prop::collection::vec(any::<u8>(), 0..2),
    )
}

fn mutate(bytes: &[u8], (at, flip, cut, junk): &Change) -> Vec<u8> {
    let mut mutated = bytes.to_vec();
    if let Some(byte) = mutated.get_mut(at % bytes.len().max(1)) {
        *byte ^= flip;
    }
    mutated.truncate(mutated.len().saturating_sub(*cut));
    mutated.extend_from_slice(junk);
    mutated
}

pub(super) fn check_canonical<T: PartialEq + std::fmt::Debug, E: std::fmt::Debug>(
    value: &T,
    encode: impl Fn(&T) -> Result<Vec<u8>, E>,
    decode: impl Fn(&[u8]) -> Result<T, CanonicalDecodeError>,
    change: &Change,
) -> Result<Vec<u8>, TestCaseError> {
    let fail = |error: E| TestCaseError::fail(format!("{error:?}"));
    let encoded = encode(value).map_err(fail)?;
    let decoded = decode(&encoded);
    prop_assert_eq!(decoded.as_ref(), Ok(value));
    let mutated = mutate(&encoded, change);
    if let Ok(decoded) = decode(&mutated) {
        prop_assert_eq!(encode(&decoded).map_err(fail)?, mutated);
    }
    Ok(encoded)
}
