//! Independent fixed-width cut oracle, shared by shrinking and byte fuzzing.
pub fn reference(bytes: &[u8]) -> Option<(Option<[u8; 32]>, [u8; 64])> {
    if bytes.len() != 97 || bytes[0] > 1 || (bytes[0] == 0 && bytes[1..33] != [0; 32]) {
        return None;
    }
    Some((
        (bytes[0] == 1).then(|| std::array::from_fn(|i| bytes[i + 1])),
        std::array::from_fn(|i| bytes[i + 33]),
    ))
}
