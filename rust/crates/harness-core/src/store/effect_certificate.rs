//! Fixed-width authenticated effect cuts. Digests are bytes, not decimal JSON arrays.
pub(crate) const LENGTH: usize = 65;
const ZERO: [u8; 32] = [0; 32];

pub(crate) fn parts<'a>(root: Option<&'a [u8; 32]>, proof: &'a [u8; 32]) -> [&'a [u8]; 3] {
    [
        if root.is_some() { &[1] } else { &[0] },
        root.unwrap_or(&ZERO),
        proof,
    ]
}

pub(crate) fn decode(bytes: &[u8]) -> Option<(Option<&[u8; 32]>, &[u8; 32])> {
    let bytes: &[u8; LENGTH] = bytes.try_into().ok()?;
    let root: &[u8; 32] = bytes[1..33].try_into().ok()?;
    let proof = bytes[33..].try_into().ok()?;
    let root = match bytes[0] {
        0 if root == &ZERO => None,
        1 => Some(root),
        _ => return None,
    };
    Some((root, proof))
}

#[cfg(test)]
#[path = "effect_certificate_tests.rs"]
mod tests;
