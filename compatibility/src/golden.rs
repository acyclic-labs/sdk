//! One byte baseline for wire and persisted format tests.

/// Look up a named frame in the shared compatibility corpus.
///
/// # Errors
/// Refuses missing or duplicate names, odd-length hex and malformed hex.
pub fn frame(name: &str) -> Result<Vec<u8>, &'static str> {
    let mut matches = include_str!("../frames/golden.hex")
        .lines()
        .filter_map(|line| line.split_once(' '))
        .filter(|(key, _)| *key == name);
    let (_, hex) = matches.next().ok_or("golden frame is missing")?;
    if matches.next().is_some() {
        return Err("golden frame name is duplicated");
    }
    if hex.len() % 2 != 0 {
        return Err("golden frame hex is truncated");
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).map_err(|_| "golden frame hex is not ASCII")?;
            u8::from_str_radix(pair, 16).map_err(|_| "golden frame hex is invalid")
        })
        .collect()
}
