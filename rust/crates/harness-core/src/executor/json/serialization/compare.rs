use super::*;

pub(super) struct Compare<'a> {
    remaining: &'a [u8],
    matched: bool,
}

impl<'a> Compare<'a> {
    pub(super) fn new(expected: &'a [u8]) -> Self {
        Self {
            remaining: expected,
            matched: true,
        }
    }

    pub(super) fn complete(&self) -> bool {
        self.matched && self.remaining.is_empty()
    }

    #[inline]
    pub(super) fn append(&mut self, bytes: &[u8]) {
        if self.matched {
            if let Some((prefix, remaining)) = self.remaining.split_at_checked(bytes.len()) {
                self.matched = match (prefix, bytes) {
                    ([], []) => true,
                    ([a], [b]) => a == b,
                    ([a, b], [c, d]) => (a == c) & (b == d),
                    ([a, b, c, d, e, f], [g, h, i, j, k, l]) => {
                        (a == g) & (b == h) & (c == i) & (d == j) & (e == k) & (f == l)
                    }
                    _ => prefix == bytes,
                };
                self.remaining = remaining;
            } else {
                self.matched = false;
            }
        }
    }
}

impl Write for Compare<'_> {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.append(bytes);
        Ok(bytes.len())
    }

    #[inline]
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.append(bytes);
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Compares already admitted data without another lexical admission pass.
/// The caller supplies normalized, ordered data for canonical comparison.
pub(crate) fn json_bytes_match(value: &Value, expected: &[u8]) -> Result<bool> {
    let mut comparison = Compare::new(expected);
    serde_json::to_writer(&mut comparison, value)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(comparison.complete())
}
