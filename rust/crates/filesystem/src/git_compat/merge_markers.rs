//! A partial generated marker block is still an unresolved merge.
#[derive(Default)]
pub(super) struct Scanner {
    prefix: [u8; 8],
    length: usize,
    separator: bool,
    found: bool,
}

impl Scanner {
    pub(super) fn new() -> Self {
        Self {
            separator: true,
            ..Self::default()
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> bool {
        for &byte in bytes {
            if byte == b'\n' {
                self.found |= self.separator && self.length >= 7;
                self.length = 0;
                self.separator = true;
            } else {
                self.separator &= if self.length < 7 {
                    byte == b'='
                } else {
                    byte == b'\r'
                };
                if self.length < 8 {
                    self.prefix[self.length] = byte;
                    self.length += 1;
                    if self.length == 8 {
                        self.found |=
                            matches!(&self.prefix[..7], b"<<<<<<<" | b"|||||||" | b">>>>>>>")
                                && matches!(byte, b' ' | b'\t');
                    }
                }
            }
            if self.found {
                return true;
            }
        }
        false
    }

    pub(super) fn finish(&self) -> bool {
        self.found || (self.separator && self.length >= 7)
    }
}

#[cfg(all(test, not(acyclic_fs_loom)))]
pub(super) fn contains(text: &str) -> bool {
    let mut scanner = Scanner::new();
    scanner.push(text.as_bytes()) || scanner.finish()
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn chunks_match_complete_line_recognition(bytes in prop_oneof![
            prop::collection::vec(any::<u8>(), 0..256),
            prop::sample::select(vec![b"<<<<<<< ours\n".to_vec(), b"||||||| base".to_vec(), b"=======\r\r\n".to_vec(), b">>>>>>> theirs\n".to_vec()]),
        ], chunk in 1usize..32) {
            let mut scanner = Scanner::new();
            for part in bytes.chunks(chunk) { scanner.push(part); }
            let text = String::from_utf8_lossy(&bytes);
            let expected = text.lines().any(|line| line.trim_end_matches('\r') == "======="
                || ["<<<<<<< ", "<<<<<<<\t", "||||||| ", "|||||||\t", ">>>>>>> ", ">>>>>>>\t"].iter().any(|marker| line.starts_with(marker)));
            prop_assert_eq!(scanner.finish(), expected);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn marker_lines_survive_line_ending_round_trip(lines in prop::collection::vec(
            prop_oneof!["[^\r\n]{0,24}", prop::sample::select(vec![
                "<<<<<<< ours".to_string(), "||||||| base".to_string(),
                "=======".to_string(), ">>>>>>> theirs".to_string(),
            ])], 0..12)) {
            let lf = lines.join("\n");
            let crlf = lines.join("\r\n");
            prop_assert_eq!(contains(&lf), contains(&crlf));
            prop_assert_eq!(contains(&lf), contains(&crlf.replace("\r\n", "\n")));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn separator_suffix_survives_repeated_normalization(carriages in 0usize..64) {
            let text = format!("======={}\n", "\r".repeat(carriages));
            prop_assert!(contains(&text));
            prop_assert!(contains(&text.replace("\r\n", "\n")));
        }
    }

    #[test]
    fn partial_marker_lines_remain_unresolved() {
        for marker in [
            "<<<<<<< ours\n",
            "||||||| base\n",
            "=======\r\n",
            ">>>>>>> theirs\n",
        ] {
            assert!(contains(marker));
        }
        assert!(!contains("resolved bytes\n"));
    }
}
