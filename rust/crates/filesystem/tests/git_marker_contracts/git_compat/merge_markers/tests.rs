use super::scanner::Scanner;

mod oracle;
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
        prop_assert_eq!(scanner.finish(), oracle::contains(&bytes));
    }
}

fn contains(text: &str) -> bool {
    let mut scanner = Scanner::new();
    scanner.push(text.as_bytes()) || scanner.finish()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn marker_lines_survive_line_ending_round_trip(lines in prop::collection::vec(
        prop_oneof!["[^\r\n]{0,24}", prop::sample::select(vec![
            "<<<<<<< ours".to_owned(), "||||||| base".to_owned(),
            "=======".to_owned(), ">>>>>>> theirs".to_owned(),
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
