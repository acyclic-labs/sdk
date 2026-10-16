//! A partial generated marker block is still an unresolved merge.
pub(super) fn contains(text: &str) -> bool {
    text.lines().any(|line| {
        line.trim_end_matches('\r') == "======="
            || [
                "<<<<<<< ",
                "<<<<<<<\t",
                "||||||| ",
                "|||||||\t",
                ">>>>>>> ",
                ">>>>>>>\t",
            ]
            .iter()
            .any(|marker| line.starts_with(marker))
    })
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

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
