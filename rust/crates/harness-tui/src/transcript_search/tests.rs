use super::*;
use proptest::prelude::*;

// Independent whole-string fold plus a naive window scan. The production path
// retains only needle-sized origins and uses prefix links rather than rescans.
fn reference(text: &str, query: &str) -> Vec<Range<usize>> {
    let folded: Vec<_> = text.chars().default_case_fold().collect();
    let needle: Vec<_> = query.chars().default_case_fold().collect();
    if needle.is_empty() {
        return Vec::new();
    }
    let mut origins = Vec::new();
    for (start, character) in text.char_indices() {
        let count = character.to_string().chars().default_case_fold().count();
        origins.extend(std::iter::repeat_n(
            start..start + character.len_utf8(),
            count,
        ));
    }
    let mut found = Vec::new();
    for (at, window) in folded.windows(needle.len()).enumerate() {
        if window == needle {
            let range = origins[at].start..origins[at + needle.len() - 1].end;
            if found.last() != Some(&range) {
                found.push(range);
            }
        }
    }
    found
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn folded_search_preserves_source_ranges_and_has_linear_work(
        text in prop::collection::vec(any::<char>(), 0..96),
        query in prop::collection::vec(any::<char>(), 0..16),
    ) {
        let text: String = text.into_iter().collect();
        let query: String = query.into_iter().collect();
        let (found, work) = matches(&text, &query);
        prop_assert_eq!(&found, &reference(&text, &query));
        for range in found {
            prop_assert!(text.get(range).is_some());
        }
        let size = text.chars().default_case_fold().count() + query.chars().default_case_fold().count();
        prop_assert!(work <= 3 * size);
    }
}

#[test]
fn corpus_expansions_and_overlapping_matches() {
    for (text, query) in [
        ("Straße STRASSE", "strasse"),
        ("İi\u{307} 日本語", "i\u{307}"),
        ("Σσς", "σ"),
        ("ßß", "sss"),
        ("aaaaa", "aaa"),
        ("🦀 € 日", "日"),
    ] {
        assert_eq!(transcript_matches(text, query), reference(text, query));
    }
}

#[test]
#[should_panic(expected = "ASCII folding loses Unicode matches")]
fn ascii_only_search_is_detected_negative_control() {
    let broken: Vec<_> = "Straße"
        .to_ascii_lowercase()
        .match_indices("strasse")
        .map(|(at, text)| at..at + text.len())
        .collect();
    assert_eq!(
        broken,
        reference("Straße", "strasse"),
        "ASCII folding loses Unicode matches"
    );
}

#[test]
#[should_panic(expected = "search must have linear work")]
fn rescanning_prefixes_is_detected_negative_control() {
    let text = format!("{}b", "a".repeat(128));
    let query = format!("{}b", "a".repeat(64));
    let mut work = 0;
    for window in text.as_bytes().windows(query.len()) {
        for (left, right) in window.iter().zip(query.as_bytes()) {
            work += 1;
            if left != right {
                break;
            }
        }
    }
    assert!(
        work <= 3 * (text.len() + query.len()),
        "search must have linear work"
    );
}
