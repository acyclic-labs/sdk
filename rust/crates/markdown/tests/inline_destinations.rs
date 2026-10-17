use super::*;
use proptest::prelude::*;

fn oracle(source: &str) -> Option<(usize, String)> {
    let probe = format!("[x]{source}");
    pulldown_cmark::Parser::new_ext(&probe, crate::options())
        .into_offset_iter()
        .find_map(|(event, range)| match event {
            crate::Event::Start(crate::Tag::Link {
                dest_url,
                link_type: pulldown_cmark::LinkType::Inline,
                ..
            }) if range.start == 0 => Some((range.end - 3, dest_url.into_string())),
            _ => None,
        })
}

fn compare(source: &str, cuts: &[usize]) {
    let mut suffix = Suffix::default();
    for end in cuts.iter().copied().chain([source.len()]) {
        let prefix = source.get(..end).expect("prefix boundary");
        let actual = suffix
            .push(prefix)
            .and_then(|end| suffix.target(prefix).map(|url| (end, url.to_string())));
        assert_eq!(actual, oracle(prefix), "prefix {prefix:?}");
    }
}

#[test]
fn multiline_title_resumes_at_the_inline_node() {
    for source in ["(<u> (\n\\))", "(<u> '\r\n  \\')", "(<u> \"\n\t\\\")"] {
        let cuts: Vec<_> = (0..source.len()).collect();
        compare(source, &cuts);
        compare_index(&format!("]{source}"), &cuts);
    }
    let source = "(<u> (\n\\))";
    let mut broken = Suffix {
        keep_title_escape: true,
        ..Suffix::default()
    };
    assert_ne!(broken.push(source), oracle(source).map(|(end, _)| end));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn destination_state_matches_full_parse_at_every_split(
        parts in prop::collection::vec(prop_oneof![
            any::<char>().prop_map(String::from),
            prop::sample::select(vec![
                "(", ")", "<", ">", "\\", "\\(", "\\)", "\\<", "\\>",
                "u", " ", "\t", "\n", "\r\n", "\u{b}", "\0", "'", "\"", "&amp;", "&#40;", "&#41;",
                "<u>", "(u)", "a(b)c", " \'title\'", " \"title\"", " (title)",
            ]).prop_map(str::to_owned),
        ], 0..48),
        cuts in prop::collection::vec(any::<usize>(), 0..48),
    ) {
        let source = format!("({})tail", parts.concat());
        let mut cuts: Vec<_> = cuts.into_iter().map(|cut| cut % (source.len()+1))
            .filter(|cut| source.is_char_boundary(*cut)).collect();
        cuts.sort_unstable();
        cuts.dedup();
        compare(&source, &cuts);
        compare(&source, &source.char_indices().map(|(at,_)| at).collect::<Vec<_>>());
    }
}

fn used(count: usize, replay: bool) -> (usize, usize) {
    let mut suffix = Suffix {
        replay,
        ..Suffix::default()
    };
    let mut source = String::from("(<");
    let before = crate::work::current();
    for _ in 0..count {
        source.push_str("x\\> ");
        assert_eq!(suffix.push(&source), None);
    }
    source.push_str("> 'title')");
    assert_eq!(suffix.push(&source), Some(source.len()));
    assert_eq!(
        suffix.target(&source).as_deref(),
        oracle(&source).map(|(_, url)| url).as_deref()
    );
    (crate::work::current() - before, source.len())
}

#[test]
fn destination_feed_and_validation_are_linear() {
    for count in [64, 128, 256, 1024] {
        let small = used(count, false);
        let large = used(count * 2, false);
        assert!(large.0 <= 128 * large.1 + 4096, "{small:?} -> {large:?}");
        assert!(large.0 <= small.0 * 2 + 4096, "{small:?} -> {large:?}");
    }
}

#[test]
fn destination_properties_detect_replay_and_lost_escape_state() {
    let small = used(64, true);
    let large = used(128, true);
    assert!(large.0 > small.0 * 2 + 4096);
    let source = r"(u\)v)";
    let mut suffix = Suffix {
        forget_escape: true,
        ..Suffix::default()
    };
    let end = suffix.push(source);
    assert_ne!(end, oracle(source).map(|(end, _)| end));
}

fn compare_index(source: &str, cuts: &[usize]) {
    let mut index = Index::default();
    let mut completed = std::collections::HashMap::new();
    for end in cuts.iter().copied().chain([source.len()]) {
        let prefix = source.get(..end).expect("prefix boundary");
        index.push(prefix, 0);
        completed.extend(index.finished.iter().copied());
        for (at, _) in prefix.match_indices("](") {
            let start = at + 1;
            let expected = oracle(prefix.get(start..).expect("destination start"))
                .map(|(end, url)| (start + end, url));
            let actual = completed.get(&start).and_then(|&end| {
                index
                    .target(prefix, start, end)
                    .map(|url| (end, url.to_string()))
            });
            assert_eq!(actual, expected, "prefix {prefix:?}, candidate {start}");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn retained_destination_index_matches_independent_suffix_parses(
        parts in prop::collection::vec(prop_oneof![
            any::<char>().prop_map(String::from),
            prop::sample::select(vec!["](", "](<", "](<u> '", "](<u> \"", "]((", "](<u> (", "u)", "\\)", "\\\\", "[", "]", ")", "\n", "\r\n", "'", "\"", "<", ">", " ", "a(b)c", "![x](u)", "[x](u)"]).prop_map(str::to_owned),
        ], 0..48),
        cuts in prop::collection::vec(any::<usize>(), 0..48),
    ) {
        let source = parts.concat();
        let mut cuts: Vec<_> = cuts.into_iter().map(|at| at % (source.len()+1)).filter(|at| source.is_char_boundary(*at)).collect();
        cuts.sort_unstable();
        cuts.dedup();
        compare_index(&source, &cuts);
        compare_index(&source, &source.char_indices().map(|(at,_)| at).collect::<Vec<_>>());
    }
}

fn indexed_work(fragments: &[&str], count: usize, replay: bool) -> (usize, usize, usize) {
    let mut index = Index {
        replay,
        ..Index::default()
    };
    let mut source = String::new();
    let before = crate::work::current();
    for fragment in fragments.iter().cycle().take(count) {
        source.push_str(fragment);
        index.push(&source, 0);
    }
    let work = crate::work::current() - before;
    let (bytes, metadata) = index.retained(&mut Default::default());
    (work, bytes + metadata, source.len())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn retained_candidate_feed_and_storage_are_linear(
        fragments in prop::collection::vec(prop::sample::select(vec![
            "](", "](<", "](<u> '", "](<u> \"", "]((", "](<u> (",
            "]((x)", "]()", "x", " ", "\\", "\\)", ")", "]", "(", "'", "\"", ">", "\n", "\u{1f980}",
        ]), 1..5),
    ) {
        for count in [64,128,256,1024] {
            let small = indexed_work(&fragments, count, false);
            let large = indexed_work(&fragments, count * 2, false);
            prop_assert!(large.0 <= 128 * large.2 + 4096, "{:?}: {:?} -> {:?}", fragments, small, large);
            prop_assert!(large.1 <= 128 * large.2 + 4096, "{:?}: {:?} -> {:?}", fragments, small, large);
            prop_assert!(large.0 <= 2 * small.0 + 4096, "{:?}: {:?} -> {:?}", fragments, small, large);
            prop_assert!(large.1 <= 2 * small.1 + 4096, "{:?}: {:?} -> {:?}", fragments, small, large);
        }
    }
}

#[test]
fn index_properties_detect_replay_and_lost_escape_state() {
    let small = indexed_work(&["](<", "x"], 64, true);
    let large = indexed_work(&["](<", "x"], 128, true);
    assert!(large.0 > 2 * small.0 + 4096);
    let source = r"](u\)v)";
    let mut index = Index {
        forget_escape: true,
        ..Index::default()
    };
    for end in 1..=source.len() {
        index.push(source.get(..end).expect("prefix boundary"), 0);
    }
    assert_ne!(
        index
            .finished
            .iter()
            .find(|(start, _)| *start == 1)
            .map(|(_, end)| *end),
        oracle(source.get(1..).expect("after bracket")).map(|(end, _)| 1 + end)
    );
}
