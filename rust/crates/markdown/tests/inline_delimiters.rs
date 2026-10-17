use super::*;
use proptest::prelude::*;

#[derive(Clone)]
struct Open {
    range: Range<usize>,
    original: usize,
    byte: u8,
    both: bool,
}

#[derive(Clone, Default)]
struct Model(Vec<Open>, usize);

impl Model {
    fn feed(&mut self, run: Run) -> Vec<Matched> {
        let original = run.range.len();
        if original == 0 || run.byte == b'~' && original > 2 {
            return Vec::new();
        }
        let both = run.open && run.close;
        let mut remaining = run.range;
        let mut found = Vec::new();
        while run.close && !remaining.is_empty() {
            let candidate = self.0.iter().enumerate().rfind(|(index, entry)| {
                entry.byte == run.byte
                    && (run.byte != b'~' || *index >= self.1 && entry.original == original)
                    && (!both && !entry.both
                        || !(entry.original + original).is_multiple_of(3)
                        || original.is_multiple_of(3))
            });
            let Some((index, _)) = candidate else {
                if run.byte == b'~' {
                    self.1 = self.0.len();
                }
                break;
            };
            let mut opener = self.0.get(index).expect("matching opener").clone();
            self.0.truncate(index);
            self.1 = self.1.min(index);
            let take = opener.range.len().min(remaining.len());
            found.push(Matched {
                opener: opener.range.end - take..opener.range.end,
                closer: remaining.start..remaining.start + take,
                byte: run.byte,
                opener_cursor: 0,
                closer_cursor: 0,
            });
            opener.range.end -= take;
            if !opener.range.is_empty() {
                self.0.push(opener);
            }
            remaining.start += take;
        }
        if run.open && !remaining.is_empty() {
            self.0.push(Open {
                range: remaining,
                original,
                byte: run.byte,
                both,
            });
        }
        found
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inline_delimiters.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn delimiter_snapshots_match_the_stack_model(
        commands in prop::collection::vec((0u8..3, 1usize..17, 0u8..4, any::<u8>()), 0..128),
    ) {
        let mut actual = Delimiters::default();
        let mut expected = Model::default();
        let mut saved = vec![(actual.snapshot(), expected.clone())];
        let mut position = 0;
        for (marker, count, flags, rewind) in commands {
            if rewind % 5 == 0 {
                let index = usize::from(rewind) % saved.len();
                let (snapshot, model) = saved.get(index).expect("saved parser state");
                actual.restore(*snapshot);
                expected = model.clone();
            }
            let byte = b"*_~".get(usize::from(marker)).copied().expect("marker");
            let run = Run { range: position..position + count, byte, open: flags & 1 != 0, close: flags & 2 != 0, cursor: 0 };
            position += count + 1;
            let mut found = Vec::new();
            actual.feed(run.clone(), |matched| { found.push(matched); (0, 0) });
            prop_assert_eq!(found, expected.feed(run));
            prop_assert_eq!(actual.first(), expected.0.first().map(|entry| entry.range.start));
            prop_assert_eq!(actual.neutral(), expected.0.is_empty());
            saved.push((actual.snapshot(), expected.clone()));
        }
    }

    #[test]
    fn delimiter_matches_agree_with_the_full_parser(
        parts in prop::collection::vec(prop::sample::select(vec![
            "*", "**", "***", "****", "_", "__", "___", "~", "~~", "~~~", "x", " ", ".", "!", "¡", "界", "🦀", "…", "\u{1bc9f}", "\u{1bca0}",
        ]), 0..64),
    ) {
        use crate::inline_scan::tokens::{Kind, Tokens};
        use pulldown_cmark::{Event, Parser, Tag};
        let source = format!("x {} x", parts.concat());
        let mut tokens = Tokens::default();
        tokens.push(&source);
        let mut actual = Vec::new();
        let mut stack = Delimiters::default();
        for token in &tokens.ready {
            if let Kind::Run { byte, before, after } = token.kind {
                let run = Run::new(token.range.clone(), byte, before, after);
                stack.feed(run, |mut matched| {
                    while !matched.opener.is_empty() {
                        let count = matched.opener.len().min(2);
                        let kind = if matched.byte == b'~' { 2 } else { usize::from(count == 2) };
                        actual.push((matched.opener.end - count, matched.closer.start + count, kind));
                        matched.opener.end -= count;
                        matched.closer.start += count;
                    }
                    (0, 0)
                });
            }
        }
        let mut expected: Vec<_> = Parser::new_ext(&source, crate::options()).into_offset_iter()
            .filter_map(|(event, range)| match event {
                Event::Start(Tag::Emphasis) => Some((range.start, range.end, 0)),
                Event::Start(Tag::Strong) => Some((range.start, range.end, 1)),
                Event::Start(Tag::Strikethrough) => Some((range.start, range.end, 2)),
                _ => None,
            }).collect();
        actual.sort_unstable();
        expected.sort_unstable();
        prop_assert_eq!(actual, expected);
    }
}

fn punctuation_batch(source: &str, characters: &[char]) {
    use pulldown_cmark::{Event, Parser, Tag, TagEnd};
    let (mut index, mut emphasis) = (0, false);
    for event in Parser::new(source) {
        match event {
            Event::Start(Tag::Paragraph) => emphasis = false,
            Event::Start(Tag::Emphasis) => emphasis = true,
            Event::End(TagEnd::Paragraph) => {
                let character = *characters.get(index).expect("independent probe paragraph");
                assert_eq!(
                    punctuation(character),
                    !emphasis,
                    "U+{:04X}",
                    u32::from(character)
                );
                index += 1;
            }
            _ => {}
        }
    }
    assert_eq!(index, characters.len());
}

#[test]
fn all_unicode_punctuation_matches_the_pinned_parser() {
    let mut source = String::new();
    let mut characters = Vec::new();
    for code in 0..=u32::from(char::MAX) {
        let Some(character) = char::from_u32(code) else {
            continue;
        };
        if character.is_ascii() || character.is_whitespace() {
            assert_eq!(punctuation(character), character.is_ascii_punctuation());
            continue;
        }
        source.push_str("x*");
        source.push(character);
        source.push_str("*\n\n");
        characters.push(character);
        if characters.len() == 256 {
            punctuation_batch(&source, &characters);
            source.clear();
            characters.clear();
        }
    }
    punctuation_batch(&source, &characters);
}

fn measure(count: usize, scan: bool) -> (usize, usize) {
    let before = crate::work::current();
    let mut stack = Delimiters {
        scan,
        ..Delimiters::default()
    };
    let mut position = 0;
    for _ in 0..count {
        stack.feed(
            Run {
                range: position..position + 1,
                byte: b'*',
                open: true,
                close: false,
                cursor: 0,
            },
            |_| (0, 0),
        );
        position += 3;
    }
    for _ in 0..count {
        stack.feed(
            Run {
                range: position..position + 2,
                byte: b'*',
                open: true,
                close: true,
                cursor: 0,
            },
            |_| (0, 0),
        );
        position += 4;
    }
    (crate::work::current() - before, stack.retained())
}

#[test]
fn delimiter_candidate_queries_have_linear_work_and_storage() {
    for size in [64, 128, 256, 1024] {
        let (small, small_storage) = measure(size, false);
        let (large, large_storage) = measure(size * 2, false);
        assert!(
            large <= 2 * small + 1024,
            "candidate work {small} -> {large}"
        );
        assert!(large_storage <= 2 * small_storage + 1024);
    }
}

#[test]
fn work_property_detects_scanning_old_openers() {
    let (small, _) = measure(64, true);
    let (large, _) = measure(128, true);
    assert!(
        large > 2 * small + 1024,
        "negative control escaped the work property: {small} -> {large}"
    );
}
