use super::*;
use crate::inline_scan::{
    delimiters::{Delimiters, Run},
    tokens::{Kind as TokenKind, Tokens},
};
use proptest::prelude::*;

fn append(forest: &mut Forest, delimiters: &mut Delimiters, source: &str, offset: usize) {
    let mut tokens = Tokens::at(
        offset,
        source
            .get(..offset)
            .and_then(|prefix| prefix.chars().next_back()),
    );
    tokens.push(source);
    assert!(tokens.neutral(), "test source ends in ordinary text");
    for token in &tokens.ready {
        let cursor = forest.text(token.range.clone());
        if let TokenKind::Run {
            byte,
            before,
            after,
        } = token.kind
        {
            let mut run = Run::new(token.range.clone(), byte, before, after);
            run.cursor = cursor;
            delimiters.feed(run, |matched| {
                forest
                    .wrap(&matched)
                    .expect("active opener and closer cursors")
            });
        }
    }
}

fn full(source: &str) -> crate::Line {
    let events: Vec<_> = pulldown_cmark::Parser::new_ext(source, crate::options()).collect();
    let mut lines = crate::render::render_styled(&events, crate::Style::default());
    assert_eq!(lines.len(), 1);
    lines.pop().expect("one paragraph")
}

fn parts() -> impl Strategy<Value = Vec<&'static str>> {
    prop::collection::vec(
        prop::sample::select(vec![
            "*", "**", "***", "****", "_", "__", "___", "~", "~~", "~~~", "x", " ", ".", "!", "¡",
            "界", "🦀", "…",
        ]),
        0..64,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inline_forest.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn shared_forest_and_restored_branches_match_full_render(
        prefix in parts(), first in parts(), second in parts(),
    ) {
        let prefix = format!("x {} x", prefix.concat());
        let first = format!("{prefix}{} x", first.concat());
        let second = format!("{prefix}{} x", second.concat());
        let mut forest = Forest::default();
        let mut delimiters = Delimiters::default();
        append(&mut forest, &mut delimiters, &prefix, 0);
        let root = forest.snapshot();
        let state = delimiters.snapshot();
        prop_assert_eq!(forest.rendered(&prefix), Some(full(&prefix)));
        append(&mut forest, &mut delimiters, &first, prefix.len());
        let first_root = forest.snapshot();
        prop_assert_eq!(forest.rendered(&first), Some(full(&first)));
        prop_assert!(forest.restore(root));
        delimiters.restore(state);
        prop_assert_eq!(forest.rendered(&prefix), Some(full(&prefix)));
        append(&mut forest, &mut delimiters, &second, prefix.len());
        prop_assert_eq!(forest.rendered(&second), Some(full(&second)));
        prop_assert!(forest.restore(first_root));
        prop_assert_eq!(forest.rendered(&first), Some(full(&first)));
    }
}

fn measure(count: usize, scan: bool) -> (usize, usize) {
    let source = format!("*a {}z", "**x** ".repeat(count));
    let mut forest = Forest {
        scan,
        ..Forest::default()
    };
    let mut delimiters = Delimiters::default();
    let before = crate::work::current();
    append(&mut forest, &mut delimiters, &source, 0);
    let root = forest.snapshot();
    let state = delimiters.snapshot();
    let start = source.len();
    for _ in 0..count {
        assert!(forest.restore(root));
        delimiters.restore(state);
        let cursor = forest.text(start..start + 1);
        let mut run = Run::new(start..start + 1, b'*', 'z', ' ');
        run.cursor = cursor;
        delimiters.feed(run, |matched| forest.wrap(&matched).expect("shared opener"));
    }
    (
        crate::work::current() - before,
        forest.retained() + delimiters.retained(),
    )
}

#[test]
fn wrapping_shared_history_has_linear_work_and_storage() {
    for size in [64, 128, 256, 1024] {
        let small = measure(size, false);
        let large = measure(size * 2, false);
        assert!(
            large.0 <= 2 * small.0 + 1024,
            "forest work {} -> {}",
            small.0,
            large.0
        );
        assert!(large.1 <= 2 * small.1 + 1024);
    }
}

#[test]
fn work_property_detects_searching_the_old_forest() {
    let small = measure(64, true);
    let large = measure(128, true);
    assert!(
        large.0 > 2 * small.0 + 1024,
        "negative control escaped the forest work property: {} -> {}",
        small.0,
        large.0
    );
}

#[test]
fn render_property_detects_reusing_a_partial_opener_cursor() {
    let source = "x ***x**y* x";
    let mut forest = Forest {
        stale: true,
        ..Forest::default()
    };
    let mut delimiters = Delimiters::default();
    append(&mut forest, &mut delimiters, source, 0);
    assert_ne!(
        forest.rendered(source),
        Some(full(source)),
        "the negative control escaped the render property"
    );
}
