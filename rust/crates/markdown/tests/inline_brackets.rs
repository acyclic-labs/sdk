use super::*;
use crate::inline_scan::tokens::{Kind, Tokens};
use proptest::prelude::*;

type Labels = Vec<(Range<usize>, bool)>;

fn actual(state: &State, pending: Option<&crate::inline_scan::tokens::Token>) -> Labels {
    let mut labels = Vec::new();
    let mut cursor = state.closed_at(pending);
    while let Some(label) = state.label(cursor) {
        if label.kind != Closure::Reference {
            labels.push((label.range.clone(), label.kind == Closure::Image));
        }
        cursor = label.previous;
    }
    labels.reverse();
    labels
}

fn expected(source: &str) -> Labels {
    let codes: Vec<_> = pulldown_cmark::Parser::new_ext(source, crate::options())
        .into_offset_iter()
        .filter_map(|(event, range)| matches!(event, crate::Event::Code(_)).then_some(range))
        .collect();
    let mut tokens = Tokens::default();
    tokens.push(source);
    let mut stack = Vec::new();
    let mut labels = Vec::new();
    for token in tokens.ready {
        if codes
            .iter()
            .any(|code| code.start < token.range.end && token.range.start < code.end)
        {
            continue;
        }
        let opening = match token.kind {
            Kind::Open { image } => Some((token.range.start, image)),
            _ => None,
        };
        if let Some(opening) = opening {
            stack.push(opening);
        } else if token.kind == Kind::Close
            && let Some((start, image)) = stack.pop()
        {
            labels.push((start..token.range.end, image));
        }
    }
    labels
}

fn feed(state: &mut State, tokens: &mut Tokens, source: &str) {
    tokens.push(source);
    while let Some(token) = tokens.ready.pop_front() {
        state.feed(token.range, token.kind, false);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn retained_label_closures_match_code_filtered_brackets(
        parts in prop::collection::vec(prop_oneof![
            any::<char>().prop_filter("inline character", |c| !matches!(c, '\n'|'\r')).prop_map(String::from),
            prop::sample::select(vec!["[", "]", "![", "[x]", "![x]", "[[a]b]", "`", "``", "\\`", "\\``", "\\[", "\\]", "\\", "*", "_", " ", "x"]).prop_map(str::to_owned),
        ], 0..48),
        cuts in prop::collection::vec(any::<usize>(), 0..48),
    ) {
        let source = format!("x {} z", parts.concat());
        let mut cuts: Vec<_> = cuts.into_iter().map(|cut| cut % (source.len()+1)).filter(|cut| source.is_char_boundary(*cut)).collect();
        cuts.push(source.len()); cuts.sort_unstable(); cuts.dedup();
        let mut state = State::default(); let mut tokens = Tokens::default();
        for end in cuts {
            feed(&mut state, &mut tokens, source.get(..end).expect("prefix boundary"));
            let before = state.snapshot();
            prop_assert_eq!(actual(&state, tokens.preview().as_ref()), expected(source.get(..end).expect("prefix boundary")), "prefix {:?}", source.get(..end).expect("prefix boundary"));
            prop_assert_eq!(state.snapshot(), before, "EOF labels mutated settled state");
        }
    }
}

#[test]
fn label_model_detects_not_restoring_closures_after_code() {
    let mut state = State::default();
    let mut tokens = Tokens::default();
    let source = "x [a] ` [b] ` z";
    feed(&mut state, &mut tokens, "x [a] ` [b] ");
    FORGET_RESTORE.set(true);
    feed(&mut state, &mut tokens, source);
    FORGET_RESTORE.set(false);
    assert_ne!(actual(&state, None), expected(source));
}

fn work(count: usize, rescan: bool) -> (usize, usize, usize) {
    let mut state = State {
        rescan,
        ..State::default()
    };
    let mut tokens = Tokens::default();
    let mut source = String::from("x ");
    let before = crate::work::current();
    for _ in 0..count {
        source.push_str("[![x]y] ");
        feed(&mut state, &mut tokens, &source);
    }
    (
        crate::work::current() - before,
        state.retained() + tokens.retained(),
        source.len(),
    )
}

#[test]
fn closure_history_work_and_storage_are_linear() {
    for count in [64, 128, 256, 1024] {
        let small = work(count, false);
        let large = work(count * 2, false);
        assert!(large.0 <= 128 * large.2 + 4096, "{small:?} -> {large:?}");
        assert!(large.1 <= 512 * large.2 + 4096, "{small:?} -> {large:?}");
        assert!(large.0 <= 2 * small.0 + 4096, "{small:?} -> {large:?}");
        assert!(large.1 <= 2 * small.1 + 4096, "{small:?} -> {large:?}");
    }
}

#[test]
fn closure_work_model_detects_scanning_retained_history() {
    let small = work(64, true);
    let large = work(128, true);
    assert!(large.0 > 128 * large.2 + 4096);
    assert!(large.0 > 2 * small.0 + 4096);
}
