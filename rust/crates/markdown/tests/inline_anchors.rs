use super::*;
use crate::inline_scan::tokens::Tokens;
use proptest::prelude::*;

fn ordinals(anchor: &Anchor) -> [usize; 4] {
    [
        anchor.delimiter(),
        anchor.forest(),
        anchor.bracket(),
        anchor.closed(),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn compact_anchors_preserve_every_ordinal(values in any::<[usize; 4]>()) {
        let context = State::default().resume().context;
        let anchor = Anchor::new(context, values);
        prop_assert_eq!(ordinals(&anchor), values);
        let snapshot = anchor.clone();
        prop_assert_eq!(ordinals(&snapshot), values);
        prop_assert_eq!(ordinals(&anchor), ordinals(&snapshot));
    }
}

#[cfg(target_pointer_width = "64")]
#[test]
fn ordinal_property_detects_truncating_large_paragraphs() {
    let context = State::default().resume().context;
    let values = [u32::MAX as usize + 1; 4];
    let mut anchor = Anchor::new(context, values);
    assert_eq!(ordinals(&anchor), values);
    std::sync::Arc::make_mut(&mut anchor.context).wide = None;
    assert_ne!(ordinals(&anchor), values);
}

fn feed(state: &mut State, source: &str, start: usize) {
    let mut tokens = Tokens::at(
        start,
        source
            .get(..start)
            .and_then(|prefix| prefix.chars().next_back()),
    );
    tokens.push(source);
    assert!(tokens.neutral(), "model ends in ordinary text");
    for token in tokens.ready {
        state.feed(token.range, token.kind, false);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn opener_restore_matches_replaying_the_prefix(
        prefix in prop::collection::vec(prop::sample::select(vec![
            "x", " ", "*", "**", "_", "~~", "`", "``", "\\", "\\*", "!", "[", "]", "<", ">", "&amp;",
        ]), 0..32),
        first in prop::collection::vec(prop::sample::select(vec![
            "x", " ", "*", "**", "_", "~~", "\\", "\\*", "!", "[", "]", "<", ">", "&amp;",
        ]), 0..32),
        second in prop::collection::vec(prop::sample::select(vec![
            "x", " ", "*", "**", "_", "~~", "`", "``", "\\", "\\*", "!", "[", "]", "<", ">", "&amp;",
        ]), 0..32),
    ) {
        let prefix = format!("x {}x", prefix.concat());
        let mut retained = State::default();
        feed(&mut retained, &prefix, 0);
        retained.remember(prefix.len());
        feed(&mut retained, &format!("{prefix}{}x", first.concat()), prefix.len());
        prop_assert_eq!(retained.restore_at(prefix.len()), Some(()));
        let source = format!("{prefix}{}x", second.concat());
        feed(&mut retained, &source, prefix.len());
        let mut replay = State::default();
        feed(&mut replay, &source, 0);
        prop_assert_eq!(retained.forest.rendered(&source), replay.forest.rendered(&source),
            "source {:?}", source);
    }
}

#[test]
fn removed_code_context_restores_the_original_prefix() {
    let prefix = "x `a x";
    let mut state = State::default();
    feed(&mut state, prefix, 0);
    state.remember(prefix.len());
    feed(&mut state, &format!("{prefix}` x ``b x"), prefix.len());
    assert_eq!(state.restore_at(prefix.len()), Some(()));
    let source = format!("{prefix}new` x");
    feed(&mut state, &source, prefix.len());
    let mut replay = State::default();
    feed(&mut replay, &source, 0);
    assert_eq!(
        state.forest.rendered(&source),
        replay.forest.rendered(&source)
    );
}

#[test]
fn opener_model_detects_missing_restore() {
    let prefix = "x *p x";
    let mut state = State::default();
    feed(&mut state, prefix, 0);
    state.remember(prefix.len());
    feed(&mut state, &format!("{prefix}**old** x"), prefix.len());
    SKIP_RESTORE.set(true);
    assert_eq!(state.restore_at(prefix.len()), Some(()));
    SKIP_RESTORE.set(false);
    let source = format!("{prefix}new x");
    feed(&mut state, &source, prefix.len());
    let mut replay = State::default();
    feed(&mut replay, &source, 0);
    assert_ne!(
        state.forest.rendered(&source),
        replay.forest.rendered(&source)
    );
}
