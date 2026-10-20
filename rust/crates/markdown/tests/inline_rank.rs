use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn bit_prefixes_match_counting(bits in prop::collection::vec(any::<bool>(), 0..512)) {
        let mut rank = Rank::default();
        let mut model = vec![0];
        let mut last = vec![None];
        for bit in bits {
            rank.push(bit);
            last.push(if bit { Some(last.len() - 1) } else { last.last().copied().flatten() });
            model.push(model.last().copied().expect("initial count") + usize::from(bit));
            prop_assert_eq!(rank.before(model.len() - 1), model.last().copied());
        }
        for (position, expected) in model.iter().enumerate() {
            prop_assert_eq!(rank.before(position), Some(*expected));
            prop_assert_eq!(rank.last_before(position), *last.get(position).expect("model position"));
        }
        prop_assert_eq!(rank.before(model.len()), None);
        prop_assert_eq!(rank.last_before(model.len()), None);
    }

    #[test]
    fn normalized_code_views_preserve_source_offsets(
        chars in prop::collection::vec(prop_oneof![
            any::<char>(), prop::sample::select(vec![' ', '\r', '\n', '\0', '🦀']),
        ], 0..64),
        left in any::<u8>(), right in any::<u8>(),
    ) {
        let source: String = chars.into_iter().collect();
        let mut input = super::super::Input::default();
        input.feed("", 0);
        for (offset, character) in source.char_indices() {
            input.feed(source.get(..offset + character.len_utf8()).expect("character boundary"), 0);
        }
        let cuts: Vec<_> = source.char_indices().map(|(offset, _)| offset).chain([source.len()]).collect();
        let (left, right) = (*cuts.get(usize::from(left) % cuts.len()).expect("left cut"), *cuts.get(usize::from(right) % cuts.len()).expect("right cut"));
        let range = left.min(right)..left.max(right);
        let normalized = source.get(range.clone()).expect("selected range").replace(['\r', '\n'], " ");
        let expected = if normalized.starts_with(' ') && normalized.ends_with(' ')
            && normalized.bytes().any(|byte| byte != b' ')
        { normalized.get(1..normalized.len() - 1).expect("space boundaries") } else { &normalized };
        let actual = input.code(range, &source).expect("source view");
        prop_assert_eq!(actual.as_str(), expected);
    }
}

#[test]
fn rank_model_detects_missing_word_prefixes() {
    let mut rank = Rank::default();
    rank.push(true);
    for _ in 0..129 {
        rank.push(false);
    }
    OMIT_PREFIX.set(true);
    assert_eq!(rank.before(65), Some(0));
    OMIT_PREFIX.set(false);
    assert_eq!(rank.before(65), Some(1));
}
