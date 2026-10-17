use super::*;
use proptest::prelude::*;

type Model = Vec<(usize, Range<usize>)>;

fn snapshot(code: usize, frontier: usize) -> Snapshot {
    let mut state = super::super::State::default();
    state.codes.head = code;
    let mut before = state.snapshot();
    before.frontier = frontier;
    before
}

fn agrees(codes: &Codes, model: &Model) -> bool {
    codes.head() == model.last().map_or(0, |entry| entry.0)
        && codes.earliest() == model.first().map(|entry| entry.1.start)
        && (1..=8).all(|width| {
            codes.get(width).map(|code| &code.opening)
                == model
                    .iter()
                    .find(|entry| entry.1.len() == width)
                    .map(|entry| &entry.1)
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn code_context_branches_match_owned_stacks(
        actions in prop::collection::vec((any::<bool>(), 1usize..=8, any::<u8>()), 0..96),
    ) {
        let mut codes = Codes::default();
        let mut model = Model::new();
        let mut saved = vec![(0, model.clone())];
        let before = crate::work::current();
        let mut changes = 0;
        for (step, (restore, width, choice)) in actions.iter().copied().enumerate() {
            if restore {
                let (cursor, target) = saved.get(usize::from(choice) % saved.len()).expect("saved code context");
                let common = model.iter().zip(target).take_while(|(a,b)| a.0 == b.0).count();
                changes += model.len() + target.len() - 2 * common;
                prop_assert!(codes.restore(*cursor));
                model = target.clone();
            } else if let Some(at) = model.iter().position(|entry| entry.1.len() == width) {
                changes += model.len() - at;
                model.truncate(at);
                prop_assert!(codes.restore(model.last().map_or(0, |entry| entry.0)));
            } else {
                let opening = step * 16..step * 16 + width;
                codes.push(opening.clone(), snapshot(codes.head(), opening.start));
                model.push((codes.head(), opening));
                changes += 1;
            }
            prop_assert!(agrees(&codes, &model), "step {}, expected {:?}", step, model);
            saved.push((codes.head(), model.clone()));
        }
        let used = crate::work::current() - before;
        prop_assert!(used <= 128 * (actions.len() + changes) + 4096,
            "{used} work for {} operations and {changes} changed records", actions.len());
        prop_assert!(codes.retained() <= 512 * (actions.len() + 1) + 4096);
        prop_assert!(!codes.restore(codes.length + 1));
        prop_assert!(agrees(&codes, &model));
    }
}

#[test]
fn code_context_model_detects_missing_lookup_restoration() {
    let mut codes = Codes::default();
    let mut model = Model::new();
    for width in [1, 2] {
        let opening = width * 16..width * 16 + width;
        codes.push(opening.clone(), snapshot(codes.head(), opening.start));
        model.push((codes.head(), opening));
    }
    let cursor = codes.head();
    assert!(codes.restore(0));
    codes.stale = true;
    assert!(codes.restore(cursor));
    codes.stale = false;
    assert!(!agrees(&codes, &model));
}
