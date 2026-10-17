//! The production completion gate rejects all obsolete layout epochs.
use super::*;
use proptest::{
    prelude::*,
    test_runner::{Config, TestCaseResult, TestError, TestRunner},
};

struct Model {
    selected: u8,
    generation: u64,
    active: [Option<u64>; 2],
    completions: Vec<(usize, u64)>,
}
fn agent(index: usize) -> StreamPath {
    StreamPath::new(format!("swarm/{index}")).unwrap()
}
fn layout(queue: &mut Snapshots<'static>, model: &mut Model, selected: u8) -> TestCaseResult {
    let wanted = ClientInterests::from_layout(
        StreamPath::new("swarm").unwrap(),
        (0..2)
            .filter(|index| selected & (1 << index) != 0)
            .map(agent),
        [],
        2,
    )
    .unwrap();
    queue.layout(&wanted).unwrap();
    model.generation += 1;
    for index in 0..2 {
        let bit = 1 << index;
        if selected & bit == 0 {
            model.active[index] = None;
        }
        if selected & bit != 0 && model.selected & bit == 0 {
            model.active[index] = Some(model.generation);
            model.completions.push((index, model.generation));
            queue
                .start(agent(index), futures::future::pending())
                .unwrap();
        }
    }
    model.selected = selected;
    prop_assert_eq!(queue.active.len(), model.active.iter().flatten().count());
    prop_assert!(queue.next().now_or_never().is_none());
    prop_assert_eq!(queue.futures.len(), model.active.iter().flatten().count());
    Ok(())
}
fn complete(
    queue: &mut Snapshots<'static>,
    model: &mut Model,
    index: usize,
    generation: u64,
    broken: bool,
) -> TestCaseResult {
    let path = agent(index);
    // A completed future has left FuturesUnordered before dispatch. Its result
    // may then race a demand transition; remove only that exact source epoch.
    if queue
        .active
        .get(&path)
        .is_some_and(|owner| owner.generation == generation)
    {
        // The real next() returns this source's completion before finish().
        queue.active.get(&path).unwrap().abort.abort();
        assert!(queue.next().now_or_never().is_none());
    }
    let expected = model.active[index] == Some(generation);
    let accepted = if broken {
        // Missing-fence mutant accepts any active request for this agent.
        queue.active.remove(&path).is_some()
    } else {
        queue
            .finish((
                path,
                generation,
                Err(crate::task_volume::Error::InvalidSegment),
            ))
            .is_some()
    };
    prop_assert_eq!(
        accepted,
        expected,
        "an obsolete snapshot passed the layout fence"
    );
    if expected {
        model.active[index] = None;
    }
    Ok(())
}
fn property(actions: &[u8], broken: bool) -> TestCaseResult {
    let mut queue = Snapshots::new(2);
    let mut model = Model {
        selected: 0,
        generation: 0,
        active: [None, None],
        completions: Vec::new(),
    };
    for &action in actions {
        match action % 3 {
            0 => layout(&mut queue, &mut model, (action >> 2) & 3)?,
            1 if !model.completions.is_empty() => {
                let (index, generation) =
                    model.completions[usize::from(action >> 2) % model.completions.len()];
                complete(&mut queue, &mut model, index, generation, broken)?;
            }
            _ => {
                let selected = model.selected;
                layout(&mut queue, &mut model, 0)?;
                layout(&mut queue, &mut model, selected)?;
            }
        }
    }
    // Always include a delayed result across clear/reopen of the same agent.
    layout(&mut queue, &mut model, 0)?;
    layout(&mut queue, &mut model, 1)?;
    let old = model.generation;
    layout(&mut queue, &mut model, 0)?;
    layout(&mut queue, &mut model, 1)?;
    complete(&mut queue, &mut model, 0, old, broken)?;
    let current = model.generation;
    complete(&mut queue, &mut model, 0, current, broken)?;
    prop_assert!(queue.next().now_or_never().is_none());
    prop_assert!(queue.active.is_empty() && queue.futures.is_empty());
    Ok(())
}
fn histories() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..32)
}
proptest! {
    #[test]
    fn snapshot_sources_and_completion_epochs_match_demand(actions in histories()) {
        property(&actions, false)?;
    }
}
#[test]
fn negative_control_omitting_the_completion_fence_shrinks_to_clear_reopen() {
    let mut runner = TestRunner::new(Config {
        cases: 32,
        failure_persistence: None,
        ..Config::default()
    });
    let inputs = prop_oneof![Just(Vec::new()), histories().no_shrink()];
    let result = runner.run(&inputs, |actions| property(&actions, true));
    assert!(
        matches!(result, Err(TestError::Fail(_, ref actions)) if actions.is_empty()),
        "missing fence escaped or did not shrink: {result:?}"
    );
}
