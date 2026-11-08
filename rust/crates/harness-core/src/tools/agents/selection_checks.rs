//! Invariants over the production resolver, shared by corpus replay and fuzzing.
use super::*;
use std::cell::Cell;

thread_local! {
    pub(super) static IDENTITIES: Cell<usize> = const { Cell::new(0) };
}

/// Fuzz container: a canonical parent path and the original tool argument value.
pub fn check(bytes: &[u8]) {
    let Ok((parent, arguments)) = serde_json::from_slice::<(String, Value)>(bytes) else {
        return;
    };
    let Ok(parent) = AgentPath::try_from(parent) else {
        return;
    };
    check_value(&parent, arguments);
}

fn check_value(parent: &AgentPath, arguments: Value) {
    IDENTITIES.with(|count| count.set(0));
    let actual = waited(parent, &arguments);
    let work = IDENTITIES.with(Cell::get);
    assert_eq!(
        actual,
        waited(parent, arguments.clone()),
        "borrowed admission and owned dispatch selected different waits"
    );
    // This is the original owned JSON decoder, rather than a second input model.
    let input = match decode::<WaitAgentInput>(arguments) {
        Ok(input) => input,
        Err(error) => {
            assert_eq!(
                actual,
                Err(error),
                "borrowed wait changed its input refusal"
            );
            assert_eq!(work, 0);
            return;
        }
    };
    assert!(
        work <= input.agents.len(),
        "wait selection repeatedly decoded a known child identity"
    );
    let resolved: Vec<_> = input
        .agents
        .iter()
        .map(|reference| named_child(parent, reference))
        .collect();
    if let Some(index) = resolved.iter().position(Option::is_none) {
        assert_eq!(
            actual,
            Err(rejected(format!(
                "{} names no child of this agent",
                input.agents[index]
            )))
        );
        return;
    }
    let distinct: BTreeSet<_> = resolved.iter().flatten().collect();
    if distinct.is_empty() {
        assert_eq!(actual, Err(rejected("wait names no child")));
        return;
    }
    let named = u32::try_from(distinct.len()).unwrap_or(u32::MAX);
    let need = input.need.unwrap_or(named);
    if need == 0 || need > named {
        assert_eq!(
            actual,
            Err(rejected(format!(
                "need must be between 1 and the {named} distinct children named"
            ))),
            "wait admitted an impossible quorum"
        );
        return;
    }
    let (children, actual_need, timeout) = actual.unwrap();
    assert_eq!(
        children.len(),
        distinct.len(),
        "wait selected a child twice"
    );
    assert_eq!((actual_need, timeout), (need, input.timeout_ms));
    let mut previous = None;
    let mut selected = BTreeSet::new();
    for (reference, child) in &children {
        assert!(child.is_child_of(parent), "wait selected a foreign child");
        assert!(selected.insert(child), "wait selected a child twice");
        let first = resolved
            .iter()
            .position(|original| original.as_ref() == Some(child))
            .unwrap();
        assert_eq!(
            reference, &input.agents[first],
            "wait lost its first reference"
        );
        assert!(previous.is_none_or(|previous| previous < first));
        previous = Some(first);
    }
    assert_eq!(selected, distinct, "wait omitted a named child");
    assert_eq!(work, input.agents.len());
}

#[cfg(test)]
mod tests {
    use super::*;

    const CORPUS: [&[u8]; 2] = [
        include_bytes!("../../../../harness-fuzz/corpus/agent_wait_selection/recorded-path.json"),
        include_bytes!("../../../../harness-fuzz/corpus/agent_wait_selection/recorded-name.json"),
    ];

    #[test]
    fn recorded_waits_and_derived_selections_preserve_identity_order_and_work() {
        for (index, bytes) in CORPUS.into_iter().enumerate() {
            check(bytes);
            let (parent, original): (String, Value) = serde_json::from_slice(bytes).unwrap();
            let (foreign_parent, foreign): (String, Value) =
                serde_json::from_slice(CORPUS[1 - index]).unwrap();
            let foreign_parent = AgentPath::try_from(foreign_parent).unwrap();
            let foreign =
                named_child(&foreign_parent, foreign["agents"][0].as_str().unwrap()).unwrap();
            let parent = AgentPath::try_from(parent).unwrap();
            let worker = spawned_child(&parent, "worker").unwrap();
            let names = ["worker".to_owned(), worker.to_string()];
            // Exhaust every alias ordering through length six, then vary the
            // quorum on both sides of the actual distinct-child count.
            for length in 0..=6 {
                for bits in 0..1_usize << length {
                    let agents: Vec<_> = (0..length)
                        .map(|bit| names[(bits >> bit) & 1].clone())
                        .collect();
                    for need in [None, Some(0), Some(1), Some(2)] {
                        check_value(&parent, json!({"agents":agents,"need":need,"timeout_ms":0}));
                    }
                }
            }
            // Distinct inputs exercise the former growing-prefix scan. These
            // names are explicit derivations from the captured spawn name.
            let distinct: Vec<_> = (0..32).map(|index| format!("worker-{index}")).collect();
            check_value(&parent, json!({"agents":distinct}));
            for arguments in [
                json!({"agents":[worker, foreign]}),
                json!({"agents":[""]}),
                json!({"agents":["worker"],"need":-1}),
                json!({"agents":["worker"],"timeout_ms":u64::MAX}),
                json!({"agents":["worker"],"unknown":original}),
                json!({}),
                Value::Null,
            ] {
                check_value(&parent, arguments);
            }
        }
    }
}
