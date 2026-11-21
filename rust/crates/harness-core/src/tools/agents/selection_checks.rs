//! Invariants over the production resolver, shared by corpus replay and fuzzing.
use super::*;
use crate::agent::SELECTED_IDENTITIES as IDENTITIES;
use std::cell::Cell;

// Every valid spawn name has one released generation in this selection input.
// Live generations and replay binding are exercised by the wired actor suite.
struct Selection(AgentPath);

impl TurnChildren for Selection {
    fn agent(&self) -> &AgentPath {
        &self.0
    }

    fn resolve<'a>(
        &'a self,
        _: &'a AgentPath,
        _: &'a str,
    ) -> BoxProviderFuture<'a, Result<crate::agent::SpawnResolution>> {
        Box::pin(async { Ok(crate::agent::SpawnResolution::Free(1)) })
    }

    fn select<'a>(
        &'a self,
        _: crate::OperationId,
        references: &'a [String],
    ) -> BoxProviderFuture<'a, Result<Vec<(String, AgentPath)>>> {
        Box::pin(crate::agent::select_children(self, references))
    }

    fn settled<'a>(
        &'a self,
        _: &'a AgentPath,
    ) -> BoxProviderFuture<'a, Result<crate::agent::Settlement>> {
        Box::pin(async { Err(Error::Invalid("selection observed settlement".into())) })
    }

    fn answer<'a>(
        &'a self,
        _: &'a AgentPath,
        _: u64,
    ) -> BoxProviderFuture<'a, Result<(String, crate::agent::TurnOutcome)>> {
        Box::pin(async { Err(Error::Invalid("selection read an answer".into())) })
    }
}

fn waited<'de>(
    parent: &AgentPath,
    arguments: impl serde::Deserializer<'de, Error = serde_json::Error>,
) -> std::result::Result<WaitSelection, ToolResult> {
    futures::executor::block_on(super::waited(
        &Selection(parent.clone()),
        crate::OperationId::from_bytes([1; 16]),
        WaitAgentInput::deserialize(arguments)
            .map_err(|error| super::super::rejected_input(Error::Invalid(error.to_string())))?,
    ))
    .map_err(super::super::rejected_input)
}

fn named_child(parent: &AgentPath, reference: &str) -> Result<Option<AgentPath>> {
    futures::executor::block_on(crate::agent::named_child(
        Some(&Selection(parent.clone())),
        parent,
        reference,
        true,
    ))
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
    let result = check_selection(parent, arguments);
    assert!(result.is_ok(), "wait selection oracle failed: {result:?}");
}

fn check_selection(parent: &AgentPath, arguments: Value) -> Result<()> {
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
            return Ok(());
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
        .collect::<Result<_>>()?;
    if let Some((reference, _)) = input
        .agents
        .iter()
        .zip(&resolved)
        .find(|(_, child)| child.is_none())
    {
        assert_eq!(
            actual,
            Err(rejected(format!(
                "{reference} names no child of this agent"
            )))
        );
        return Ok(());
    }
    let distinct: BTreeSet<_> = resolved.iter().flatten().collect();
    let mut sent = Vec::new();
    let mut seen = BTreeSet::new();
    for id in &input.sent {
        if seen.insert(id) {
            match IntentId::try_from(id.clone()) {
                Ok(id) => sent.push(id),
                Err(error) => {
                    assert_eq!(
                        actual,
                        Err(super::super::rejected_input(Error::from(error)))
                    );
                    return Ok(());
                }
            }
        }
    }
    if distinct.is_empty() && sent.is_empty() {
        assert_eq!(actual, Err(rejected("wait names no child or sent message")));
        return Ok(());
    }
    let named = u32::try_from(distinct.len().saturating_add(sent.len())).unwrap_or(u32::MAX);
    let need = input.need.unwrap_or(named);
    if need == 0 || need > named {
        assert_eq!(
            actual,
            Err(rejected(format!(
                "need must be between 1 and the {named} distinct children and sent messages named"
            ))),
            "wait admitted an impossible quorum"
        );
        return Ok(());
    }
    let WaitSelection {
        children,
        sent: selected_sent,
        need: actual_need,
        timeout_ms: timeout,
    } = actual.map_err(|error| Error::Invalid(format!("admitted wait was refused: {error:?}")))?;
    assert_eq!(selected_sent, sent, "wait changed original send identities");
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
        let (first, (original, _)) = input
            .agents
            .iter()
            .zip(&resolved)
            .enumerate()
            .find(|(_, (_, resolved))| resolved.as_ref() == Some(child))
            .ok_or_else(|| {
                Error::Invalid("wait selected a child absent from the request".into())
            })?;
        assert_eq!(reference, original, "wait lost its first reference");
        assert!(previous.is_none_or(|previous| previous < first));
        previous = Some(first);
    }
    assert_eq!(selected, distinct, "wait omitted a named child");
    assert_eq!(work, input.agents.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CORPUS: [&[u8]; 2] = [
        include_bytes!("../../../../harness-fuzz/corpus/agent_wait_selection/recorded-path.json"),
        include_bytes!("../../../../harness-fuzz/corpus/agent_wait_selection/recorded-name.json"),
    ];

    #[test]
    fn recorded_waits_and_derived_selections_preserve_identity_order_and_work() -> Result<()> {
        let [recorded_path, recorded_name] = CORPUS;
        let decode = |bytes| {
            serde_json::from_slice::<(String, Value)>(bytes)
                .map_err(|error| Error::Invalid(error.to_string()))
        };
        let require_child = |parent: &AgentPath, reference: &str| {
            named_child(parent, reference)?.ok_or_else(|| {
                Error::Invalid(format!("recorded wait names no child at {reference}"))
            })
        };
        for (bytes, foreign_bytes) in [
            (recorded_path, recorded_name),
            (recorded_name, recorded_path),
        ] {
            check(bytes);
            let (parent, original) = decode(bytes)?;
            let (foreign_parent, foreign) = decode(foreign_bytes)?;
            let foreign_parent = AgentPath::try_from(foreign_parent)?;
            let foreign_reference = foreign
                .pointer("/agents/0")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("recorded wait has no first reference".into()))?;
            let foreign = require_child(&foreign_parent, foreign_reference)?;
            let parent = AgentPath::try_from(parent)?;
            let worker = require_child(&parent, "worker")?;
            for reference in ["worker", worker.as_str()] {
                assert_eq!(named_child(&parent, reference)?.as_ref(), Some(&worker));
            }
            for reference in ["other", "agents/", foreign.as_str()] {
                assert_ne!(named_child(&parent, reference)?.as_ref(), Some(&worker));
            }
            let prefixed = require_child(&parent, "agents/worker")?;
            assert_ne!(
                prefixed, worker,
                "a literal spawn name cannot alias a branch operand"
            );
            for reference in ["agents/worker", prefixed.as_str()] {
                assert_eq!(named_child(&parent, reference)?.as_ref(), Some(&prefixed));
            }
            check_value(
                &parent,
                json!({"agents":["worker", worker, "agents/worker", prefixed]}),
            );
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
            for agents in [json!([]), json!(["worker", worker])] {
                for sent in [
                    json!(["first"]),
                    json!(["first", "first", "second"]),
                    json!([""]),
                ] {
                    for need in [None, Some(0), Some(1), Some(2), Some(3), Some(4)] {
                        check_value(&parent, json!({"agents":agents,"sent":sent,"need":need}));
                    }
                }
            }
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
        Ok(())
    }
}
