//! Generated decisions use the shipping dispatcher, decoder and final check.
use super::*;
use serde_json::json;
use std::cell::RefCell;

pub(super) fn plugin(instance: &str, matcher: Option<RequestMatch>) -> HookSpec {
    HookSpec {
        run: HookRun::Plugin {
            instance: instance.into(),
            sh: Some("exit 0".into()),
            ps1: None,
            config: "{}".into(),
            matcher,
        },
        timeout_ms: u64::MAX,
        failure: HookFailure::Block,
    }
}

pub(super) fn instance(hook: &HookSpec) -> &str {
    match &hook.run {
        HookRun::Plugin { instance, .. } => instance,
        _ => unreachable!("only plugin hooks here"),
    }
}

/// Own protocol decisions, not captured external process results: the callback
/// supplies the hook function while the original dispatcher owns the schedule.
pub(super) async fn judge(
    hooks: Vec<HookSpec>,
    command: &str,
    decide: impl Fn(&str, &str) -> Value,
) -> Result<(HookOutput, Vec<(String, String)>)> {
    let mut configured = Hooks::default();
    configured.add_rules([HookRule {
        on: HookEvent::PreToolUse,
        matcher: String::new(),
        hooks,
    }])?;
    let original = json!({"tool_name":"shell","tool_input":{"command":command}});
    let runs = RefCell::new(Vec::new());
    let output = configured
        .run_commands(
            HookEvent::PreToolUse,
            &original,
            &crate::tool::ToolRegistry::default(),
            u64::MAX,
            |hook, _, rewritten| {
                let input = rewritten.unwrap_or(&original["tool_input"]);
                let command = input["command"].as_str().unwrap_or_default().to_owned();
                let name = instance(hook).to_owned();
                let decision = decide(&name, &command);
                runs.borrow_mut().push((name, command));
                std::future::ready(Ok((Some(0), decision.to_string().into_bytes(), vec![])))
            },
        )
        .await?;
    Ok((output, runs.into_inner()))
}

/// Strictness is independent of configuration order. Rewriters must compose in
/// that order, then every sibling must have checked the exact final input.
pub async fn check(bytes: &[u8]) -> Result<()> {
    let decisions: Vec<_> = bytes
        .iter()
        .copied()
        .chain(std::iter::once(0))
        .map(|byte| byte % 3)
        .collect();
    let hooks = (0..decisions.len())
        .map(|index| plugin(&index.to_string(), None))
        .collect();
    let (output, runs) = judge(hooks, "original", |name, _| {
        match decisions[name.parse::<usize>().unwrap()] {
            2 => json!({"allow":false,"reason":"denied"}),
            1 => json!({"ask":"approval?","to":"person"}),
            _ => json!({"allow":true}),
        }
    })
    .await?;
    let denied = decisions.contains(&2);
    if output.blocks() != denied
        || output.updated_input.is_some()
        || (!denied && output.approval.is_some() != decisions.contains(&1))
        || runs.len() != decisions.len()
    {
        return Err(Error::Invalid("strictest-wins oracle".into()));
    }

    let original =
        serde_json::to_string(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    let middle = format!("{original}:first");
    let final_input = format!("{original}:second");
    let mode = bytes.first().copied().unwrap_or_default() % 3;
    let order = if bytes.get(1).copied().unwrap_or_default() & 1 == 0 {
        ["guard", "a", "b"]
    } else {
        ["a", "guard", "b"]
    };
    let (output, runs) = judge(
        order.iter().map(|name| plugin(name, None)).collect(),
        &original,
        |name, command| {
            let rewrite = |command: &str| json!({"allow":true,"input":{"command":command}});
            match name {
                "a" if command == original => rewrite(&middle),
                "a" if mode == 2 && command == final_input => rewrite("unapproved"),
                "b" if command != final_input => rewrite(&final_input),
                "guard" if mode == 1 && command == final_input => {
                    json!({"allow":false,"reason":"final input denied"})
                }
                "guard" => json!({"ask":format!("run {command}?"),"to":"person"}),
                _ => json!({"allow":true}),
            }
        },
    )
    .await?;
    if runs.len() > 3 * order.len() {
        return Err(Error::Invalid(
            "hook work exceeded three visits per selected hook".into(),
        ));
    }
    if mode != 0 {
        if !output.blocks() || output.updated_input.is_some() {
            return Err(Error::Invalid("final-input refusal was bypassed".into()));
        }
    } else if output.blocks()
        || output.updated_input != Some(json!({"command":final_input}))
        || output.approval != Some(format!("run {final_input}?"))
        || order.iter().any(|name| {
            !runs
                .iter()
                .any(|(seen, input)| seen == name && input == &final_input)
        })
    {
        return Err(Error::Invalid(
            "siblings did not approve the exact final input".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generated_chains_keep_strictness_final_input_and_linear_visits() -> Result<()> {
        for first in 0..6 {
            for second in 0..6 {
                check(&[first, second]).await?;
            }
        }
        check(&[]).await
    }
}
