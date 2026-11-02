//! Several before-hooks on one request (FILESYSTEM §2.6).
//!
//! Claude Code, Codex and Gemini run matching hooks in parallel and take the
//! strictest answer, but resolve competing rewrites by configuration order or
//! completion order, so a rewrite can slip past a sibling's check. This keeps
//! their parallel, strictest-first result and closes that hole:
//! 1. every hook runs on the request; with no rewrite the strictest answer
//!    wins: deny, then ask, then allow;
//! 2. rewrites apply one at a time in authority order (configuration order,
//!    highest issuer first), each hook seeing the current input, and the
//!    new input replaces the whole input;
//! 3. every hook must then allow the final input unchanged: an answer counts
//!    only for the exact input it saw, and a rewrite here denies;
//! 4. an ask goes out last, about the final input, so approving it runs the
//!    request that was checked.
//!
//! The chain is one hook outcome, recorded once per call by its caller.
use super::*;

/// One command's raw result.
pub(super) type Run = Result<(Option<i32>, Vec<u8>, Vec<u8>)>;

/// One hook's answer about the input it saw.
#[derive(Debug, PartialEq)]
enum Answer {
    Allow,
    Ask,
    Rewrite(Value),
    Deny,
}

fn answer(hook: &HookSpec, event: HookEvent, run: &Run) -> Result<Answer> {
    let Ok((code, stdout, stderr)) = run else {
        return Ok(if hook.failure.blocks(event) {
            Answer::Deny
        } else {
            Answer::Allow
        });
    };
    let mut single = HookOutput::default();
    let mut budget = budget::Budget::new(u64::MAX)?;
    budget.apply(
        &mut single,
        event,
        hook,
        Ok((*code, stdout.clone(), stderr.clone())),
    );
    budget.finish(&mut single);
    Ok(if single.blocks() {
        Answer::Deny
    } else if let Some(input) = single.updated_input {
        Answer::Rewrite(input)
    } else if single.approval.is_some() {
        Answer::Ask
    } else {
        Answer::Allow
    })
}

fn fold<'a>(
    runs: impl IntoIterator<Item = (&'a Arc<HookSpec>, Run)>,
    event: HookEvent,
    maximum_bytes: u64,
) -> Result<HookOutput> {
    let mut budget = budget::Budget::new(maximum_bytes)?;
    let mut output = HookOutput::default();
    for (hook, run) in runs {
        budget.apply(&mut output, event, &**hook, run);
    }
    budget.finish(&mut output);
    Ok(output)
}

/// Runs `hooks` (highest authority first) on a before-tool request whose
/// tool input is `original`. `execute` runs one hook on the original input
/// (`None`) or on a rewritten one.
pub(super) async fn judge<F, Fut>(
    hooks: &[&Arc<HookSpec>],
    event: HookEvent,
    original: &Value,
    (quota, maximum_bytes): (usize, u64),
    execute: &mut F,
) -> Result<HookOutput>
where
    F: FnMut(&Arc<HookSpec>, usize, Option<&Value>) -> Fut,
    Fut: Future<Output = Run>,
{
    // Step 1: everything on the request, in parallel.
    let mut runs: Vec<Run> =
        futures::future::join_all(hooks.iter().map(|hook| execute(hook, quota, None))).await;
    let answers = hooks
        .iter()
        .zip(&runs)
        .map(|(hook, run)| answer(hook, event, run))
        .collect::<Result<Vec<_>>>()?;
    let denied = answers.contains(&Answer::Deny);
    if denied || !answers.iter().any(|answer| matches!(answer, Answer::Rewrite(_))) {
        let mut output = fold(hooks.iter().copied().zip(runs), event, maximum_bytes)?;
        // A denial wins over every rewrite; none of them applies.
        if denied {
            output.updated_input = None;
        }
        return Ok(output);
    }
    // The input each run saw.
    let mut seen: Vec<Option<Value>> = vec![None; hooks.len()];
    let saw = |seen: &Option<Value>| seen.clone().unwrap_or_else(|| original.clone());
    let deny = |hook: &Arc<HookSpec>, run: Run| {
        let mut output = fold([(hook, run)], event, maximum_bytes)?;
        output.updated_input = None;
        Ok::<_, Error>(output)
    };
    // Step 2: rewrites in authority order, each on the current input.
    let mut current = original.clone();
    for index in 0..hooks.len() {
        if saw(&seen[index]) != current {
            runs[index] = execute(hooks[index], quota, Some(&current)).await;
            seen[index] = Some(current.clone());
        }
        match answer(hooks[index], event, &runs[index])? {
            Answer::Deny => return deny(hooks[index], runs.swap_remove(index)),
            Answer::Rewrite(input) => current = input,
            Answer::Allow | Answer::Ask => {}
        }
    }
    // Step 3: every hook on the final input, in parallel.
    let stale: Vec<usize> = (0..hooks.len())
        .filter(|&index| saw(&seen[index]) != current)
        .collect();
    let reruns = futures::future::join_all(
        stale
            .iter()
            .map(|&index| execute(hooks[index], quota, Some(&current))),
    )
    .await;
    for (index, run) in stale.into_iter().zip(reruns) {
        runs[index] = run;
    }
    for index in 0..hooks.len() {
        match answer(hooks[index], event, &runs[index])? {
            Answer::Deny => return deny(hooks[index], runs.swap_remove(index)),
            Answer::Rewrite(input) if input != current => {
                return Ok(HookOutput {
                    blocked: Some(
                        "a before-hook changed the request after every rewrite was applied".into(),
                    ),
                    ..HookOutput::default()
                });
            }
            Answer::Rewrite(_) | Answer::Allow | Answer::Ask => {}
        }
    }
    // Step 4: the final input, with any ask about it.
    let mut output = fold(hooks.iter().copied().zip(runs), event, maximum_bytes)?;
    output.updated_input = Some(current);
    Ok(output)
}

#[cfg(test)]
#[path = "chain/tests.rs"]
mod tests;
