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
    input: Option<Value>,
) -> Result<HookOutput> {
    let mut output = HookOutput {
        updated_input: input,
        ..HookOutput::default()
    };
    let mut budget = budget::Budget::existing(maximum_bytes, &output)?;
    for (hook, run) in runs {
        budget.apply(&mut output, event, &**hook, run);
    }
    budget.finish(&mut output);
    Ok(output)
}

/// Runs `hooks` (highest authority first) on a before-tool request whose
/// tool input is `original`. `execute` runs one hook on the original input
/// (`None`) or on a rewritten one.
pub(super) async fn judge<F, Fut, M>(
    hooks: &[(&Arc<HookSpec>, Option<&Requests>)],
    event: HookEvent,
    original: &Value,
    (quota, maximum_bytes): (usize, u64),
    matches: &M,
    execute: &mut F,
) -> Result<HookOutput>
where
    F: FnMut(&Arc<HookSpec>, usize, Option<&Value>) -> Fut,
    Fut: Future<Output = Run>,
    M: Fn(Option<&Requests>, &Value) -> bool,
{
    // Keep all candidates. A skipped hook has no process or output, and is
    // reconsidered whenever another hook changes the input.
    let pending = hooks.iter().map(|(hook, matcher)| {
        let execution = matches(*matcher, original).then(|| execute(hook, quota, None));
        async move {
            match execution {
                Some(run) => Some(run.await),
                None => None,
            }
        }
    });
    let mut runs: Vec<Option<Run>> = futures::future::join_all(pending).await;
    let classify = |hook: &HookSpec, run: &Option<Run>| match run {
        Some(run) => answer(hook, event, run),
        None => Ok(Answer::Allow),
    };
    let answers = hooks
        .iter()
        .zip(&runs)
        .map(|((hook, _), run)| classify(hook, run))
        .collect::<Result<Vec<_>>>()?;
    let denied = answers.contains(&Answer::Deny);
    if denied
        || !answers
            .iter()
            .any(|answer| matches!(answer, Answer::Rewrite(_)))
    {
        let mut output = fold(
            hooks
                .iter()
                .zip(runs)
                .filter_map(|((hook, _), run)| run.map(|run| (*hook, run))),
            event,
            maximum_bytes,
            None,
        )?;
        if denied {
            output.updated_input = None;
        }
        return Ok(output);
    }
    let mut seen: Vec<Option<Value>> = vec![None; hooks.len()];
    let mut current = original.clone();
    for index in 0..hooks.len() {
        let (hook, matcher) = hooks[index];
        if seen[index].as_ref().unwrap_or(original) != &current {
            runs[index] = if matches(matcher, &current) {
                Some(execute(hook, quota, Some(&current)).await)
            } else {
                None
            };
            seen[index] = Some(current.clone());
        }
        match classify(hook, &runs[index])? {
            Answer::Deny => {
                return fold(
                    [(
                        hook,
                        runs.swap_remove(index)
                            .ok_or_else(|| Error::Invalid("missing denying hook outcome".into()))?,
                    )],
                    event,
                    maximum_bytes,
                    None,
                );
            }
            Answer::Rewrite(input) => current = input,
            Answer::Allow | Answer::Ask => {}
        }
    }
    let stale: Vec<_> = (0..hooks.len())
        .filter(|&index| seen[index].as_ref().unwrap_or(original) != &current)
        .collect();
    let pending = stale.iter().map(|&index| {
        let (hook, matcher) = hooks[index];
        let execution = matches(matcher, &current).then(|| execute(hook, quota, Some(&current)));
        async move {
            match execution {
                Some(run) => Some(run.await),
                None => None,
            }
        }
    });
    let reruns = futures::future::join_all(pending).await;
    for (index, run) in stale.into_iter().zip(reruns) {
        runs[index] = run;
    }
    for index in 0..hooks.len() {
        let hook = hooks[index].0;
        match classify(hook, &runs[index])? {
            Answer::Deny => {
                return fold(
                    [(
                        hook,
                        runs.swap_remove(index)
                            .ok_or_else(|| Error::Invalid("missing denying hook outcome".into()))?,
                    )],
                    event,
                    maximum_bytes,
                    None,
                );
            }
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
    // Reserve the final input together with every retained hook contribution.
    fold(
        hooks
            .iter()
            .zip(runs)
            .filter_map(|((hook, _), run)| run.map(|run| (*hook, run))),
        event,
        maximum_bytes,
        Some(current),
    )
}

#[cfg(test)]
#[path = "chain/tests.rs"]
mod tests;
