//! Generated decisions use the shipping dispatcher, decoder and final check.
use super::*;
use serde_json::json;
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

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

fn identity(hook: &HookSpec) -> &str {
    match &hook.run {
        HookRun::Plugin { instance, .. } => instance,
        HookRun::Shell { command, .. } => command,
        HookRun::Executable { executable, .. } => executable,
    }
}

fn configured(event: HookEvent, hooks: Vec<HookSpec>) -> Result<Hooks> {
    let mut configured = Hooks::default();
    configured.add_rules([HookRule {
        on: event,
        matcher: String::new(),
        hooks,
    }])?;
    Ok(configured)
}

/// Own protocol decisions, not captured external process results: the callback
/// supplies the hook function while the original dispatcher owns the schedule.
pub(super) async fn judge(
    hooks: Vec<HookSpec>,
    command: &str,
    decide: impl Fn(&str, &str) -> Value,
) -> Result<(HookOutput, Vec<(String, String)>)> {
    let configured = configured(HookEvent::PreToolUse, hooks)?;
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
                let name = identity(hook).to_owned();
                let decision = decide(&name, &command);
                runs.borrow_mut().push((name, command));
                std::future::ready(Ok((Some(0), decision.to_string().into_bytes(), vec![])))
            },
        )
        .await?;
    Ok((output, runs.into_inner()))
}

/// Step the original future across producer withdrawal, before or after its
/// first suspension. A registered result wait must wake and retain its cause.
/// Two explicit transition polls make a lost wake or terminal fail without a hang.
/// Caller cancellation must retire the same pending result receiver. Generated
/// cuts cover the initial round, ordered rewrites and final-input revalidation.
fn withdrawn(bytes: &[u8]) -> Result<()> {
    const CLOSED: &str = "hook result producer closed";
    let phase = bytes.get(3).copied().unwrap_or_default() % 3;
    // Rewriting failure controls block immediately, preserving the original
    // failing callback as the terminal cause before another revalidation.
    let report = phase == 0 && bytes.first().is_some_and(|byte| byte & 1 != 0);
    let mut source = plugin("source", None);
    if report {
        source.run = HookRun::Shell {
            command: "source".into(),
            shell: None,
        };
        source.failure = HookFailure::Report;
    }
    let hooks = if phase == 0 {
        vec![source, plugin("allow", None)]
    } else {
        vec![plugin("first", None), source, plugin("last", None)]
    };
    let configured = configured(HookEvent::PreToolUse, hooks)?;
    let wait_input = if phase == 1 { "first" } else { "final" };
    let expected_visits = match phase {
        0 => 2,
        1 => 4,
        _ => 8,
    };
    let original = json!({"tool_name":"shell","tool_input":{"command":"checked"}});
    let registry = crate::tool::ToolRegistry::default();
    let (producer, receiver) = futures::channel::oneshot::channel::<()>();
    let mut receiver = Some(receiver);
    let visits = Cell::new(0);
    let cancel = bytes.get(2).is_some_and(|byte| byte & 1 != 0);
    let suspended = cancel || bytes.get(1).is_some_and(|byte| byte & 1 != 0);
    let mut producer = Some(producer);
    {
        let run = configured.run_commands(
            HookEvent::PreToolUse,
            &original,
            &registry,
            u64::MAX,
            |hook, _, rewritten| {
                visits.set(visits.get() + 1);
                let input = rewritten.unwrap_or(&original["tool_input"]);
                let command = input["command"].as_str().unwrap_or_default();
                let name = identity(hook);
                let result = (name == "source" && (phase == 0 || command == wait_input))
                    .then(|| receiver.take());
                let decision: &[u8] = match (name, command) {
                    ("first", "checked") => br#"{"allow":true,"input":{"command":"first"}}"#,
                    ("last", command) if command != "final" => {
                        br#"{"allow":true,"input":{"command":"final"}}"#
                    }
                    _ => br#"{"allow":true}"#,
                };
                async move {
                    if let Some(result) = result {
                        let receiver =
                            result.ok_or_else(|| Error::Invalid("hook dispatched twice".into()))?;
                        receiver.await.map_err(|_| Error::Invalid(CLOSED.into()))?;
                        return Err(Error::Invalid("withdrawn producer returned success".into()));
                    }
                    Ok((Some(0), decision.to_vec(), Vec::new()))
                }
            },
        );
        let mut run = std::pin::pin!(run);
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut context = Context::from_waker(&waker);
        if suspended
            && (run.as_mut().poll(&mut context).is_ready() || visits.get() != expected_visits)
        {
            return Err(Error::Invalid(
                "hook did not wait on its result producer".into(),
            ));
        }
        if !cancel {
            drop(producer.take());
            if suspended && wakes.0.load(Ordering::Relaxed) != 1 {
                return Err(Error::Invalid(
                    "hook result withdrawal lost its wake".into(),
                ));
            }
            let Poll::Ready(output) = run.as_mut().poll(&mut context) else {
                return Err(Error::Invalid(
                    "hook result withdrawal lost its terminal".into(),
                ));
            };
            let output = output?;
            if visits.get() != expected_visits
                || output.blocks() == report
                || output.updated_input.is_some()
                || !matches!(
                    output.diagnostics.as_slice(),
                    [HookDiagnostic {
                        cause: crate::ProcessError::InvalidOutput { exit_code: None, message },
                        blocking,
                    }] if message == CLOSED && *blocking != report
                )
            {
                return Err(Error::Invalid(
                    "hook withdrawal lost its cause, policy or dispatch count".into(),
                ));
            }
        }
    }
    // Leaving the original chain's scope must retire its pending callback.
    // A successful send here would reveal a retained or detached result wait.
    if let Some(producer) = producer
        && producer.send(()).is_ok()
    {
        return Err(Error::Invalid(
            "cancelled hook chain retained its result receiver".into(),
        ));
    }
    Ok(())
}

/// Two original callback results remain jointly owned until both settle. The
/// configured failure policies bind each retained cause to its selected hook,
/// independently of completion order. Every event uses the same fixture;
/// cancellation releases pending receivers on either dispatch path.
fn joined(bytes: &[u8]) -> Result<()> {
    let mut first = plugin("first", None);
    first.run = HookRun::Shell {
        command: "first".into(),
        shell: None,
    };
    first.failure = HookFailure::Report;
    let event = HookEvent::ALL
        [usize::from(bytes.get(2).copied().unwrap_or_default()) % HookEvent::ALL.len()];
    let blocking = event != HookEvent::Stop;
    let configured = configured(event, vec![first, plugin("second", None)])?;
    let original = json!({"tool_name":"shell","tool_input":{"command":"checked"}});
    let registry = crate::tool::ToolRegistry::default();
    let channels = [
        futures::channel::oneshot::channel::<()>(),
        futures::channel::oneshot::channel::<()>(),
    ];
    let [(first_tx, first_rx), (second_tx, second_rx)] = channels;
    let mut producers = [Some(first_tx), Some(second_tx)];
    let mut receivers = [Some(first_rx), Some(second_rx)];
    let visits = [Cell::new(0), Cell::new(0)];
    let cancel = bytes.first().copied().unwrap_or_default() % 3;
    let first_completed = usize::from(bytes.get(1).is_some_and(|byte| byte & 1 != 0));
    {
        let run = configured.run_commands(event, &original, &registry, u64::MAX, |hook, _, _| {
            let index = usize::from(identity(hook) == "second");
            visits[index].set(visits[index].get() + 1);
            let receiver = receivers[index].take();
            async move {
                let receiver = receiver
                    .ok_or_else(|| Error::Invalid("joined hook dispatched twice".into()))?;
                receiver.await.map_err(|_| {
                    Error::Invalid(
                        if index == 0 {
                            "first closed"
                        } else {
                            "second closed"
                        }
                        .into(),
                    )
                })?;
                Err(Error::Invalid("withdrawn producer returned success".into()))
            }
        });
        let mut run = std::pin::pin!(run);
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut context = Context::from_waker(&waker);
        let initial = run.as_mut().poll(&mut context);
        if visits.iter().any(|visits| visits.get() != 1) {
            return Err(Error::Invalid(
                "joined hooks exceeded one dispatch per producer".into(),
            ));
        }
        if initial.is_ready() {
            return Err(Error::Invalid(
                "joined hooks did not retain both producers".into(),
            ));
        }
        if cancel != 1 {
            // The join may also schedule itself while polling. Measure only
            // the wake caused by withdrawing this original result producer.
            let before = wakes.0.load(Ordering::Relaxed);
            drop(producers[first_completed].take());
            if wakes.0.load(Ordering::Relaxed) != before + 1 {
                return Err(Error::Invalid(
                    "joined hook result lost its first wake".into(),
                ));
            }
            if run.as_mut().poll(&mut context).is_ready() {
                return Err(Error::Invalid(
                    "joined hooks settled before both producers".into(),
                ));
            }
            if cancel == 0 {
                let before = wakes.0.load(Ordering::Relaxed);
                drop(producers[1 - first_completed].take());
                if wakes.0.load(Ordering::Relaxed) != before + 1 {
                    return Err(Error::Invalid(
                        "joined hook result lost its final wake".into(),
                    ));
                }
                let Poll::Ready(output) = run.as_mut().poll(&mut context) else {
                    return Err(Error::Invalid("joined hooks lost their terminal".into()));
                };
                let output = output?;
                if output.blocks() != blocking
                    || output.updated_input.is_some()
                    || visits.iter().any(|visits| visits.get() != 1)
                    || !matches!(
                        output.diagnostics.as_slice(),
                        [
                            HookDiagnostic {
                                cause: crate::ProcessError::InvalidOutput { exit_code: None, message: first },
                                blocking: false,
                            },
                            HookDiagnostic {
                                cause: crate::ProcessError::InvalidOutput { exit_code: None, message: second },
                                blocking: actual_blocking,
                            },
                        ] if first == "first closed" && second == "second closed" && *actual_blocking == blocking
                    )
                {
                    return Err(Error::Invalid(
                        "joined hooks lost cause identity or dispatch count".into(),
                    ));
                }
            }
        }
    }
    for producer in producers.into_iter().flatten() {
        if producer.send(()).is_ok() {
            return Err(Error::Invalid(
                "cancelled joined hooks retained a result receiver".into(),
            ));
        }
    }
    Ok(())
}

/// Strictness is independent of configuration order. Rewriters must compose in
/// that order, then every sibling must have checked the exact final input.
/// Result-producer withdrawal must wake the chain and retain its failure policy;
/// caller cancellation must release the original result wait.
pub async fn check(bytes: &[u8]) -> Result<()> {
    joined(bytes)?;
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
    let mut visits = vec![0; decisions.len()];
    for (name, _) in &runs {
        visits[name.parse::<usize>().unwrap()] += 1;
    }
    if visits.iter().any(|visits| *visits != 1) {
        return Err(Error::Invalid(
            "hook work exceeded one visit per unchanged selected hook".into(),
        ));
    }
    if output.blocks() != denied
        || output.updated_input.is_some()
        || (!denied && output.approval.is_some() != decisions.contains(&1))
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
    if order
        .iter()
        .any(|name| runs.iter().filter(|(seen, _)| seen == name).count() > 3)
    {
        return Err(Error::Invalid(
            "hook work exceeded three visits per selected hook".into(),
        ));
    }
    if !runs
        .iter()
        .any(|(name, input)| name == "b" && input == &middle)
    {
        return Err(Error::Invalid("ordered rewrite was not forwarded".into()));
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
    withdrawn(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generated_chains_keep_strictness_final_input_and_linear_visits() -> Result<()> {
        for first in 0..6 {
            for second in 0..6 {
                for event in 0..HookEvent::ALL.len() as u8 {
                    for phase in 0..3 {
                        check(&[first, second, event, phase]).await?;
                    }
                }
            }
        }
        check(&[]).await
    }
}
