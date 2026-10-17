use super::plan::Plan;
use super::*;
use crate::ProcessError;
use serde::Serialize;

/// The fold never stops at overflow: its bounded exhausted state keeps guard
/// decisions and policies, even after it has discarded optional context.
pub(super) struct Budget {
    maximum: u64,
    state: State,
}

enum State {
    Available {
        used: u64,
        replacement: u64,
    },
    Exhausted {
        denied: bool,
        asks: bool,
        blocking: bool,
    },
}

/// Only the ledger constructs this token. Decoding retained strings/arguments
/// and growing the outcome require ownership of an accepted reservation.
pub(super) struct Reservation<'a> {
    plan: Option<Plan<'a>>,
}

#[derive(Serialize)]
struct DiagnosticRef<'a, M> {
    cause: &'a ProcessError<M>,
    blocking: bool,
}

impl Budget {
    pub(super) fn new(maximum: u64) -> Result<Self> {
        // Reserve the largest exhausted representation before an external
        // effect starts; even overflow must have a publishable outcome.
        #[derive(Serialize)]
        struct Fallback<'a> {
            blocked: Option<&'a str>,
            approval: Option<&'a str>,
            additional_context: [(); 0],
            updated_input: Option<()>,
            diagnostics: [DiagnosticRef<'a, &'a str>; 1],
        }
        let cause = ProcessError::OutputLimit {
            exit_code: None,
            message: "aggregate hook output limit exceeded",
        };
        encoded_size(
            &Fallback {
                blocked: Some("Hook denied"),
                approval: Some("Hook approval"),
                additional_context: [],
                updated_input: None,
                diagnostics: [DiagnosticRef {
                    cause: &cause,
                    blocking: false,
                }],
            },
            maximum,
        )?;
        Ok(Self {
            maximum,
            state: State::Available {
                used: encoded_size(&HookOutput::default(), maximum)?,
                replacement: 4,
            },
        })
    }

    pub(super) fn apply(
        &mut self,
        output: &mut HookOutput,
        event: HookEvent,
        policy: HookFailure,
        result: Result<(Option<i32>, Vec<u8>, Vec<u8>)>,
    ) {
        let blocking = policy == HookFailure::Block && event != HookEvent::Stop;
        match result {
            Ok((code, stdout, stderr)) => {
                let plan = if code != Some(0) && (code != Some(2) || blocking) {
                    Plan::exit(code, &stderr, blocking)
                } else {
                    Plan::inspect(output, event, code, &stdout, &stderr)
                        .unwrap_or_else(|message| Plan::invalid(message, blocking, code))
                };
                self.fold(output, plan, blocking);
            }
            Err(error) => self.fold(output, Plan::failed(error, blocking), blocking),
        }
    }

    fn reserve<'a>(
        &mut self,
        output: &HookOutput,
        plan: Plan<'a>,
    ) -> std::result::Result<Reservation<'a>, Plan<'a>> {
        let State::Available { used, replacement } = &mut self.state else {
            return Err(plan);
        };
        let cost = || -> Result<(u64, u64)> {
            let mut next = *used;
            if let Some(text) = &plan.blocked {
                next = next
                    .checked_add(encoded_size(text, self.maximum)?)
                    .ok_or_else(exceeded)?
                    - 4;
            }
            if let Some(text) = &plan.approval {
                next = next
                    .checked_add(encoded_size(text, self.maximum)?)
                    .ok_or_else(exceeded)?
                    - 4;
            }
            let mut contexts = output.additional_context.len();
            for text in plan.context.iter().flatten() {
                next = next
                    .checked_add(encoded_size(text, self.maximum)? + u64::from(contexts > 0))
                    .ok_or_else(exceeded)?;
                contexts += 1;
            }
            if let Some((cause, blocking)) = &plan.failure {
                next = next
                    .checked_add(
                        encoded_size(
                            &DiagnosticRef {
                                cause,
                                blocking: *blocking,
                            },
                            self.maximum,
                        )? + u64::from(!output.diagnostics.is_empty()),
                    )
                    .ok_or_else(exceeded)?;
            }
            let input = match plan.input {
                Some(raw) => {
                    let size = raw.get().len() as u64;
                    next = (next - *replacement)
                        .checked_add(size)
                        .ok_or_else(exceeded)?;
                    size
                }
                None => *replacement,
            };
            if next > self.maximum {
                return Err(exceeded());
            }
            Ok((next, input))
        }();
        match cost {
            Ok((next, input)) => {
                *used = next;
                *replacement = input;
                Ok(Reservation { plan: Some(plan) })
            }
            Err(_) => Err(plan),
        }
    }

    fn fold(&mut self, output: &mut HookOutput, plan: Plan<'_>, blocking_policy: bool) {
        match self.reserve(output, plan) {
            Ok(reservation) => {
                if let Err(error) = reservation.commit(output) {
                    self.fold(
                        output,
                        Plan::failed(error, blocking_policy),
                        blocking_policy,
                    );
                }
            }
            Err(plan) => {
                let (mut denied, mut asks, mut blocking) = match self.state {
                    State::Available { .. } => (
                        output.blocked.is_some(),
                        output.approval.is_some(),
                        output.diagnostics.iter().any(|failure| failure.blocking),
                    ),
                    State::Exhausted {
                        denied,
                        asks,
                        blocking,
                    } => (denied, asks, blocking),
                };
                // An unretainable delta is itself this command's failure;
                // honor its policy, without letting a later empty successful
                // command retroactively block an earlier report-only overflow.
                let contributes = plan.blocked.is_some()
                    || plan.approval.is_some()
                    || plan.context.iter().any(Option::is_some)
                    || plan.input.is_some()
                    || plan.failure.is_some();
                blocking |= blocking_policy && contributes;
                denied |= plan.blocked.is_some();
                asks |= plan.approval.is_some();
                blocking |= plan.failure.is_some_and(|(_, blocking)| blocking);
                self.state = State::Exhausted {
                    denied,
                    asks,
                    blocking,
                };
                // Drop all optional allocated data once. Later plans remain
                // borrowed; no owned reason or argument is decoded on exhaustion.
                *output = HookOutput::default();
            }
        }
    }

    pub(super) fn finish(self, output: &mut HookOutput) {
        if let State::Exhausted {
            denied,
            asks,
            blocking,
        } = self.state
        {
            *output = HookOutput::output_limit(blocking);
            output.blocked = denied.then(|| "Hook denied".into());
            output.approval = asks.then(|| "Hook approval".into());
        }
    }

    #[cfg(any(test, fuzzing))]
    pub(super) fn observe(
        &mut self,
        output: &mut HookOutput,
        event: HookEvent,
        code: Option<i32>,
        stdout: &[u8],
        stderr: &[u8],
    ) -> Result<bool> {
        // This test/fuzz entry point starts with an existing outcome.
        self.state = State::Available {
            used: encoded_size(output, self.maximum)?,
            replacement: encoded_size(&output.updated_input, self.maximum)?,
        };
        let plan = Plan::inspect(output, event, code, stdout, stderr)
            .map_err(|message| Error::Invalid(message.into()))?;
        let updated = plan.input.is_some();
        self.reserve(output, plan)
            .map_err(|_| exceeded())?
            .commit(output)?;
        Ok(updated)
    }

    #[cfg(any(test, fuzzing))]
    pub(super) fn existing(maximum: u64, output: &HookOutput) -> Result<Self> {
        let mut budget = Self::new(maximum)?;
        budget.state = State::Available {
            used: encoded_size(output, maximum)?,
            replacement: encoded_size(&output.updated_input, maximum)?,
        };
        Ok(budget)
    }
}

impl Reservation<'_> {
    fn commit(mut self, output: &mut HookOutput) -> Result<()> {
        let plan = self.plan.take().ok_or_else(exceeded)?;
        if let Some(text) = plan.blocked {
            output.blocked = Some(text.into_string(&self)?);
        }
        if let Some(text) = plan.approval {
            output.approval = Some(text.into_string(&self)?);
        }
        for text in plan.context.into_iter().flatten() {
            output.additional_context.push(text.into_string(&self)?);
        }
        if let Some(raw) = plan.input {
            output.updated_input = Some(
                serde_json::from_str(raw.get())
                    .map_err(|_| Error::Invalid("invalid hook arguments".into()))?,
            );
        }
        if let Some((cause, blocking)) = plan.failure {
            let cause = cause.try_map_message(|text| text.into_string(&self))?;
            output.diagnostics.push(HookDiagnostic { cause, blocking });
        }
        Ok(())
    }
}

pub(super) fn encoded_size(value: &impl Serialize, maximum: u64) -> Result<u64> {
    struct Count {
        used: u64,
        maximum: u64,
    }
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.used = self
                .used
                .checked_add(bytes.len() as u64)
                .filter(|used| *used <= self.maximum)
                .ok_or_else(|| std::io::Error::other("hook output budget exhausted"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { used: 0, maximum };
    serde_json::to_writer(&mut count, value).map_err(|_| exceeded())?;
    Ok(count.used)
}

fn exceeded() -> Error {
    Error::Process(ProcessError::OutputLimit {
        exit_code: None,
        message: "aggregate hook output limit exceeded".into(),
    })
}

#[cfg(test)]
mod tests;
