#[cfg(any(test, fuzzing))]
use super::{HookEvent, HookFailure};
use crate::{Error, Result};
use serde_json::Value;

/// Applied hook decisions and context for the harness loop.
#[derive(Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookOutput {
    /// Reason the guarded action was blocked.
    pub blocked: Option<String>,
    /// A pre-tool hook requested human approval, with its explanation.
    pub approval: Option<String>,
    /// Extra context to include in the next model request.
    pub additional_context: Vec<String>,
    /// Replacement arguments returned by a pre-tool hook.
    pub updated_input: Option<Value>,
    /// Command failures, including explicit failure-policy blocks.
    pub diagnostics: Vec<HookDiagnostic>,
}

/// A retained process cause and the event's failure policy.
#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookDiagnostic {
    /// The original cause; propagation moves it without conversion.
    pub cause: crate::ProcessError,
    /// Whether the configured policy blocks this event.
    pub blocking: bool,
}

impl HookOutput {
    /// Whether an explicit decision or configured failure policy blocks.
    pub fn blocks(&self) -> bool {
        self.blocked.is_some() || self.diagnostics.iter().any(|failure| failure.blocking)
    }

    /// Propagates the same retained cause, including output validation failures.
    pub fn into_result(mut self) -> Result<Self> {
        if let Some(index) = self.diagnostics.iter().position(|failure| failure.blocking) {
            return Err(Error::Process(self.diagnostics.remove(index).cause));
        }
        Ok(self)
    }

    pub(crate) fn output_limit(blocking: bool) -> Self {
        Self {
            diagnostics: vec![HookDiagnostic {
                cause: crate::ProcessError::OutputLimit {
                    exit_code: None,
                    message: "aggregate hook output limit exceeded".into(),
                },
                blocking,
            }],
            ..Default::default()
        }
    }

    // Tests and the pure fuzz target use the same reservation/merge choke point.
    #[cfg(any(test, fuzzing))]
    pub(super) fn apply(
        &mut self,
        event: HookEvent,
        policy: HookFailure,
        result: Result<(Option<i32>, Vec<u8>, Vec<u8>)>,
    ) {
        let mut budget = super::budget::Budget::existing(65_536, self).unwrap();
        budget.apply(self, event, policy, result);
        budget.finish(self);
    }

    #[cfg(any(test, fuzzing))]
    pub(super) fn observe(
        &mut self,
        event: HookEvent,
        code: Option<i32>,
        stdout: &[u8],
        stderr: &[u8],
    ) -> Result<bool> {
        super::budget::Budget::new(65_536)?.observe(self, event, code, stdout, stderr)
    }
}

#[cfg(test)]
#[path = "output/properties.rs"]
mod properties;
