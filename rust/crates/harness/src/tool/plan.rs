//! Task-local plans published through the ordinary authenticated file boundary.

use crate::{NativeCustody, Result};
use serde::{Deserialize, Serialize};

/// Canonical plan path in the task's bound output volume.
pub const PLAN_PATH: &str = "plan.md";

/// Replaces the complete plan; an empty list clears the steps.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdatePlanInput {
    /// Optional reason for this revision, at most 8192 UTF-8 bytes.
    #[serde(default)]
    #[schemars(length(max = 8192))]
    pub explanation: Option<String>,
    /// Ordered steps, with at most one currently in progress.
    #[schemars(length(max = 100))]
    pub plan: Vec<PlanStep>,
}

/// One model-visible plan step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    /// Nonempty single-line description, at most 1024 UTF-8 bytes.
    #[schemars(length(min = 1, max = 1024))]
    pub step: String,
    /// Current execution status.
    pub status: PlanStatus,
}

/// Finite step lifecycle shared by the tool and client renderers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    /// Work has not started.
    Pending,
    /// Work is underway.
    InProgress,
    /// Work has finished.
    Completed,
}

/// Builds the independently registerable `update_plan` tool.
pub fn update_plan() -> Result<super::Tool> {
    super::files::FileTool::Plan.assemble(super::schema::ProjectionMode::Full)
}

pub(super) struct PlanView<'a> {
    explanation: Option<&'a str>,
    steps: &'a [serde_json::Value],
    markdown_bytes: usize,
}

fn step<'a>(
    value: &'a serde_json::Value,
    custody: &NativeCustody,
) -> Result<(&'a str, PlanStatus)> {
    let invalid = || super::edit::invalid(custody, "plan step requires only text and status");
    let object = value
        .as_object()
        .filter(|value| value.len() == 2)
        .ok_or_else(invalid)?;
    let text = object
        .get("step")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(invalid)?;
    let status = match object.get("status").and_then(serde_json::Value::as_str) {
        Some("pending") => PlanStatus::Pending,
        Some("in_progress") => PlanStatus::InProgress,
        Some("completed") => PlanStatus::Completed,
        _ => return Err(invalid()),
    };
    Ok((text, status))
}

impl<'a> PlanView<'a> {
    pub(super) fn borrow(
        arguments: &'a serde_json::Value,
        custody: &NativeCustody,
    ) -> Result<Self> {
        let invalid = |reason| super::edit::invalid(custody, reason);
        let object = arguments
            .as_object()
            .filter(|object| {
                object.len() <= 2
                    && object
                        .keys()
                        .all(|key| matches!(key.as_str(), "plan" | "explanation"))
            })
            .ok_or_else(|| invalid("plan requires only steps and an optional explanation"))?;
        let steps = object
            .get("plan")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| invalid("plan requires a step list"))?;
        let explanation = match object.get("explanation") {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => Some(
                value
                    .as_str()
                    .ok_or_else(|| invalid("plan explanation must be text"))?,
            ),
        };
        super::edit::charge(custody, 0, explanation.map_or(0, str::len) as u64)?;
        if steps.len() > 100
            || explanation.is_some_and(|text| {
                text.len() > 8192 || text.chars().any(|c| c.is_control() && c != '\n')
            })
        {
            return Err(invalid("plan exceeds step or explanation limits"));
        }
        let mut active = 0;
        let mut markdown_bytes = 8 + explanation.map_or(0, |text| text.len() + 2);
        for value in steps {
            let (text, status) = step(value, custody)?;
            super::edit::charge(custody, 0, text.len() as u64 * 2 + 4)?;
            if text.trim().is_empty() || text.len() > 1024 || text.chars().any(char::is_control) {
                return Err(invalid(
                    "plan steps require bounded nonempty single-line text",
                ));
            }
            active += usize::from(status == PlanStatus::InProgress);
            markdown_bytes += text.len() + 7;
        }
        if active > 1 {
            return Err(invalid("plan has more than one step in progress"));
        }
        Ok(Self {
            explanation,
            steps,
            markdown_bytes,
        })
    }

    pub(super) fn markdown(
        &self,
        maximum_bytes: u64,
        custody: &mut NativeCustody,
    ) -> Result<String> {
        let length = self.markdown_bytes;
        if length as u64 > maximum_bytes {
            return Err(super::edit::invalid(
                custody,
                "plan exceeds task file byte limit",
            ));
        }
        let mut text = String::new();
        super::edit::reserve_string(custody, &mut text, length)?;
        super::edit::charge(custody, length as u64, 0)?;
        text.push_str("# Plan\n\n");
        if let Some(explanation) = self.explanation {
            text.push_str(explanation);
            text.push_str("\n\n");
        }
        for value in self.steps {
            let (description, status) = step(value, custody)?;
            text.push_str(match status {
                PlanStatus::Pending => "- [ ] ",
                PlanStatus::InProgress => "- [~] ",
                PlanStatus::Completed => "- [x] ",
            });
            text.push_str(description);
            text.push('\n');
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plan_rejects_invalid_steps_statuses_fields_and_bounds() -> Result<()> {
        let invalid = [
            json!(null),
            json!([]),
            json!({}),
            json!({"plan":null}),
            json!({"plan":[],"explanation":7}),
            json!({"plan":[null]}),
            json!({"plan":[{"status":"pending"}]}),
            json!({"plan":[{"step":7,"status":"pending"}]}),
            json!({"plan":[{"step":"one","status":7}]}),
            json!({"plan":[{"step":"one","status":"pending","extra":true}]}),
            json!({"plan":[{"step":"","status":"pending"}]}),
            json!({"plan":[{"step":" \t","status":"pending"}]}),
            json!({"plan":[{"step":"one\ntwo","status":"pending"}]}),
            json!({"plan":[{"step":"one\u{1b}","status":"pending"}]}),
            json!({"plan":[{"step":"x".repeat(1025),"status":"pending"}]}),
            json!({"plan":[{"step":"one","status":"started"}]}),
            json!({"plan":[{"step":"one","status":"in_progress"},{"step":"two","status":"in_progress"}]}),
            json!({"plan":[],"path":"other.md"}),
            json!({"plan":[],"explanation":"x".repeat(8193)}),
            json!({"plan":[],"explanation":"\u{1b}"}),
            json!({"plan":vec![json!({"step":"one","status":"pending"});101]}),
        ];
        for value in invalid {
            assert!(PlanView::borrow(&value, &None).is_err(), "{value}");
        }
        let arguments = json!({"plan":[{"step":"é".repeat(512),"status":"pending"}]});
        let input = PlanView::borrow(&arguments, &None)?;
        let markdown = input.markdown(1039, &mut None)?;
        assert_eq!(markdown.len(), 1039);
        assert!(input.markdown(1038, &mut None).is_err());
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn plan_rendering_uses_and_releases_the_original_native_budget() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, performance::work_budget};
        let arguments = json!({"plan":[{"step":"Inspect","status":"in_progress"}]});
        let minimum = NativeMaterializationWork::accounting_state_bytes()
            + std::mem::size_of::<String>() as u64;
        for (items, bytes, succeeds) in [
            (10_000, 65_536, true),
            (1, 65_536, false),
            (10_000, minimum, false),
        ] {
            let work = NativeMaterializationWork::new(
                work_budget(items, bytes),
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
            let before = work.live_allocation_bytes();
            let mut custody =
                super::super::edit::allocation(Some(&work), std::mem::size_of::<String>())?;
            let result = PlanView::borrow(&arguments, &custody)
                .and_then(|input| input.markdown(65_536, &mut custody));
            assert_eq!(result.is_ok(), succeeds);
            if let Ok(text) = &result {
                assert!(work.live_allocation_bytes() >= before + text.capacity() as u64);
                assert_eq!(text, "# Plan\n\n- [~] Inspect\n");
            }
            drop(result);
            drop(custody);
            assert_eq!(work.live_allocation_bytes(), before);
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn plan_preflight_borrows_100_steps_and_constructs_markdown_once() -> Result<()> {
        use crate::{Capabilities, OperationId, conversation::Limits, runtime::RuntimeScope};
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, performance::work_budget};

        let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?;
        let mut preflight_allocations = None;
        for count in [0, 100] {
            let invocation = super::super::ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "plan".into(),
                name: "update_plan".into(),
                arguments: json!({"plan":vec![json!({"step":"é".repeat(512),"status":"pending"});count]}),
            };
            let work = NativeMaterializationWork::new(
                work_budget(1_000_000, 1_000_000),
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
            let before = work.receipt();
            // Caller authorization and run's repeated authorization use this same preflight.
            for _ in 0..2 {
                let (file, custody) = super::super::files::FileTool::Plan.preflight(
                    &scope,
                    &invocation,
                    Some(&work),
                )?;
                assert!(file.is_none());
                assert_eq!(custody.as_ref().map(|value| value.bytes()), Some(0));
            }
            let after_preflight = work.receipt();
            assert_eq!(after_preflight.bytes_copied, before.bytes_copied);
            // The generic JSON-bound diagnostics cost the same for an empty or full plan.
            let allocations = after_preflight.allocation_operations - before.allocation_operations;
            assert_eq!(
                *preflight_allocations.get_or_insert(allocations),
                allocations
            );
            let mut custody =
                super::super::edit::allocation(Some(&work), std::mem::size_of::<String>())?;
            let view = PlanView::borrow(&invocation.arguments, &custody)?;
            let text = view.markdown(1_000_000, &mut custody)?;
            let after_render = work.receipt();
            assert_eq!(
                after_render.allocation_operations - after_preflight.allocation_operations,
                1
            );
            assert_eq!(
                after_render.bytes_copied - after_preflight.bytes_copied,
                text.len() as u64
            );
            assert_eq!(text.lines().count(), count + 2);
        }
        Ok(())
    }
}
