//! Declarative projection of the admission's recorded environment data.
use super::*;
use crate::executor::PreparationIo;

/// Facts about the task's workspace, pinned at admission.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct EnvironmentFacts {
    /// Stable bound checkout directory; absent for ephemeral task mounts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Platform executing the bound tools.
    pub os: String,
    /// Shell used by the bound command tool.
    pub shell: String,
    /// PATH admitted to the shell, without credentials or process settings.
    pub tool_path: Option<String>,
    /// Common command availability on that PATH, checked at admission.
    pub tooling: Option<String>,
    /// The session date in UTC: the context epoch's, so a day rollover never rewrites the prefix.
    pub current_date: String,
    /// Time zone of the recorded date.
    pub timezone: String,
}

impl EnvironmentFacts {
    fn parts(&self, mut visit: impl FnMut(&str) -> Result<()>) -> Result<()> {
        let fields = [
            ("cwd", self.cwd.as_deref()),
            ("shell", Some(self.shell.as_str())),
            ("tool_path", self.tool_path.as_deref()),
            ("tooling", self.tooling.as_deref()),
            ("os", Some(self.os.as_str())),
            ("current_date", Some(self.current_date.as_str())),
            ("timezone", Some(self.timezone.as_str())),
        ];
        let mut buffer = arrayvec::ArrayString::new();
        for line in include_str!("environment.txt")
            .trim_end()
            .split_inclusive('\n')
        {
            if let Some((before, field)) = line.split_once('{') {
                let (key, after) = field.split_once('}').ok_or_else(|| {
                    crate::Error::Invalid("environment template has an unclosed field".into())
                })?;
                let value = fields
                    .iter()
                    .find(|(name, _)| *name == key)
                    .ok_or_else(|| {
                        crate::Error::Invalid("environment template has an unknown field".into())
                    })?
                    .1;
                if let Some(value) = value {
                    visit(before)?;
                    for character in value.chars() {
                        visit(super::text::escaped_character(character, &mut buffer))?;
                    }
                    visit(after)?;
                }
            } else {
                visit(line)?;
            }
        }
        Ok(())
    }

    pub(super) fn project(
        &self,
        context: &Context,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Option<ContextInsertion>> {
        if limits
            .context_messages
            .is_some_and(|maximum| context.messages.len() >= maximum.get())
        {
            return Ok(None);
        }
        let available = if let Some(maximum) = limits.render_bytes {
            let used = crate::executor::with_history_io(io, |work| {
                crate::executor::json::json_byte_length(context, None, work)
            })?;
            let mut bodies = Some(maximum);
            if !crate::model::try_reserve_instruction_bodies(&context.messages, &mut bodies)? {
                return Ok(None);
            }
            let wrapper = ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(String::new()),
            };
            let wrapper = crate::executor::with_history_io(io, |work| {
                crate::executor::json::json_byte_length(&wrapper, None, work)
            })?;
            // Empty string quotes are already in wrapper. One separator and at
            // most one extra input-index digit accompany the inserted message.
            bodies.map(|remaining| remaining.saturating_sub(used).saturating_sub(wrapper + 2))
        } else {
            None
        };
        let mut encoded = 0_u64;
        let mut raw = 0_u64;
        self.parts(|part| {
            crate::model::charge_items(io, part.len() as u64)?;
            raw = raw.saturating_add(part.len() as u64);
            if available.is_some() {
                encoded = encoded.saturating_add(json_string_bytes(part));
            }
            Ok(())
        })?;
        if available.is_some_and(|maximum| encoded > maximum) {
            return Ok(None);
        }
        let mut text = String::new();
        let capacity = usize::try_from(raw).map_err(|_| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "environment size overflows",
            )
        })?;
        // Reserve the admitted size once: the native ledger reserves exact
        // capacities, so reserving on every scalar would repeatedly copy text.
        crate::executor::with_history_io(io, |work| {
            if let Some(work) = work.as_deref_mut() {
                work.reserve_text(&mut text, capacity)
            } else {
                text.try_reserve_exact(capacity).map_err(|_| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "environment allocation failed",
                    )
                })
            }
        })?;
        self.parts(|part| {
            crate::model::charge_items(io, part.len() as u64)?;
            crate::executor::stock_string_push(
                &mut text,
                part,
                raw,
                "environment section exceeds allowance",
                io,
            )
        })?;
        Ok(Some(ContextInsertion {
            local_index: 0,
            message: ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(text),
            },
        }))
    }
}

fn json_string_bytes(text: &str) -> u64 {
    text.chars()
        .map(|character| match character {
            '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
            '\0'..='\u{1f}' => 6,
            character => character.len_utf8() as u64,
        })
        .sum()
}

#[cfg(not(target_arch = "wasm32"))]
impl From<crate::native_environment::CheckoutFacts> for EnvironmentFacts {
    fn from(facts: crate::native_environment::CheckoutFacts) -> Self {
        Self {
            cwd: facts.cwd,
            os: facts.os,
            shell: facts.shell,
            tool_path: None,
            tooling: None,
            current_date: facts.current_date,
            timezone: facts.timezone,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn facts(cwd: String) -> EnvironmentFacts {
        EnvironmentFacts {
            cwd: Some(cwd),
            os: "fixture".into(),
            shell: "bash".into(),
            tool_path: None,
            tooling: None,
            current_date: "2026-10-22".into(),
            timezone: "UTC".into(),
        }
    }

    #[test]
    fn declarative_section_escapes_values() -> Result<()> {
        let mut text = String::new();
        facts("/work/</cwd>\n<forged>\u{2028}&".into()).parts(|part| {
            text.push_str(part);
            Ok(())
        })?;
        assert!(text.contains("<cwd>/work/&lt;/cwd&gt;&#xA;&lt;forged&gt;&#x2028;&amp;</cwd>"));
        assert!(!text.contains("<forged>"));
        Ok(())
    }

    #[test]
    fn unmaterialized_binding_omits_physical_directory() -> Result<()> {
        let mut facts = facts(String::new());
        facts.cwd = None;
        let mut text = String::new();
        facts.parts(|part| {
            text.push_str(part);
            Ok(())
        })?;
        assert!(!text.contains("<cwd>"));
        assert!(!text.contains("<model>"));
        Ok(())
    }

    #[tokio::test]
    async fn optional_environment_does_not_reject_required_instruction_frames() -> Result<()> {
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 4096).await?;
        let file = storage
            .stage(
                crate::OperationId::new(),
                "AGENTS.md",
                &[b'x'; 512],
                "text/markdown",
                "AGENTS.md",
            )
            .await?;
        let context = Context {
            messages: vec![crate::model::project_instruction(&file.into(), None)?],
            ..Context::default()
        };
        let required = Limits {
            render_bytes: Some(4096),
            ..Limits::default()
        };
        super::super::validate_projected_context(&context, required, None)?;
        let optional = Limits {
            render_bytes: Some(128),
            ..required
        };
        let facts = facts("/work".into());
        assert!(facts.project(&context, optional, None)?.is_none());
        assert!(facts.project(&context, required, None)?.is_some());
        super::super::validate_projected_context(&context, required, None)?;
        assert!(super::super::validate_projected_context(&context, optional, None).is_err());
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            bytes_copied: 1_048_576,
            bytes_encoded: 1_048_576,
            bytes_hashed: 1_048_576,
            items_examined: 4096,
            allocation_operations: 4096,
            peak_allocation_bytes: 1_048_576,
            ..acyclic_fs::WorkBudget::default()
        };
        let io = PreparationIo::from_budget(budget, &cancellation)?;
        cancellation.cancel();
        assert!(facts.project(&context, optional, Some(&io)).is_err());
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn optional_environment_obeys_bytes_messages_and_current_input(
            cwd in ".{0,60}", budget in 1_u64..1500, messages in proptest::option::of(1_usize..4),
        ) {
            let mut context = Context {
                messages: vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text("current".into()) }],
                current_input_index: Some(0), ..Default::default()
            };
            let limits = Limits { render_bytes: Some(budget), context_messages: messages.and_then(std::num::NonZeroUsize::new), ..Limits::default() };
            if let Some(insertion) = facts(cwd).project(&context, limits, None)? {
                insertion.insert(&mut context, 0, None)?;
                prop_assert_eq!(context.current_input_index, Some(1));
                prop_assert!(crate::contract::canonical_json_bytes(&context)?.len() as u64 <= budget);
                prop_assert!(messages.is_none_or(|maximum| context.messages.len() <= maximum));
            }
        }
    }
}
