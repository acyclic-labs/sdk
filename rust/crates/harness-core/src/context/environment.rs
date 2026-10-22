//! Declarative projection of the admission's recorded environment data.
use super::*;
use crate::executor::PreparationIo;

/// Facts about the task's workspace and selected model, pinned at admission.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct EnvironmentFacts {
    /// Bound checkout directory, never the harness process's working directory.
    pub cwd: String,
    /// Platform executing the bound tools.
    pub os: String,
    /// Shell used by the bound command tool.
    pub shell: String,
    /// Admission date in UTC.
    pub current_date: String,
    /// Time zone of the recorded date.
    pub timezone: String,
    /// Applicable checkout branch and status; absent for independent SDK Git views.
    pub git_status: Option<String>,
    /// Selected provider model name.
    pub model: String,
}

impl EnvironmentFacts {
    fn parts(&self, mut visit: impl FnMut(&str) -> Result<()>) -> Result<()> {
        let fields = [
            ("cwd", Some(self.cwd.as_str())),
            ("shell", Some(self.shell.as_str())),
            ("os", Some(self.os.as_str())),
            ("current_date", Some(self.current_date.as_str())),
            ("timezone", Some(self.timezone.as_str())),
            ("git_status", self.git_status.as_deref()),
            ("model", Some(self.model.as_str())),
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
        if context.messages.len() >= limits.context_messages {
            return Ok(None);
        }
        let used = crate::executor::with_history_io(io, |work| {
            crate::executor::json::json_byte_length(context, u64::MAX, work)
        })?;
        let mut bodies = u64::MAX;
        crate::model::reserve_instruction_bodies(&context.messages, &mut bodies, io)?;
        let wrapper = ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text(String::new()),
        };
        let wrapper = crate::executor::with_history_io(io, |work| {
            crate::executor::json::json_byte_length(&wrapper, u64::MAX, work)
        })?;
        // Empty string quotes are already in wrapper. One separator and at
        // most one extra input-index digit accompany the inserted message.
        let available = limits
            .render_bytes
            .saturating_sub(used)
            .saturating_sub(u64::MAX - bodies)
            .saturating_sub(wrapper + 2);
        let mut encoded = 0_u64;
        let mut raw = 0_u64;
        self.parts(|part| {
            crate::model::charge_items(io, part.len() as u64)?;
            raw = raw.saturating_add(part.len() as u64);
            encoded = encoded.saturating_add(json_string_bytes(part));
            Ok(())
        })?;
        if encoded > available {
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
                role: ModelRole::System,
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

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<crate::native_environment::CheckoutFacts> for EnvironmentFacts {
    fn from(facts: crate::native_environment::CheckoutFacts) -> Self {
        Self {
            cwd: facts.cwd,
            os: facts.os,
            shell: facts.shell,
            current_date: facts.current_date,
            timezone: facts.timezone,
            git_status: facts.git_status,
            model: facts.model,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn facts(cwd: String) -> EnvironmentFacts {
        EnvironmentFacts {
            cwd,
            os: "fixture".into(),
            shell: "bash".into(),
            current_date: "2026-10-22".into(),
            timezone: "UTC".into(),
            git_status: None,
            model: "model".into(),
        }
    }

    #[test]
    fn declarative_section_escapes_values_and_omits_absent_git() -> Result<()> {
        let mut text = String::new();
        facts("/work/</cwd>\n<forged>\u{2028}&".into()).parts(|part| {
            text.push_str(part);
            Ok(())
        })?;
        assert!(text.contains("<cwd>/work/&lt;/cwd&gt;&#xA;&lt;forged&gt;&#x2028;&amp;</cwd>"));
        assert!(!text.contains("git_status"));
        assert!(!text.contains("<forged>"));
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn optional_environment_obeys_bytes_messages_and_current_input(
            cwd in ".{0,60}", budget in 1_u64..1500, messages in 1_usize..4,
        ) {
            let mut context = Context {
                messages: vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text("current".into()) }],
                current_input_index: Some(0), ..Default::default()
            };
            let limits = Limits { render_bytes: budget, context_messages: messages, ..Limits::default() };
            if let Some(insertion) = facts(cwd).project(&context, limits, None)? {
                insertion.insert(&mut context, 0, None)?;
                prop_assert_eq!(context.current_input_index, Some(1));
                prop_assert!(crate::contract::canonical_json_bytes(&context)?.len() as u64 <= budget);
                prop_assert!(context.messages.len() <= messages);
            }
        }
    }
}
