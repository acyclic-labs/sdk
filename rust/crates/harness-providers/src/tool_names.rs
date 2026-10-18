use acyclic_harness::{
    Result,
    model::{ModelContentPart, ModelRequest},
};
use std::borrow::Cow;

// N-05, ANT-TOOL-01, OAR-TOOL-01, OAC-TOOL-03, GEM-TOOL-01.
// Reserve the namespace, including canonical names that already use it.
const PREFIX: &str = "harness_tool_";

pub(crate) struct Names<'a>(Vec<&'a str>);

fn names(request: &ModelRequest) -> impl Iterator<Item = &str> {
    request.tools.iter().map(|tool| tool.name.as_str()).chain(
        request
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::ToolCall(call) => Some(call.name.as_str()),
                ModelContentPart::ToolResult { name, .. } => Some(name.as_str()),
                _ => None,
            }),
    )
}

fn unchanged(name: &str) -> bool {
    !name.starts_with(PREFIX) && crate::protocol::name::TOOL.accepts(name)
}

impl<'a> Names<'a> {
    pub(crate) fn controls(
        &self,
        adapter: crate::Adapter,
        options: &mut serde_json::Value,
    ) -> Result<()> {
        let pointer = match adapter {
            crate::Adapter::Anthropic | crate::Adapter::Responses => "/tool_choice/name",
            crate::Adapter::Chat => "/tool_choice/function/name",
            crate::Adapter::Gemini => "/toolConfig/functionCallingConfig/allowedFunctionNames",
        };
        if let Some(value) = options.pointer_mut(pointer) {
            if adapter == crate::Adapter::Gemini {
                for name in value
                    .as_array_mut()
                    .ok_or_else(|| crate::invalid("allowed tool names must be an array"))?
                {
                    self.control_name(name)?;
                }
            } else {
                self.control_name(value)?;
            }
        }
        if matches!(adapter, crate::Adapter::Responses | crate::Adapter::Chat) {
            if let Some(tools) = options.pointer_mut("/tool_choice/tools") {
                for tool in tools
                    .as_array_mut()
                    .ok_or_else(|| crate::invalid("allowed tools must be an array"))?
                {
                    let pointer = if adapter == crate::Adapter::Chat {
                        "/function/name"
                    } else {
                        "/name"
                    };
                    if let Some(name) = tool.pointer_mut(pointer) {
                        self.control_name(name)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn control_name(&self, value: &mut serde_json::Value) -> Result<()> {
        let name = value
            .as_str()
            .ok_or_else(|| crate::invalid("selected tool name must be a string"))?;
        if let Cow::Owned(alias) = self.wire(name)? {
            *value = alias.into();
        }
        Ok(())
    }

    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
    pub(crate) fn assert_round_trip(request: &'a ModelRequest) {
        let table = Self::new(request);
        for adapter in [
            crate::Adapter::Anthropic,
            crate::Adapter::Responses,
            crate::Adapter::Chat,
            crate::Adapter::Gemini,
        ] {
            let mut options = request.model.options.clone();
            let _ = table.controls(adapter, &mut options);
        }
        let mut seen = std::collections::BTreeSet::new();
        for &name in &table.0 {
            let wire = table.wire(name).unwrap();
            assert!(!wire.is_empty() && wire.len() <= 64);
            assert!(wire.as_bytes()[0].is_ascii_alphabetic() || wire.starts_with('_'));
            assert!(
                wire.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            );
            assert!(
                seen.insert(wire.to_string()),
                "distinct canonical names collided"
            );
            let mut event = acyclic_harness::model::ModelEvent::ToolCall(
                acyclic_harness::model::ToolCall::new(
                    "call".into(),
                    wire.into_owned(),
                    serde_json::json!({}),
                ),
            );
            table.restore(&mut event).unwrap();
            let acyclic_harness::model::ModelEvent::ToolCall(call) = event else {
                unreachable!()
            };
            assert_eq!(call.name, name);
            let _ = table.canonical(name); // Also exercise malformed/untrusted alias strings.
        }
    }

    pub(crate) fn new(request: &'a ModelRequest) -> Self {
        Self::with_capacity(request, names(request).count())
    }

    fn with_capacity(request: &'a ModelRequest, count: usize) -> Self {
        if count == 0 {
            return Self(Vec::new());
        }
        let mut values = Vec::with_capacity(count);
        values.extend(names(request));
        values.sort_unstable();
        values.dedup();
        Self(values)
    }

    pub(crate) fn wire<'n>(&self, name: &'n str) -> Result<Cow<'n, str>> {
        if unchanged(name) {
            return Ok(Cow::Borrowed(name));
        }
        let index = self
            .0
            .binary_search(&name)
            .map_err(|_| crate::invalid("missing tool name binding"))?;
        Ok(Cow::Owned(format!("{PREFIX}{index}")))
    }

    pub(crate) fn canonical(&self, wire: &str) -> Result<Option<&'a str>> {
        let Some(index) = wire.strip_prefix(PREFIX) else {
            return Ok(None);
        };
        if index.is_empty()
            || !index.bytes().all(|byte| byte.is_ascii_digit())
            || (index.len() > 1 && index.starts_with('0'))
        {
            return Err(crate::invalid("invalid model tool alias"));
        }
        let name = index
            .parse::<usize>()
            .ok()
            .and_then(|index| self.0.get(index))
            .copied()
            .ok_or_else(|| crate::invalid("unknown model tool alias"))?;
        if unchanged(name) {
            return Err(crate::invalid("invalid model tool alias"));
        }
        Ok(Some(name))
    }

    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
    pub(crate) fn restore(&self, event: &mut acyclic_harness::model::ModelEvent) -> Result<()> {
        if let acyclic_harness::model::ModelEvent::ToolCall(call) = event {
            if let Some(name) = self.canonical(&call.name)? {
                call.name = name.into();
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native(
        request: &'a ModelRequest,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let count = names(request).count() as u64;
        let bytes = count
            .checked_mul(std::mem::size_of::<&str>() as u64)
            .ok_or_else(|| crate::invalid("tool name binding size overflow"))?;
        let custody = work.claim(
            bytes,
            u64::from(count != 0),
            acyclic_fs::WorkCounters {
                bytes_copied: bytes,
                items_examined: count,
                ..acyclic_fs::WorkCounters::UNCHARGED
            },
        )?;
        Ok((Self::with_capacity(request, count as usize), custody))
    }
}
