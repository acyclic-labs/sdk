use acyclic_harness::{
    Result,
    model::{ModelContentPart, ModelRequest},
};
use std::borrow::Cow;

// N-05, ANT-TOOL-01, OAR-TOOL-01, OAC-TOOL-03, GEM-TOOL-01.
// A provider-invalid canonical name gets an alias derived from the name alone,
// so it never depends on its position among the request's tools: `.` becomes
// `__` (`mcp.server.tool` -> `mcp__server__tool`) and any other unsupported
// byte becomes `_`. An alias that is too long or collides with another wire
// name instead keeps a readable prefix and appends a digest of the canonical
// name.
const DIGEST_HEX: usize = 16;

struct Binding<'a> {
    canonical: &'a str,
    wire: Cow<'a, str>,
}

/// Bindings sorted by canonical name, plus their indices sorted by wire name.
pub(crate) struct Names<'a>(Vec<Binding<'a>>, Vec<usize>);

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
    crate::protocol::name::TOOL.accepts(name)
}

fn readable(name: &str, maximum: usize) -> String {
    let mut alias = String::with_capacity(maximum.min(name.len().saturating_mul(2)));
    if !name
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
    {
        alias.push('_');
    }
    for byte in name.bytes() {
        if alias.len() >= maximum {
            break;
        }
        match byte {
            b'.' => alias.push_str("__"),
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') => {
                alias.push(char::from(byte));
            }
            _ => alias.push('_'),
        }
    }
    alias.truncate(maximum);
    alias
}

fn digested(name: &str, salt: u32) -> String {
    let maximum = crate::protocol::name::TOOL.maximum - DIGEST_HEX - 1;
    let mut alias = readable(name, maximum);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic:tool-name:v1\0");
    hasher.update(&salt.to_le_bytes());
    hasher.update(name.as_bytes());
    alias.push('_');
    alias.push_str(&hasher.finalize().to_hex()[..DIGEST_HEX]);
    alias
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
        for name in table.0.iter().map(|binding| binding.canonical) {
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
            return Self(Vec::new(), Vec::new());
        }
        let mut canonical = Vec::with_capacity(count);
        canonical.extend(names(request));
        canonical.sort_unstable();
        canonical.dedup();
        let mut bindings: Vec<Binding<'a>> = canonical
            .into_iter()
            .map(|name| Binding {
                canonical: name,
                wire: if unchanged(name) {
                    Cow::Borrowed(name)
                } else {
                    let alias = readable(name, crate::protocol::name::TOOL.maximum + 1);
                    Cow::Owned(if unchanged(&alias) {
                        alias
                    } else {
                        digested(name, 0)
                    })
                },
            })
            .collect();
        // Colliding aliases take digests; an unchanged name keeps its wire.
        // Salts advance in canonical order, so the result is a function of
        // the name set alone.
        let mut salt = 0_u32;
        let mut order: Vec<usize> = Vec::with_capacity(bindings.len());
        loop {
            order.clear();
            order.extend(0..bindings.len());
            order.sort_unstable_by(|&a, &b| {
                bindings[a]
                    .wire
                    .cmp(&bindings[b].wire)
                    .then(bindings[a].canonical.cmp(bindings[b].canonical))
            });
            let mut collided = Vec::new();
            for pair in order.windows(2) {
                if bindings[pair[0]].wire == bindings[pair[1]].wire {
                    collided.extend(
                        pair.iter()
                            .copied()
                            .filter(|&index| !unchanged(bindings[index].canonical)),
                    );
                }
            }
            if collided.is_empty() {
                break;
            }
            collided.sort_unstable();
            collided.dedup();
            salt = salt.saturating_add(1);
            for index in collided {
                bindings[index].wire = Cow::Owned(digested(bindings[index].canonical, salt));
            }
        }
        Self(bindings, order)
    }

    pub(crate) fn wire<'n>(&self, name: &'n str) -> Result<Cow<'n, str>> {
        if unchanged(name) {
            return Ok(Cow::Borrowed(name));
        }
        let index = self
            .0
            .binary_search_by(|binding| binding.canonical.cmp(name))
            .map_err(|_| crate::invalid("missing tool name binding"))?;
        Ok(Cow::Owned(self.0[index].wire.clone().into_owned()))
    }

    /// The canonical name behind a provider alias; `None` for a wire name
    /// that is itself canonical or undeclared, which tool admission validates.
    /// A name outside the provider grammar was never declared, so it rejects
    /// the response.
    pub(crate) fn canonical(&self, wire: &str) -> Result<Option<&'a str>> {
        match self
            .1
            .binary_search_by(|&index| self.0[index].wire.as_ref().cmp(wire))
        {
            Ok(found) => {
                let binding = &self.0[self.1[found]];
                Ok((binding.canonical != wire).then_some(binding.canonical))
            }
            Err(_) if unchanged(wire) => Ok(None),
            Err(_) => Err(crate::invalid("provider named an undeclared tool alias")),
        }
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
        // Each binding and an alias bounded by the provider name maximum.
        let per_name = (std::mem::size_of::<Binding<'_>>()
            + std::mem::size_of::<usize>()
            + crate::protocol::name::TOOL.maximum
            + 1) as u64;
        let bytes = count
            .checked_mul(per_name)
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
