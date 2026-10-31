use crate::{Adapter, invalid, stream::family};
use acyclic_harness_core::{
    Result,
    model::{ModelContentPart, ModelRequest},
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

// ANT-TOOL-02, OAR-TOOL-03, OAC-TOOL-04, N-05. Restricted API IDs reserve
// this namespace. Opaque native Gemini IDs are retained and excluded below.
const PREFIX: &str = "harness_call_";

struct Binding<'a> {
    wire: Cow<'a, str>,
    freeform: bool,
    omit_gemini_id: bool,
    native_gemini: Option<&'a str>,
}

pub(crate) struct CallIds<'a>(HashMap<&'a str, Binding<'a>>);

fn ids(
    request: &ModelRequest,
    adapter: Adapter,
) -> impl Iterator<Item = (&str, Option<&str>, bool, bool)> {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(move |part| match part {
            ModelContentPart::ToolCall(call) => {
                let binding = &call.metadata["provider_call"];
                let native = (binding["provider"].as_str() == Some(family(adapter)))
                    .then(|| binding["id"].as_str())
                    .flatten();
                Some((
                    call.call_id.as_str(),
                    native,
                    adapter == Adapter::Responses
                        && binding["provider"].as_str() == Some(family(adapter))
                        && call.metadata["freeform"] == true,
                    adapter == Adapter::Gemini
                        && binding["provider"].as_str() == Some(family(adapter))
                        && binding.get("id").is_some_and(serde_json::Value::is_null),
                ))
            }
            ModelContentPart::ToolResult { call_id, .. } => {
                Some((call_id.as_str(), None, false, false))
            }
            _ => None,
        })
}

fn unchanged(id: &str) -> bool {
    !id.starts_with(PREFIX) && crate::protocol::name::CALL_ID.accepts(id)
}

impl<'a> CallIds<'a> {
    pub(crate) fn new(request: &'a ModelRequest, adapter: Adapter) -> Self {
        let mut bindings = Vec::with_capacity(ids(request, adapter).count());
        bindings.extend(ids(request, adapter).map(
            |(canonical, native, freeform, omit_gemini_id)| {
                let candidate = native.filter(|id| unchanged(id)).unwrap_or(canonical);
                (
                    canonical,
                    Binding {
                        freeform,
                        omit_gemini_id,
                        native_gemini: (adapter == Adapter::Gemini).then_some(native).flatten(),
                        wire: Cow::Borrowed(if unchanged(candidate) { candidate } else { "" }),
                    },
                )
            },
        ));
        // Calls carry the original binding; results only carry the canonical ID.
        // Prefer a valid original binding, independent of traversal order.
        bindings.sort_unstable_by(|(a_key, a), (b_key, b)| {
            let a_native = !a.wire.is_empty() && a.wire != *a_key;
            let b_native = !b.wire.is_empty() && b.wire != *b_key;
            a_key
                .cmp(b_key)
                .then_with(|| b.native_gemini.is_some().cmp(&a.native_gemini.is_some()))
                .then_with(|| b_native.cmp(&a_native))
                .then_with(|| b.freeform.cmp(&a.freeform))
                .then_with(|| b.omit_gemini_id.cmp(&a.omit_gemini_id))
                .then(a.wire.cmp(&b.wire))
        });
        bindings.dedup_by_key(|(canonical, _)| *canonical);
        bindings
            .sort_unstable_by(|(a_key, a), (b_key, b)| a.wire.cmp(&b.wire).then(a_key.cmp(b_key)));
        let mut start = 0;
        while start < bindings.len() {
            let mut end = start + 1;
            while end < bindings.len() && bindings[start].1.wire == bindings[end].1.wire {
                end += 1;
            }
            if bindings[start].1.wire.is_empty() || end - start > 1 {
                for (offset, (_, binding)) in bindings[start..end].iter_mut().enumerate() {
                    if binding.native_gemini.is_none() {
                        binding.wire = Cow::Owned(format!("{PREFIX}{}", start + offset));
                    }
                }
            }
            start = end;
        }
        // Gemini's returned ID is opaque. In particular, its native signed part
        // must not be rewritten to another API's alphabet/length restrictions.
        // Reuse in separate exchanges is valid: dispatch IDs remain distinct.
        if adapter == Adapter::Gemini {
            let native: HashSet<_> = bindings
                .iter()
                .filter_map(|(_, binding)| binding.native_gemini)
                .collect();
            let mut next = bindings.len() as u128;
            for (_, binding) in &mut bindings {
                if let Some(id) = binding.native_gemini {
                    binding.wire = Cow::Borrowed(id);
                } else if native.contains(binding.wire.as_ref()) {
                    // Existing generated ordinals are below len. Each rejection
                    // consumes one distinct native ordinal; total work is linear
                    // in this request, with no retry or history-dependent lookup.
                    loop {
                        let alias = format!("{PREFIX}{next}");
                        next += 1;
                        if !native.contains(alias.as_str()) {
                            binding.wire = Cow::Owned(alias);
                            break;
                        }
                    }
                }
            }
        }
        Self(bindings.into_iter().collect())
    }

    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
    pub(crate) fn wire<'b>(&'b self, id: &str) -> Result<Cow<'b, str>> {
        self.binding(id).map(|(wire, _, _)| wire)
    }

    pub(crate) fn binding<'b>(&'b self, id: &str) -> Result<(Cow<'b, str>, bool, bool)> {
        let binding = self
            .0
            .get(id)
            .ok_or_else(|| invalid("missing tool call ID binding"))?;
        Ok((
            Cow::Borrowed(binding.wire.as_ref()),
            binding.freeform,
            binding.omit_gemini_id,
        ))
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn freeform(&self, id: &str) -> bool {
        self.binding(id).is_ok_and(|(_, freeform, _)| freeform)
    }

    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
    pub(crate) fn assert_round_trip(request: &'a ModelRequest) {
        for adapter in [
            Adapter::Anthropic,
            Adapter::Responses,
            Adapter::Chat,
            Adapter::Gemini,
        ] {
            let table = Self::new(request, adapter);
            let mut reverse = std::collections::BTreeMap::new();
            for (&canonical, binding) in &table.0 {
                let wire = table.wire(canonical).unwrap();
                if let Some(native) = binding.native_gemini {
                    assert_eq!(wire, native);
                    continue;
                }
                assert!(!wire.is_empty() && wire.len() <= 40);
                assert!(
                    wire.bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                );
                assert!(
                    reverse.insert(wire.into_owned(), canonical).is_none(),
                    "distinct canonical exchanges collided"
                );
            }
            for (id, _, _, _) in ids(request, adapter) {
                let wire = table.wire(id).unwrap();
                if table.0[id].native_gemini.is_none() {
                    assert_eq!(reverse[wire.as_ref()], id);
                }
            }
        }
    }
}
