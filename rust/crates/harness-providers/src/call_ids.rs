use crate::{Adapter, invalid, stream::family};
use acyclic_harness_core::{
    Result,
    model::{ModelContentPart, ModelRequest},
};
use std::{
    borrow::Cow,
    collections::{HashMap, hash_map::Entry},
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
    pub(crate) fn new(request: &'a ModelRequest, adapter: Adapter) -> Result<Self> {
        if adapter == Adapter::Gemini {
            return Self::gemini(request);
        }
        let mut bindings = Vec::with_capacity(ids(request, adapter).count());
        bindings.extend(ids(request, adapter).map(
            |(canonical, native, freeform, omit_gemini_id)| {
                let candidate = native.filter(|id| unchanged(id)).unwrap_or(canonical);
                (
                    canonical,
                    Binding {
                        freeform,
                        omit_gemini_id,
                        native_gemini: None,
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
                    binding.wire = Cow::Owned(format!("{PREFIX}{}", start + offset));
                }
            }
            start = end;
        }
        Ok(Self(bindings.into_iter().collect()))
    }

    fn gemini(request: &'a ModelRequest) -> Result<Self> {
        let mut bindings = HashMap::new();
        for (canonical, native, freeform, omit_gemini_id) in ids(request, Adapter::Gemini) {
            match bindings.entry(canonical) {
                Entry::Vacant(entry) => {
                    entry.insert(Binding {
                        // The API accepts opaque IDs, including our original
                        // canonical IDs. It needs no restricted-API alias.
                        wire: Cow::Borrowed(native.unwrap_or(canonical)),
                        freeform,
                        omit_gemini_id,
                        native_gemini: native,
                    });
                }
                Entry::Occupied(mut entry) => {
                    let binding = entry.get_mut();
                    if (native.is_some() && binding.omit_gemini_id)
                        || (omit_gemini_id && binding.native_gemini.is_some())
                    {
                        return Err(invalid(
                            "Gemini call has conflicting ID presence; recovery: retain its original provider call binding",
                        ));
                    }
                    if let Some(id) = native {
                        if binding.native_gemini.is_some_and(|prior| prior != id) {
                            return Err(invalid(
                                "Gemini call has conflicting native IDs; recovery: retain its original provider call binding",
                            ));
                        }
                        binding.wire = Cow::Borrowed(id);
                        binding.native_gemini = Some(id);
                    }
                    binding.omit_gemini_id |= omit_gemini_id;
                }
            }
        }
        // Native IDs may repeat in separately answered exchanges. Every other
        // binding is request-independent: appending history cannot rename it.
        // Refuse a collision instead of changing any earlier signed/cache bytes.
        let mut wires = HashMap::with_capacity(bindings.len());
        for binding in bindings.values() {
            if binding.omit_gemini_id {
                continue;
            }
            let native = binding.native_gemini.is_some();
            match wires.entry(binding.wire.as_ref()) {
                Entry::Vacant(entry) => {
                    entry.insert(native);
                }
                Entry::Occupied(entry) if !native || !*entry.get() => {
                    return Err(invalid(
                        "Gemini native and projected call IDs collide; recovery: start an exchange without the conflicting foreign call",
                    ));
                }
                Entry::Occupied(_) => {}
            }
        }
        drop(wires);
        Ok(Self(bindings))
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
            let table = match Self::new(request, adapter) {
                Ok(table) => table,
                Err(_) => {
                    assert_eq!(adapter, Adapter::Gemini);
                    // Fuzzed transcripts can forge colliding native metadata.
                    // Refusal must survive the original durable codec too.
                    let bytes = serde_json::to_vec(request).unwrap();
                    let restored = serde_json::from_slice(&bytes).unwrap();
                    assert!(CallIds::new(&restored, adapter).is_err());
                    continue;
                }
            };
            let mut reverse = std::collections::BTreeMap::new();
            for (&canonical, binding) in &table.0 {
                let wire = table.wire(canonical).unwrap();
                if adapter == Adapter::Gemini {
                    assert_eq!(wire, binding.native_gemini.unwrap_or(canonical));
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
                if adapter != Adapter::Gemini {
                    assert_eq!(reverse[wire.as_ref()], id);
                }
            }
        }
    }
}
