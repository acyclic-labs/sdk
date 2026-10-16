use crate::{Adapter, invalid, stream::family};
use acyclic_harness::{
    Result,
    model::{ModelContentPart, ModelRequest},
};
use std::borrow::Cow;

// ANT-TOOL-02, OAR-TOOL-03, OAC-TOOL-04, N-05. The alias namespace
// cannot appear in a borrowed ID, so generated and original IDs cannot collide.
const PREFIX: &str = "harness_call_";

struct Binding<'a> {
    canonical: &'a str,
    wire: Cow<'a, str>,
}

pub(crate) struct CallIds<'a>(Vec<Binding<'a>>);

fn ids(request: &ModelRequest, adapter: Adapter) -> impl Iterator<Item = (&str, Option<&str>)> {
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
                Some((call.call_id.as_str(), native))
            }
            ModelContentPart::ToolResult { call_id, .. } => Some((call_id.as_str(), None)),
            _ => None,
        })
}

fn unchanged(id: &str) -> bool {
    !id.starts_with(PREFIX) && crate::protocol::name::CALL_ID.accepts(id)
}

impl<'a> CallIds<'a> {
    pub(crate) fn new(request: &'a ModelRequest, adapter: Adapter) -> Self {
        let mut bindings = Vec::with_capacity(ids(request, adapter).count());
        bindings.extend(ids(request, adapter).map(|(canonical, native)| {
            let candidate = native.filter(|id| unchanged(id)).unwrap_or(canonical);
            Binding {
                canonical,
                wire: Cow::Borrowed(if unchanged(candidate) { candidate } else { "" }),
            }
        }));
        // Calls carry the original binding; results only carry the canonical ID.
        // Prefer a valid original binding, independent of traversal order.
        bindings.sort_unstable_by(|a, b| {
            let a_native = !a.wire.is_empty() && a.wire != a.canonical;
            let b_native = !b.wire.is_empty() && b.wire != b.canonical;
            a.canonical
                .cmp(b.canonical)
                .then_with(|| b_native.cmp(&a_native))
                .then(a.wire.cmp(&b.wire))
        });
        bindings.dedup_by_key(|binding| binding.canonical);
        bindings.sort_unstable_by(|a, b| a.wire.cmp(&b.wire).then(a.canonical.cmp(b.canonical)));
        let mut start = 0;
        while start < bindings.len() {
            let mut end = start + 1;
            while end < bindings.len() && bindings[start].wire == bindings[end].wire {
                end += 1;
            }
            if bindings[start].wire.is_empty() || end - start > 1 {
                for (offset, binding) in bindings[start..end].iter_mut().enumerate() {
                    binding.wire = Cow::Owned(format!("{PREFIX}{}", start + offset));
                }
            }
            start = end;
        }
        bindings.sort_unstable_by(|a, b| a.canonical.cmp(b.canonical));
        Self(bindings)
    }

    pub(crate) fn wire<'b>(&'b self, id: &str) -> Result<Cow<'b, str>> {
        let index = self
            .0
            .binary_search_by(|binding| binding.canonical.cmp(id))
            .map_err(|_| invalid("missing tool call ID binding"))?;
        Ok(Cow::Borrowed(self.0[index].wire.as_ref()))
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
            for binding in &table.0 {
                let wire = table.wire(binding.canonical).unwrap();
                assert!(!wire.is_empty() && wire.len() <= 40);
                assert!(
                    wire.bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                );
                assert!(
                    reverse
                        .insert(wire.into_owned(), binding.canonical)
                        .is_none(),
                    "distinct canonical exchanges collided"
                );
            }
            for (id, _) in ids(request, adapter) {
                let wire = table.wire(id).unwrap();
                assert_eq!(reverse[wire.as_ref()], id);
            }
        }
    }
}
