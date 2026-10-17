//! Validate exchanges in the SDK's compaction- and edit-aware projection.
use super::*;
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct Message<'a> {
    #[serde(borrow)]
    role: Cow<'a, str>,
    #[serde(default, borrow)]
    content: Option<&'a RawValue>,
    #[serde(rename = "toolCallId", default, borrow)]
    call: Option<Cow<'a, str>>,
    #[serde(rename = "toolName", default, borrow)]
    name: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct Part<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(default, borrow)]
    id: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    name: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Replacement<'a> {
    #[serde(borrow)]
    content: &'a RawValue,
}

fn calls(content: Option<&RawValue>) -> Result<Vec<Part<'_>>> {
    let Some(content) = content else {
        return Ok(Vec::new());
    };
    let bytes = content.get().as_bytes();
    match bytes.first() {
        Some(b'[') => decode(bytes, bytes.len() as u64),
        Some(b'"') => Ok(Vec::new()),
        _ => Err(invalid()),
    }
}

impl SessionFile<'_> {
    pub(super) fn invocation_tool(&self, index: usize, invocation: &str) -> Result<Option<String>> {
        let row = self.rows.get(index).ok_or_else(invalid)?;
        let Some(message) = row.meta.message else {
            return Ok(None);
        };
        let message: Message<'_> = decode(message.get().as_bytes(), row.bytes.len() as u64)?;
        if row.meta.kind != "message" || message.role != "assistant" {
            return Ok(None);
        }
        let mut tool = None;
        for part in calls(message.content)? {
            if part.kind != "toolCall" || part.id.as_deref() != Some(invocation) {
                continue;
            }
            let name = part.name.filter(|name| text(name)).ok_or_else(invalid)?;
            if tool.replace(name.into_owned()).is_some() {
                return Err(invalid());
            }
        }
        Ok(tool)
    }

    pub(super) fn complete_exchanges(&self, branch: &[usize]) -> Result<()> {
        let compact = branch.iter().enumerate().rev().find(|(_, index)| {
            self.rows
                .get(**index)
                .is_some_and(|row| row.meta.kind == "compaction")
        });
        let mut context = Vec::new();
        if let Some((position, index)) = compact {
            let row = self.rows.get(*index).ok_or_else(invalid)?;
            let kept = row.meta.kept.as_deref().ok_or_else(invalid)?;
            let start = branch
                .iter()
                .position(|index| self.rows.get(*index).is_some_and(|row| row.meta.id == kept))
                .ok_or_else(invalid)?;
            if start > position {
                return Err(invalid());
            }
            context.push(*index);
            context.extend_from_slice(branch.get(start..position).ok_or_else(invalid)?);
            context.extend_from_slice(branch.get(position + 1..).ok_or_else(invalid)?);
        } else {
            context.extend_from_slice(branch)
        }
        let mut edits = BTreeMap::new();
        for index in &context {
            let row = self.rows.get(*index).ok_or_else(invalid)?;
            if row.meta.kind != "context_edit" {
                continue;
            }
            let target = self
                .ids
                .get(row.meta.target.as_deref().ok_or_else(invalid)?)
                .copied()
                .ok_or_else(invalid)?;
            if target >= *index || branch.binary_search(&target).is_err() {
                return Err(invalid());
            }
            let target_row = self.rows.get(target).ok_or_else(invalid)?;
            if !matches!(target_row.meta.kind.as_ref(), "message" | "custom_message") {
                return Err(invalid());
            }
            let replacement = row.meta.replacement.ok_or_else(invalid)?;
            if replacement.get() != "null" {
                let value: Replacement<'_> =
                    decode(replacement.get().as_bytes(), row.bytes.len() as u64)?;
                calls(Some(value.content))?;
            }
            edits.insert(target, replacement);
        }
        let mut pending = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for index in context {
            let row = self.rows.get(index).ok_or_else(invalid)?;
            if edits
                .get(&index)
                .is_some_and(|replacement| replacement.get() == "null")
            {
                continue;
            }
            if row.meta.kind == "custom_message" && !pending.is_empty() {
                return Err(invalid());
            }
            if row.meta.kind != "message" {
                continue;
            }
            let raw = row.meta.message.ok_or_else(invalid)?;
            let message: Message<'_> = decode(raw.get().as_bytes(), row.bytes.len() as u64)?;
            let content = match edits.get(&index) {
                Some(replacement) if replacement.get() == "null" => continue,
                Some(replacement) => Some(
                    decode::<Replacement<'_>>(
                        replacement.get().as_bytes(),
                        row.bytes.len() as u64,
                    )?
                    .content,
                ),
                None => message.content,
            };
            // The next conversational message cannot overtake unresolved calls.
            // A later result does not repair an invalid intervening exchange.
            if message.role != "toolResult" && !pending.is_empty() {
                return Err(invalid());
            }
            match message.role.as_ref() {
                "assistant" => {
                    for part in calls(content)? {
                        if part.kind != "toolCall" {
                            continue;
                        }
                        let id = part.id.filter(|id| text(id)).ok_or_else(invalid)?;
                        let name = part.name.filter(|name| text(name)).ok_or_else(invalid)?;
                        if !seen.insert(id.clone()) || pending.insert(id, name).is_some() {
                            return Err(invalid());
                        }
                    }
                }
                "toolResult" => {
                    let id = message
                        .call
                        .as_deref()
                        .filter(|id| text(id))
                        .ok_or_else(invalid)?;
                    let name = message
                        .name
                        .as_deref()
                        .filter(|name| text(name))
                        .ok_or_else(invalid)?;
                    if pending.remove(id).as_deref() != Some(name) {
                        return Err(invalid());
                    }
                }
                _ => {}
            }
        }
        if pending.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}
