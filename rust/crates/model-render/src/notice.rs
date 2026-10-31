//! Primitive event notices use the same immutable item and segment renderer.
use crate::{Family, ItemKind, Provenance, RenderError, WireItem};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use std::collections::BTreeMap;

/// An appended event with independent actor scheduling policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct Notice {
    /// Original primitive event producer.
    pub source: String,
    /// Open event tag, for example `files.changed` or `child.finished`.
    pub kind: String,
    /// Structured event payload; `paths`, when present, is a changed-path list.
    pub body: Value,
    /// Whether delivery requests a model call.
    pub wake: bool,
    /// Only unsent events with the same key may be collapsed.
    pub coalesce_key: Option<String>,
}

impl Notice {
    /// Checks producer identity and changed-path shape without closing the tag set.
    ///
    /// # Errors
    /// Refuses a missing identity or malformed changed-path list.
    pub fn validate(&self) -> Result<(), RenderError> {
        if self.source.is_empty() || self.kind.is_empty() {
            return Err(RenderError::InvalidItem(
                "notice source and kind are required",
            ));
        }
        if let Some(paths) = self.body.get("paths")
            && !paths
                .as_array()
                .is_some_and(|paths| paths.iter().all(Value::is_string))
        {
            return Err(RenderError::InvalidItem("notice paths must be strings"));
        }
        Ok(())
    }
}

/// A caller-selected unsent batch. It cannot hold or mutate a rendered item.
/// Append is amortized constant work plus payload validation; finishing visits
/// only this batch's keys and changed paths, independent of retained history.
#[derive(Default)]
pub struct PendingNotices(Vec<Notice>);

impl PendingNotices {
    /// Moves an event into the unsent batch.
    ///
    /// # Errors
    /// Invalid events leave the pending batch unchanged.
    pub fn push(&mut self, notice: Notice) -> Result<(), RenderError> {
        notice.validate()?;
        self.0.push(notice);
        Ok(())
    }

    /// Consumes the batch. Latest metadata wins; changed paths form a union.
    /// Surviving notices keep their last publication's order. Waking events
    /// and unkeyed events are never collapsed. No history cursor is advanced.
    pub fn finish(self) -> Vec<Notice> {
        let mut result: Vec<Option<Notice>> = (0..self.0.len()).map(|_| None).collect();
        let mut keyed = Vec::new();
        for (position, notice) in self.0.into_iter().enumerate() {
            if notice.coalesce_key.is_some() && !notice.wake {
                keyed.push((position, notice));
            } else {
                result[position] = Some(notice);
            }
        }
        radix(&mut keyed, |(_, notice)| {
            notice
                .coalesce_key
                .as_deref()
                .unwrap_or_default()
                .as_bytes()
        });
        let mut merged: Option<Group> = None;
        for (position, notice) in keyed {
            if let Some(group) = &mut merged
                && group.latest.coalesce_key == notice.coalesce_key
            {
                group.push(position, notice);
                continue;
            }
            if let Some(group) = merged.take() {
                let (position, notice) = group.finish();
                result[position] = Some(notice);
            }
            merged = Some(Group::new(position, notice));
        }
        if let Some(group) = merged {
            let (position, notice) = group.finish();
            result[position] = Some(notice);
        }
        result.into_iter().flatten().collect()
    }
}

struct Group {
    position: usize,
    latest: Notice,
    paths: Vec<Value>,
    has_paths: bool,
}
impl Group {
    fn new(position: usize, notice: Notice) -> Self {
        let mut group = Self {
            position,
            latest: notice,
            paths: Vec::new(),
            has_paths: false,
        };
        group.take_paths();
        group
    }
    fn take_paths(&mut self) {
        if let Some(body) = self.latest.body.as_object_mut()
            && let Some(Value::Array(paths)) = body.remove("paths")
        {
            self.has_paths = true;
            self.paths.extend(paths);
        }
    }
    fn push(&mut self, position: usize, mut notice: Notice) {
        if position > self.position {
            self.position = position;
            self.latest = notice;
            self.take_paths();
        } else if let Some(body) = notice.body.as_object_mut()
            && let Some(Value::Array(paths)) = body.remove("paths")
        {
            self.has_paths = true;
            self.paths.extend(paths);
        }
    }
    fn finish(mut self) -> (usize, Notice) {
        if self.has_paths
            && let Some(body) = self.latest.body.as_object_mut()
        {
            radix(&mut self.paths, |path| {
                path.as_str().unwrap_or_default().as_bytes()
            });
            self.paths.dedup();
            body.insert("paths".into(), Value::Array(self.paths));
        }
        (self.position, self.latest)
    }
}

// In-place byte radix partition. Each edge has a fixed 257-symbol alphabet
// (end plus every byte); comparisons and work cannot grow with key population.
// Explicit ranges also avoid recursion for arbitrarily long common prefixes.
fn radix<T>(items: &mut [T], key: impl Fn(&T) -> &[u8]) {
    let mut ranges = vec![(0, items.len(), 0)];
    while let Some((start, end, depth)) = ranges.pop() {
        if end - start < 2 {
            continue;
        }
        let symbol = |item: &T| {
            key(item)
                .get(depth)
                .map_or(0, |byte| usize::from(*byte) + 1)
        };
        let mut counts = [0usize; 257];
        for item in &items[start..end] {
            counts[symbol(item)] += 1;
        }
        let mut begins = [start; 257];
        let mut offset = start;
        for bucket in 0..257 {
            begins[bucket] = offset;
            offset += counts[bucket];
        }
        let mut next = begins;
        for bucket in 0..257 {
            let limit = begins[bucket] + counts[bucket];
            while next[bucket] < limit {
                let actual = symbol(&items[next[bucket]]);
                if actual == bucket {
                    next[bucket] += 1;
                } else {
                    items.swap(next[bucket], next[actual]);
                    next[actual] += 1;
                }
            }
            if bucket != 0 && counts[bucket] > 1 {
                ranges.push((begins[bucket], limit, depth + 1));
            }
        }
    }
}

fn escaped(output: &mut String, text: &str) {
    for character in text.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            character => output.push(character),
        }
    }
}

fn projection(family: Family, notices: &[Notice]) -> Result<Box<RawValue>, RenderError> {
    let content = if family == Family::Anthropic {
        let mut content = String::new();
        for notice in notices {
            content.push_str("<notice source=\"");
            escaped(&mut content, &notice.source);
            content.push_str("\" kind=\"");
            escaped(&mut content, &notice.kind);
            content.push_str("\">");
            // A body cannot manufacture a second notice by closing its tag.
            let body = serde_json::to_string(&notice.body).map_err(RenderError::Json)?;
            escaped(&mut content, &body);
            content.push_str("</notice>\n");
        }
        content
    } else {
        serde_json::to_string(notices).map_err(RenderError::Json)?
    };
    let value = match family {
        Family::Responses | Family::Chat => json!({"role":"developer", "content":content}),
        Family::Anthropic => json!({"role":"user", "content":content}),
        Family::Gemini => json!({"role":"user", "parts":[{"text":content}]}),
    };
    RawValue::from_string(serde_json::to_string(&value).map_err(RenderError::Json)?)
        .map_err(RenderError::Json)
}

/// Admits one frozen block from the owner's selected, unsent notice batch.
/// Persist the result at its request position and use [`crate::render`]; later
/// notices cannot mutate any of the returned item's native or foreign bytes.
///
/// # Errors
/// Refuses an empty batch or malformed notices. Encoding happens at admission,
/// never during replay of an already rendered prefix.
pub fn notice_block(target: Provenance, notices: &[Notice]) -> Result<WireItem, RenderError> {
    if notices.is_empty() {
        return Err(RenderError::InvalidItem("notice block must not be empty"));
    }
    notices.iter().try_for_each(Notice::validate)?;
    let mut projections = BTreeMap::new();
    for family in [
        Family::Responses,
        Family::Chat,
        Family::Anthropic,
        Family::Gemini,
    ] {
        if family != target.family {
            projections.insert(family, projection(family, notices)?);
        }
    }
    let native = projection(target.family, notices)?;
    WireItem::capture(target, ItemKind::Notice, native, projections)
}
