//! Projection of the existing command outcome envelope, without another store.
use super::HeadlessOutcome;
use crate::{Error, Result, task_volume::FileValue};
use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Deserialize)]
struct Outcome<'a> {
    sequence: u64,
    completion: Option<HeadlessOutcome>,
    #[serde(borrow)]
    ok: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
    #[serde(default)]
    completed: bool,
    #[serde(default)]
    output_omitted: bool,
}
#[derive(Deserialize)]
struct Text {
    #[serde(default)]
    text: Option<String>,
}

fn settled(outcome: &Outcome<'_>) -> Result<HeadlessOutcome> {
    let invalid = || Error::Invalid("invalid command outcome envelope".into());
    if outcome.sequence == 0 {
        return Err(invalid());
    }
    if outcome.completed && outcome.output_omitted {
        return Ok(outcome.completion.unwrap_or(HeadlessOutcome::Indeterminate));
    }
    match (outcome.ok.is_some(), outcome.error) {
        (true, None)
            if outcome
                .completion
                .is_none_or(|state| state == HeadlessOutcome::Completed) =>
        {
            Ok(HeadlessOutcome::Completed)
        }
        (false, Some(error))
            if error.get().starts_with('"')
                && outcome.completion != Some(HeadlessOutcome::Completed) =>
        {
            Ok(outcome.completion.unwrap_or(HeadlessOutcome::Indeterminate))
        }
        _ => Err(invalid()),
    }
}

pub(super) fn completion(bytes: &[u8]) -> Result<HeadlessOutcome> {
    let outcome: Outcome<'_> =
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    settled(&outcome)
}

pub(super) fn text(file: &FileValue) -> Option<(u64, String)> {
    let outcome: Outcome<'_> = serde_json::from_slice(&file.bytes).ok()?;
    if settled(&outcome).ok()? != HeadlessOutcome::Completed {
        return None;
    }
    let text: Text = serde_json::from_str(outcome.ok?.get()).ok()?;
    Some((outcome.sequence, text.text?))
}

/// Disposable incremental fold of the retained session archive.
#[derive(Default)]
pub(super) struct Projection {
    owner: String,
    after: u64,
    cut: Option<(u64, Option<u64>)>,
    segments: Vec<std::sync::Arc<str>>,
    revision: u64,
    applied_revision: u64,
    applied_segments: usize,
    archive_len: usize,
    live_len: usize,
    reasoning_len: usize,
    active: Option<crate::TaskId>,
}

impl Projection {
    pub(super) fn refresh<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &super::CommandSession<P>,
        owner: &crate::wire::Authority,
    ) -> Result<bool> {
        let key = super::views::authority_key(owner);
        let history_after = files
            .state(super::CommandFileRoot::Session, "layout.json")?
            .and_then(|value| value["history_after"].as_u64())
            .unwrap_or(0);
        let cut = (history_after, files.first_output_sequence());
        let rebuild =
            self.owner != key || self.cut != Some(cut) || self.after > files.output_sequence();
        if rebuild {
            self.owner = key;
            self.cut = Some(cut);
            self.after = history_after;
            self.segments.clear();
            self.revision = self.revision.wrapping_add(1);
        }
        let mut changed = rebuild;
        // A BTreeMap range visits each new outcome once. History/retention
        // changes rebuild with the same single forward pass, never nth(back).
        for record in files.output_since(self.after) {
            let (sequence, value) = record?;
            if let Some(text) = value
                .get("ok")
                .and_then(|value| value.get("text"))
                .and_then(serde_json::Value::as_str)
            {
                self.segments.push(std::sync::Arc::from(text));
            }
            self.after = sequence;
            changed = true;
        }
        Ok(changed)
    }

    pub(super) fn apply(
        &mut self,
        views: &mut super::ClientViews,
        active: Option<crate::TaskId>,
    ) -> usize {
        let reset = self.applied_revision != self.revision;
        let archived = self.applied_segments != self.segments.len();
        let live = active.and_then(|task| {
            views
                .transcripts
                .get(&format!("AGGREGATE_KIND_TASK:{task}"))
        });
        if self.after == 0 && live.is_none() {
            return 0;
        }
        let live_size = live.map_or(0, |text| text.streaming_text.len());
        let restart = reset || archived || self.active != active || live_size < self.live_len;
        let offset = if restart { 0 } else { self.live_len };
        // Copy only newly accepted live bytes. The retained archive is shared
        // segments and each segment is appended to the owner projection once.
        let delta = live
            .map(|text| {
                text.streaming_text
                    .get(offset..)
                    .unwrap_or_default()
                    .to_owned()
            })
            .unwrap_or_default();
        let reasoning_size = live.map_or(0, |text| text.reasoning.len());
        let reasoning_reset = restart || reasoning_size < self.reasoning_len;
        let reasoning_offset = if reasoning_reset {
            0
        } else {
            self.reasoning_len
        };
        let reasoning = live
            .map(|text| {
                text.reasoning
                    .get(reasoning_offset..)
                    .unwrap_or_default()
                    .to_owned()
            })
            .unwrap_or_default();
        let mut work = delta.len() + reasoning.len();
        let transcript = views.transcripts.entry(self.owner.clone()).or_default();
        if reset {
            transcript.streaming_text.clear();
            self.archive_len = 0;
            self.applied_segments = 0;
        }
        if restart {
            transcript.streaming_text.truncate(self.archive_len);
        }
        for segment in &self.segments[self.applied_segments..] {
            if !transcript.streaming_text.is_empty() {
                transcript.streaming_text.push_str("\n\n");
            }
            work += segment.len();
            transcript.streaming_text.push_str(segment);
        }
        if archived || reset {
            self.archive_len = transcript.streaming_text.len();
        }
        if offset == 0 && !delta.is_empty() && self.archive_len != 0 {
            transcript.streaming_text.push_str("\n\n");
        }
        transcript.streaming_text.push_str(&delta);
        if reasoning_reset {
            transcript.reasoning.clear();
        }
        transcript.reasoning.push_str(&reasoning);
        transcript.streaming_turn = Some(active.map_or_else(
            || crate::OperationId::from_bytes(u128::from(self.after).to_be_bytes()),
            |task| crate::OperationId::from_bytes(task.into_bytes()),
        ));
        self.applied_revision = self.revision;
        self.applied_segments = self.segments.len();
        self.live_len = live_size;
        self.reasoning_len = reasoning_size;
        self.active = active;
        work
    }
}

#[cfg(test)]
mod tests;
