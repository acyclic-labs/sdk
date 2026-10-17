//! Projection of the existing command outcome envelope, without another store.
use super::HeadlessOutcome;
use crate::{Error, Result, task_volume::FileValue};
use serde::Deserialize;
use serde_json::value::RawValue;

mod display;

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

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub(super) fn completion(bytes: &[u8]) -> Result<HeadlessOutcome> {
    let outcome: Outcome<'_> =
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    settled(&outcome)
}

pub(super) fn text(file: &FileValue) -> Option<(u64, String)> {
    if !matches!(
        file.key,
        crate::task_volume::FileKey::Task {
            file: crate::task_volume::TaskFile::ExecutionResult(_),
            ..
        }
    ) {
        return None;
    }
    let outcome: Outcome<'_> = serde_json::from_slice(&file.bytes).ok()?;
    if settled(&outcome).ok()? != HeadlessOutcome::Completed {
        return None;
    }
    let ok: serde_json::Value = serde_json::from_str(outcome.ok?.get()).ok()?;
    Some((outcome.sequence, display::display(&ok)?.into_owned()))
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
    text_pending: usize,
    reasoning_pending: usize,
    live_start: Option<crate::task_volume::FileKey>,
    reasoning_start: Option<crate::task_volume::FileKey>,
}

impl Projection {
    pub(super) fn refresh<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &super::CommandSession<P>,
        owner: &crate::wire::Authority,
    ) -> Result<bool> {
        let key = super::task_compat::views::authority_key(owner);
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
            if let Some(text) = value.get("ok").and_then(display::display) {
                self.segments.push(std::sync::Arc::from(text));
            }
            self.after = sequence;
            changed = true;
        }
        Ok(changed)
    }

    pub(super) fn apply(
        &mut self,
        views: &mut super::TaskFileViews,
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
            if self.live_start.is_some()
                || self.reasoning_start.is_some()
                || self.live_len != 0
                || self.reasoning_len != 0
            {
                views.transcripts.remove(&self.owner);
                self.live_start = None;
                self.reasoning_start = None;
                self.live_len = 0;
                self.reasoning_len = 0;
            }
            return 0;
        }
        let live_start = live.and_then(|text| text.file_start.clone());
        let reasoning_start = live.and_then(|text| text.reasoning_start.clone());
        let text_pending = live.map_or(0, |text| text.pending(false));
        let reasoning_pending = live.map_or(0, |text| text.pending(true));
        let text = live.map_or("", |text| text.streaming_text.as_str());
        let live_size = text.len();
        let restart = reset
            || archived
            || self.active != active
            || live_size < self.live_len.saturating_sub(self.text_pending)
            || self.live_start != live_start
            || self.reasoning_start != reasoning_start;
        let offset = if restart {
            0
        } else {
            self.live_len.saturating_sub(self.text_pending)
        };
        let delta = text.get(offset..).unwrap_or_default().to_owned();
        let text = live.map_or("", |text| text.reasoning.as_str());
        let reasoning_size = text.len();
        let reasoning_reset =
            restart || reasoning_size < self.reasoning_len.saturating_sub(self.reasoning_pending);
        let reasoning_offset = if reasoning_reset {
            0
        } else {
            self.reasoning_len.saturating_sub(self.reasoning_pending)
        };
        let reasoning = text.get(reasoning_offset..).unwrap_or_default().to_owned();
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
        if offset == 0
            && !delta.is_empty()
            && self.archive_len != 0
            && (restart || self.live_len == 0)
        {
            transcript.streaming_text.push_str("\n\n");
        }
        if !restart {
            transcript.streaming_text.truncate(
                transcript
                    .streaming_text
                    .len()
                    .saturating_sub(self.text_pending),
            );
        }
        transcript.streaming_text.push_str(&delta);
        if reasoning_reset {
            transcript.reasoning.clear();
        }
        if !reasoning_reset {
            transcript.reasoning.truncate(
                transcript
                    .reasoning
                    .len()
                    .saturating_sub(self.reasoning_pending),
            );
        }
        transcript.reasoning.push_str(&reasoning);
        transcript.text_pending = text_pending;
        transcript.reasoning_pending = reasoning_pending;
        self.text_pending = text_pending;
        self.reasoning_pending = reasoning_pending;
        transcript.streaming_turn = Some(active.map_or_else(
            || crate::OperationId::from_bytes(u128::from(self.after).to_be_bytes()),
            |task| crate::OperationId::from_bytes(task.into_bytes()),
        ));
        self.applied_revision = self.revision;
        self.applied_segments = self.segments.len();
        self.live_len = live_size;
        self.reasoning_len = reasoning_size;
        self.active = active;
        transcript.file_start.clone_from(&live_start);
        self.live_start = live_start;
        transcript.reasoning_start.clone_from(&reasoning_start);
        self.reasoning_start = reasoning_start;
        work
    }
}

#[cfg(test)]
mod tests;
