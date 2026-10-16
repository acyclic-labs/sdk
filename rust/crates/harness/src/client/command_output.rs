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
    text: String,
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
            self.text.clear();
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
                if !self.text.is_empty() {
                    self.text.push_str("\n\n");
                }
                self.text.push_str(text);
            }
            self.after = sequence;
            changed = true;
        }
        Ok(changed)
    }

    pub(super) fn apply(&self, views: &mut super::ClientViews, active: Option<crate::TaskId>) {
        let live = active.and_then(|task| {
            views
                .transcripts
                .get(&format!("AGGREGATE_KIND_TASK:{task}"))
                .cloned()
        });
        if self.after == 0 && live.is_none() {
            return;
        }
        let transcript = views.transcripts.entry(self.owner.clone()).or_default();
        transcript.streaming_text.clone_from(&self.text);
        transcript.reasoning.clear();
        if let Some(live) = live {
            if !transcript.streaming_text.is_empty() && !live.streaming_text.is_empty() {
                transcript.streaming_text.push_str("\n\n");
            }
            transcript.streaming_text.push_str(&live.streaming_text);
            transcript.reasoning = live.reasoning;
        }
        transcript.streaming_turn = Some(active.map_or_else(
            || crate::OperationId::from_bytes(u128::from(self.after).to_be_bytes()),
            |task| crate::OperationId::from_bytes(task.into_bytes()),
        ));
    }
}
