//! Measure a real edit through its journal, checkpoint and native publication.
use super::*;
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    instrument::WithSubscriber,
};
use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};

#[derive(Clone, Default)]
struct Phases(Arc<Mutex<Vec<(String, Duration)>>>);

struct Started {
    label: String,
    at: Instant,
}

fn result(provider: &ScriptedModelProvider, index: usize, call_id: &str) -> serde_json::Value {
    let request = provider.request(index).expect("request after tool");
    request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id: id,
                content: crate::model::ToolResultContent::Json { value },
                ..
            } if id == call_id => Some(value.clone()),
            _ => None,
        })
        .expect("original tool result")
}

fn call(id: &str, name: &str, arguments: serde_json::Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::ToolCall(
            crate::model::ToolCall::new(id.into(), name.into(), arguments),
        )),
        crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

impl Visit for Started {
    fn record_str(&mut self, field: &Field, value: &str) {
        if matches!(field.name(), "phase" | "rpc") {
            self.label.push('/');
            self.label.push_str(value);
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Phases {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let name = attrs.metadata().name();
        if !matches!(
            name,
            "acyclic.harness.checkout.publication" | "acyclic.harness.executor.publication"
        ) {
            return;
        }
        let mut started = Started {
            label: name.into(),
            at: Instant::now(),
        };
        attrs.record(&mut started);
        ctx.span(id)
            .expect("new span")
            .extensions_mut()
            .insert(started);
    }
    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("closing span");
        let mut extensions = span.extensions_mut();
        if let Some(started) = extensions.remove::<Started>() {
            self.0
                .lock()
                .expect("phase measurements")
                .push((started.label, started.at.elapsed()));
        }
    }
}

#[tokio::test]
async fn small_edits_measure_publication_and_settle_within_half_a_second() -> Result<()> {
    let phases = Phases::default();
    let subscriber = tracing_subscriber::registry().with(phases.clone());
    async {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        std::fs::create_dir(directory.path().join(".git")).map_err(super::super::storage)?;
        std::fs::write(directory.path().join("finding.md"), b"line reference: 12\n")
            .map_err(super::super::storage)?;
        let provider = scripted([ScriptedTurn::Text("ready".into())]);
        let editor = crate::tool::files::edit()?;
        let name = editor.definition.name.clone();
        let harness = Arc::new(
            super::open_scripted(
                checkout_config(directory.path(), (model()?, provider.clone()))?
                    .without_default_tools()
                    .tools([editor, crate::tool::files::read()?]),
            )
            .await?,
        );
        let mut observed = ObservedConversation::open(&harness).await?;
        let ready = observed.send("bind", "Prepare the checkout").await?;
        assert!(ready.outcome.is_completed(), "{:?}", ready.outcome);
        for (index, before, after) in [(0, "12", "13"), (1, "13", "14")] {
            let call_id = format!("small-edit-{index}");
            let first = provider.request_count();
            provider.extend([
                call(
                    &call_id,
                    &name,
                    json!({"path": "finding.md", "old_text": before, "new_text": after}),
                ),
                ScriptedTurn::Text("edited".into()),
            ]);
            phases.0.lock().expect("phase measurements").clear();
            let started = Instant::now();
            let entry = observed
                .send(&call_id, "Correct the line reference")
                .await?;
            let elapsed = started.elapsed();
            assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
            let measured = phases.0.lock().expect("phase measurements").clone();
            eprintln!(
                "SMALL_EDIT index={index} turn_us={} phases={measured:?}",
                elapsed.as_micros()
            );
            let published = result(&provider, first + 1, &call_id);
            assert_eq!(published["path"], "finding.md");
            let version = published["version"]
                .as_str()
                .expect("published opaque version");
            assert!(!version.is_empty());
            // Read back the original publication outside the measured interval.
            let verification = provider.request_count();
            provider.extend([
                call(
                    "read-back",
                    "read",
                    json!({"path": "finding.md", "version": version}),
                ),
                ScriptedTurn::Text("verified".into()),
            ]);
            let verified = observed
                .send(&format!("verify-{index}"), "Read the edited version")
                .await?;
            assert!(verified.outcome.is_completed(), "{:?}", verified.outcome);
            assert_eq!(
                result(&provider, verification + 1, "read-back")["text"],
                format!("line reference: {after}\n")
            );
            assert!(
                measured
                    .iter()
                    .any(|(phase, _)| phase.ends_with("/content-stage")),
                "native publication was not observed: {measured:?}"
            );
            assert!(
                measured
                    .iter()
                    .any(|(phase, _)| phase.ends_with("/tool-settlement")),
                "journal settlement was not observed: {measured:?}"
            );
            assert!(
                elapsed < Duration::from_millis(500),
                "small edit took {elapsed:?}: {measured:?}"
            );
        }
        drop(observed);
        harness.disconnect().await
    }
    .with_subscriber(subscriber)
    .await
}
