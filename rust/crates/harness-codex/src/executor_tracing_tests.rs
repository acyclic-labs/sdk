//! Synchronous hook tracing contract, including subscriber lifecycle and unwind.
#![allow(clippy::unwrap_used, clippy::panic, reason = "test assertions")]

use super::{CodexObserver, notify_observer};
use crate::events::{CodexEvent, parse_line};
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::{Context, SubscriberExt as _};

#[derive(Default, Debug)]
struct Seen {
    fields: Vec<(String, String)>,
    lifecycle: Vec<&'static str>,
}

struct Capture(Arc<Mutex<Seen>>);
struct Fields<'a>(&'a mut Seen);

impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .fields
            .push((field.name().into(), format!("{value:?}")));
    }
}

impl<S> tracing_subscriber::Layer<S> for Capture
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: Context<'_, S>,
    ) {
        assert_eq!(attrs.metadata().name(), "acyclic.harness.codex.hook");
        let mut seen = self.0.lock().unwrap();
        seen.lifecycle.push("new");
        attrs.record(&mut Fields(&mut seen));
    }

    fn on_enter(&self, _: &tracing::span::Id, _: Context<'_, S>) {
        self.0.lock().unwrap().lifecycle.push("enter");
    }

    fn on_exit(&self, _: &tracing::span::Id, _: Context<'_, S>) {
        self.0.lock().unwrap().lifecycle.push("exit");
    }

    fn on_close(&self, _: tracing::span::Id, _: Context<'_, S>) {
        self.0.lock().unwrap().lifecycle.push("close");
    }
}

struct Observer {
    seen: Arc<Mutex<Seen>>,
    events: Mutex<Vec<CodexEvent>>,
    panic: bool,
}

impl CodexObserver for Observer {
    fn event(&self, event: &CodexEvent) {
        assert_eq!(
            tracing::Span::current().metadata().unwrap().name(),
            "acyclic.harness.codex.hook"
        );
        self.events.lock().unwrap().push(event.clone());
        self.seen.lock().unwrap().lifecycle.push("callback");
        assert!(!self.panic, "observer panic sentinel");
    }
}

#[test]
fn hooks_classify_all_events_without_payload_fields_and_close_after_callback() {
    // Hostile values appear in known bodies, IDs, and the unknown type itself.
    // Only the fixed classification is allowed into tracing fields.
    let cases = [
        (
            r#"{"type":"thread.started","thread_id":"secret/path"}"#,
            "thread.started",
        ),
        (
            r#"{"type":"turn.started","secret":"payload"}"#,
            "turn.started",
        ),
        (
            r#"{"type":"turn.completed","usage":{"input_tokens":18446744073709551615}}"#,
            "turn.completed",
        ),
        (
            r#"{"type":"turn.failed","error":{"message":"secret/key"}}"#,
            "turn.failed",
        ),
        (
            r#"{"type":"item.started","item":{"id":"secret/path","type":"secret.kind","content":"secret"}}"#,
            "item.started",
        ),
        (
            r#"{"type":"item.updated","item":{"id":"secret/path","type":"reasoning","text":"secret"}}"#,
            "item.updated",
        ),
        (
            r#"{"type":"item.completed","item":{"id":"secret/path","type":"agent_message","text":"secret"}}"#,
            "item.completed",
        ),
        (r#"{"type":"error","message":"secret/key"}"#, "error"),
        (r#"{"type":"secret.type/path","body":"secret"}"#, "other"),
    ];
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
    let seen = Arc::new(Mutex::new(Seen::default()));
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(Capture(Arc::clone(&seen))),
    );
    let observer = Observer {
        seen: Arc::clone(&seen),
        events: Mutex::default(),
        panic: false,
    };
    for (line, kind) in cases {
        let event = parse_line(line).unwrap();
        notify_observer(&observer, &event);
        assert_eq!(observer.events.lock().unwrap().pop(), Some(event));
        assert!(
            tracing::Span::current().is_none(),
            "hook scope escaped callback"
        );
        let mut seen = seen.lock().unwrap();
        assert_eq!(
            seen.lifecycle,
            ["new", "enter", "callback", "exit", "close"]
        );
        assert_eq!(seen.fields, [("event.kind".into(), format!("{kind:?}"))]);
        seen.lifecycle.clear();
        seen.fields.clear();
    }
}

#[test]
fn panicking_hooks_exit_and_close_without_swallowing_the_panic() {
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
    let seen = Arc::new(Mutex::new(Seen::default()));
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(Capture(Arc::clone(&seen))),
    );
    let observer = Observer {
        seen: Arc::clone(&seen),
        events: Mutex::default(),
        panic: true,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        notify_observer(&observer, &CodexEvent::TurnStarted);
    }));
    assert!(result.is_err());
    assert!(tracing::Span::current().is_none());
    assert_eq!(
        seen.lock().unwrap().lifecycle,
        ["new", "enter", "callback", "exit", "close"]
    );
}

#[test]
fn disabled_tracing_still_delivers_the_event() {
    struct Untraced(Mutex<Vec<CodexEvent>>);
    impl CodexObserver for Untraced {
        fn event(&self, event: &CodexEvent) {
            assert!(tracing::Span::current().is_none());
            self.0.lock().unwrap().push(event.clone());
        }
    }
    let observer = Untraced(Mutex::default());
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        notify_observer(&observer, &CodexEvent::TurnStarted);
    });
    assert_eq!(*observer.0.lock().unwrap(), [CodexEvent::TurnStarted]);
}
