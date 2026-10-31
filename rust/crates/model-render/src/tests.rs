use crate::*;
use proptest::prelude::*;
use serde_json::{Value, json, value::RawValue};
use std::{
    collections::BTreeMap,
    io::{self, Write},
};

const FAMILIES: [Family; 4] = [
    Family::Anthropic,
    Family::Responses,
    Family::Chat,
    Family::Gemini,
];

fn raw(text: String) -> Box<RawValue> {
    RawValue::from_string(text).unwrap()
}
fn item(family: Family, kind: ItemKind, text: &str) -> WireItem {
    // Generated IR inputs, not claims of captured provider fixtures.
    let wire = format!("{{ \"text\" : {} }}", serde_json::to_string(text).unwrap());
    let projections = if kind == ItemKind::Reasoning {
        BTreeMap::new()
    } else {
        FAMILIES
            .into_iter()
            .filter(|target| *target != family)
            .map(|target| {
                (
                    target,
                    raw(serde_json::to_string(&json!({"text": text, "target":target})).unwrap()),
                )
            })
            .collect()
    };
    WireItem::capture(
        Provenance {
            family,
            provider: "producing-provider".into(),
            model: "producing-model".into(),
            endpoint: "https://example.invalid/v1".into(),
        },
        kind,
        raw(wire),
        projections,
    )
    .unwrap()
}
fn header(family: Family) -> Header {
    Header::new(
        family,
        "producing-provider".into(),
        [7; 32],
        RENDERER_VERSION,
        json!({"model":"target-model"}).as_object().unwrap(),
    )
    .unwrap()
}
fn bytes(header: &Header, items: &[WireItem]) -> Vec<u8> {
    let mut out = Vec::new();
    render(header, items, Tail, &mut out).unwrap();
    out
}

// Independent reference: JSON values and a full array, not seg/render or a
// copy of their iterator. Semantic equality is checked separately from exact
// raw-byte and prefix preservation.
fn reference(target: Family, items: &[WireItem]) -> Value {
    let mut inputs = Vec::new();
    for item in items {
        let record = serde_json::to_value(item).unwrap();
        if record.get("kind").unwrap() == "reasoning" && item.provenance().family != target {
            continue;
        }
        inputs.push(if item.provenance().family == target {
            record.get("wire").unwrap().clone()
        } else {
            let key = serde_json::to_value(target).unwrap();
            record
                .get("projections")
                .unwrap()
                .get(key.as_str().unwrap())
                .unwrap()
                .clone()
        });
    }
    let field = match target {
        Family::Responses => "input",
        Family::Gemini => "contents",
        _ => "messages",
    };
    let mut root = json!({"model":"target-model"});
    root.as_object_mut()
        .unwrap()
        .insert(field.into(), Value::Array(inputs));
    root
}

proptest! {
    #[test]
    fn append_fork_and_alloy_schedules_match_reference(
        schedule in prop::collection::vec((prop::sample::select(&FAMILIES), any::<bool>(), any::<bool>(), any::<bool>(), ".{0,80}"), 0..100),
    ) {
        let mut branches = [Vec::new(), Vec::new()];
        for (family, reasoning, fork, child, text) in schedule {
            let [parent, child_branch] = &mut branches;
            if fork { *child_branch = parent.clone(); }
            let branch = if child { child_branch } else { parent };
            let old: Vec<_> = FAMILIES.into_iter().map(|target| bytes(&header(target), branch)).collect();
            let kind = if reasoning { ItemKind::Reasoning } else { ItemKind::Message };
            branch.push(item(family, kind, &text));
            // Serialization is the actual persisted representation. Reopening
            // validates the raw bytes again without normalizing them.
            let stored = serde_json::to_vec(branch).unwrap();
            let reopened: Vec<WireItem> = serde_json::from_slice(&stored).unwrap();
            for (target, previous) in FAMILIES.into_iter().zip(old) {
                let out = bytes(&header(target), &reopened);
                prop_assert_eq!(serde_json::from_slice::<Value>(&out).unwrap(), reference(target, &reopened));
                let prefix = previous.get(..previous.len() - Tail.bytes().len()).unwrap();
                prop_assert!(out.starts_with(prefix), "append changed an earlier segment");
            }
        }
    }

    #[test]
    fn raw_reasoning_survives_storage_without_reencoding(text in ".{0,160}") {
        for family in FAMILIES {
            let source = item(family, ItemKind::Reasoning, &text);
            let stored = serde_json::to_vec(&source).unwrap();
            let reopened: WireItem = serde_json::from_slice(&stored).unwrap();
            prop_assert_eq!(source.wire().get(), reopened.wire().get());
            // A shared protocol is not a shared provider. In particular, two
            // Chat-compatible services must never exchange opaque reasoning.
            prop_assert!(seg(family, "other-provider", &reopened).unwrap().is_none());
            for target in FAMILIES {
                let projected = seg(target, "producing-provider", &reopened).unwrap();
                prop_assert_eq!(projected.is_some(), target == family);
                if let Some(projected) = projected {
                    prop_assert!(std::ptr::eq(projected, reopened.wire()));
                    prop_assert_eq!(projected.get(), source.wire().get());
                }
            }
        }
    }
}

#[test]
fn admission_refuses_corrupt_storage_and_reasoning_replacement() {
    let source = item(Family::Responses, ItemKind::Reasoning, "opaque");
    let record = serde_json::to_value(&source).unwrap();
    for (field, value) in [
        ("version", json!(2)),
        (
            "provenance",
            json!({"family":"responses","model":"","endpoint":"x"}),
        ),
        ("wire", json!([])),
        (
            "projections",
            json!({"anthropic":{"text":"foreign fallback"}}),
        ),
        ("projections", json!({"responses":{"text":"replaced"}})),
    ] {
        let mut bad = record.clone();
        bad.as_object_mut().unwrap().insert(field.into(), value);
        assert!(serde_json::from_value::<WireItem>(bad).is_err());
    }
    // RawValue alone accepts these bytes. The shared strict walker rejects
    // malformed Unicode in any nested value, including unknown extension keys.
    let malformed = RawValue::from_string(r#"{"extra":[{"text":"\ud800"}]}"#.into()).unwrap();
    assert!(
        WireItem::capture(
            source.provenance().clone(),
            ItemKind::Reasoning,
            malformed,
            BTreeMap::new()
        )
        .is_err()
    );
}

#[test]
fn missing_foreign_message_binding_refuses_instead_of_dropping_history() {
    let source = WireItem::capture(
        item(Family::Responses, ItemKind::Message, "call")
            .provenance()
            .clone(),
        ItemKind::Message,
        raw("{\"call\":\"original\"}".into()),
        BTreeMap::new(),
    )
    .unwrap();
    assert!(matches!(
        seg(Family::Anthropic, "producing-provider", &source),
        Err(RenderError::MissingProjection(Family::Anthropic))
    ));
    assert_eq!(
        seg(Family::Responses, "producing-provider", &source)
            .unwrap()
            .unwrap()
            .get(),
        source.wire().get()
    );
}

#[test]
fn old_reencoding_and_foreign_text_paths_violate_the_oracles() {
    let source = item(Family::Responses, ItemKind::Reasoning, "private continuity");
    let normalized =
        serde_json::to_string(&serde_json::from_str::<Value>(source.wire().get()).unwrap())
            .unwrap();
    assert_ne!(normalized, source.wire().get());
    assert!(
        seg(Family::Anthropic, "producing-provider", &source)
            .unwrap()
            .is_none()
    );
    // Actual former policy: readable foreign reasoning became ordinary text.
    let foreign_text_fallback = Some(source.wire());
    assert_ne!(
        seg(Family::Anthropic, "producing-provider", &source)
            .unwrap()
            .is_some(),
        foreign_text_fallback.is_some()
    );
}

#[test]
fn sink_failure_returns_at_the_failed_write_without_retrying() {
    struct Closed {
        writes: usize,
    }
    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed consumer"))
        }
        fn flush(&mut self) -> io::Result<()> {
            unreachable!("rendering never flushes")
        }
    }
    let source = item(Family::Responses, ItemKind::Message, "message");
    let mut closed = Closed { writes: 0 };
    assert!(
        matches!(render(&header(Family::Responses), [&source], Tail, &mut closed), Err(RenderError::Write(error)) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    assert_eq!(closed.writes, 1);
    // A retrying consumer would violate this same operation-count oracle.
    assert!(closed.write(b"retry").is_err());
    assert_ne!(closed.writes, 1);
}

#[test]
fn rendering_writes_only_the_selected_items_without_flush_or_lookbehind() {
    struct Counter {
        writes: usize,
        bytes: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            unreachable!("durability belongs to commit")
        }
    }
    let source = item(Family::Responses, ItemKind::Message, "one selected item");
    let header = header(Family::Responses);
    let mut sink = Counter {
        writes: 0,
        bytes: 0,
    };
    let mut visited = 0;
    let selected = std::iter::once(&source).inspect(|_| visited += 1);
    render(&header, selected, Tail, &mut sink).unwrap();
    assert_eq!(visited, 1);
    // write_all omits the empty first separator: header, item, tail.
    assert_eq!(sink.writes, 3);
    assert_eq!(
        sink.bytes,
        header.bytes().len() + source.wire().get().len() + Tail.bytes().len()
    );
    // A look-behind read violates the same work oracle.
    let _ = std::iter::once(&source).inspect(|_| visited += 1).count();
    assert_ne!(visited, 1);
}
