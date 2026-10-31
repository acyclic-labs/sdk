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
        let stored = if item.provenance().family == target {
            record.get("wire").unwrap()
        } else {
            let key = serde_json::to_value(target).unwrap();
            record
                .get("projections")
                .unwrap()
                .get(key.as_str().unwrap())
                .unwrap()
        };
        inputs.push(serde_json::from_str::<Value>(stored.as_str().unwrap()).unwrap());
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
        ("wire", json!({"text":"embedded tree"})),
        ("wire", json!(" {\"text\":\"trimmed\"} ")),
        ("wire", json!("{\"text\":\"\\ud800\"}")),
        (
            "projections",
            json!({"anthropic":"{\"text\":\"foreign fallback\"}"}),
        ),
        (
            "projections",
            json!({"responses":"{\"text\":\"replaced\"}"}),
        ),
    ] {
        let mut bad = record.clone();
        bad.as_object_mut().unwrap().insert(field.into(), value);
        assert!(serde_json::from_value::<WireItem>(bad).is_err());
    }
    let provenance = serde_json::to_string(source.provenance()).unwrap();
    let prefix = format!(
        "{{\"version\":1,\"provenance\":{provenance},\"kind\":\"message\",\"wire\":\"{{}}\",\"projections\":"
    );
    let ordinary = format!("{prefix}{{\"chat\":\"{{}}\"}}}}");
    let duplicate = format!("{prefix}{{\"chat\":\"{{}}\",\"chat\":\"{{}}\"}}}}");
    assert!(serde_json::from_str::<WireItem>(&ordinary).is_ok());
    assert!(serde_json::from_str::<WireItem>(&duplicate).is_err());
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
        accept: usize,
    }
    impl Write for Closed {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes > self.accept {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed consumer"))
            } else {
                Ok(bytes.len())
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            unreachable!("rendering never flushes")
        }
    }
    let source = item(Family::Responses, ItemKind::Message, "message");
    let mut closed = Closed {
        writes: 0,
        accept: 0,
    };
    assert!(
        matches!(render(&header(Family::Responses), [&source], Tail, &mut closed), Err(RenderError::Write(error)) if error.kind() == io::ErrorKind::BrokenPipe)
    );
    assert_eq!(closed.writes, 1);
    // Fail in the prefix, separator and borrowed instruction serializer. Each
    // failure must leave the original sink error typed and end immediately.
    for accept in 0..6 {
        let mut sink = Closed { writes: 0, accept };
        assert!(
            matches!(render_summary(&header(Family::Responses), [&source], "summarize", &mut sink), Err(RenderError::Write(error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert_eq!(sink.writes, accept + 1);
    }
    // A retrying consumer would violate this same operation-count oracle.
    assert!(closed.write(b"retry").is_err());
    assert_ne!(closed.writes, 1);
}

#[test]
fn interrupted_and_partial_writers_preserve_progress_and_exact_bytes() {
    struct Writer {
        writes: usize,
        fail_at: Option<usize>,
        chunk: usize,
        bytes: Vec<u8>,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.fail_at == Some(self.writes) {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let length = bytes.len().min(self.chunk);
            self.bytes.extend_from_slice(&bytes[..length]);
            Ok(length)
        }
        fn flush(&mut self) -> io::Result<()> {
            unreachable!("rendering never flushes")
        }
    }
    let source = item(Family::Responses, ItemKind::Message, "original bytes");
    let header = header(Family::Responses);
    let ordinary = bytes(&header, std::slice::from_ref(&source));
    let mut summary = Vec::new();
    let instruction = "summarize \"\\\n猫";
    render_summary(&header, [&source], instruction, &mut summary).unwrap();
    for (is_summary, expected) in [(false, ordinary), (true, summary)] {
        let run = |writer: &mut Writer| {
            if is_summary {
                render_summary(&header, [&source], instruction, writer)
            } else {
                render(&header, [&source], Tail, writer)
            }
        };
        let mut partial = Writer {
            writes: 0,
            fail_at: None,
            chunk: 1,
            bytes: Vec::new(),
        };
        run(&mut partial).unwrap();
        assert_eq!(partial.bytes, expected);
        assert_eq!(partial.writes, expected.len());
        // Every failure position, including JSON instruction escaping, is
        // finite. The writer would succeed on the next call: retrying here
        // would silently convert an interrupted request into a successful one.
        for fail_at in 1..=expected.len() {
            let mut interrupted = Writer {
                writes: 0,
                fail_at: Some(fail_at),
                chunk: 1,
                bytes: Vec::new(),
            };
            assert!(
                matches!(run(&mut interrupted), Err(RenderError::Write(error))
                if error.kind() == io::ErrorKind::Interrupted)
            );
            assert_eq!(interrupted.writes, fail_at);
            assert_eq!(interrupted.bytes, expected[..fail_at - 1]);
        }
        let mut stopped = Writer {
            writes: 0,
            fail_at: None,
            chunk: 0,
            bytes: Vec::new(),
        };
        assert!(matches!(run(&mut stopped), Err(RenderError::Write(error))
            if error.kind() == io::ErrorKind::WriteZero));
        assert_eq!(stopped.writes, 1);
    }
    // The actual former primitive retries Interrupted. This control exposes
    // the incorrect success with a finite producer, without a hang or timeout.
    let mut former = Writer {
        writes: 0,
        fail_at: Some(1),
        chunk: usize::MAX,
        bytes: Vec::new(),
    };
    former.write_all(b"original bytes").unwrap();
    assert_eq!(former.writes, 2);
}

proptest! {
    // Generated request inputs and an independent semantic oracle, not captured
    // provider exchanges. Byte-prefix identity is not a cache-hit assertion.
    #[test]
    fn summary_keeps_the_epoch_prefix_and_disables_invocation(
        instruction in ".{1,160}",
        history in prop::collection::vec(".{0,40}", 0..30),
    ) {
        for family in FAMILIES {
            let declaration = |name| match family {
                Family::Anthropic => json!({"name":name,"input_schema":{"type":"object","properties":{}}}),
                Family::Responses => json!({"type":"function","name":name,"parameters":{"type":"object","properties":{}}}),
                Family::Chat => json!({"type":"function","function":{"name":name,"parameters":{"type":"object","properties":{}}}}),
                Family::Gemini => json!({"name":name,"parameters":{"type":"object","properties":{}}}),
            };
            let declarations = json!([declaration("first"), declaration("second")]);
            let tools = if family == Family::Gemini {
                json!([{"functionDeclarations":declarations}])
            } else { declarations };
            let fields = if family == Family::Gemini { json!({"tools":tools}) }
                else { json!({"model":"target-model","tools":tools}) };
            let fields = fields.as_object().unwrap();
            let header = Header::new(family, "producing-provider".into(), [7;32], RENDERER_VERSION, fields).unwrap();
            let items: Vec<_> = history.iter().enumerate().map(|(index, text)| {
                let role = if index % 2 == 0 { "user" } else if family == Family::Gemini { "model" } else { "assistant" };
                let wire = if family == Family::Gemini { json!({"role":role,"parts":[{"text":text}]}) }
                    else { json!({"role":role,"content":text}) };
                WireItem::capture(Provenance {family,provider:"producing-provider".into(),model:"producing-model".into(),endpoint:"https://example.invalid".into()}, ItemKind::Message, raw(wire.to_string()), BTreeMap::new()).unwrap()
            }).collect();
            let ordinary = bytes(&header, &items);
            let prefix = &ordinary[..ordinary.len() - Tail.bytes().len()];
            let mut summary = Vec::new();
            let mut visited = 0;
            render_summary(&header, items.iter().inspect(|_| visited += 1), &instruction, &mut summary).unwrap();
            prop_assert_eq!(visited, items.len());
            prop_assert!(summary.starts_with(prefix));
            let actual: Value = serde_json::from_slice(&summary).unwrap();
            let mut expected: Value = serde_json::from_slice(&ordinary).unwrap();
            let (input, message) = match family {
                Family::Gemini => ("contents", json!({"role":"user","parts":[{"text":instruction}]})),
                Family::Responses => ("input", json!({"role":"user","content":instruction})),
                _ => ("messages", json!({"role":"user","content":instruction})),
            };
            expected[input].as_array_mut().unwrap().push(message);
            match family {
                Family::Gemini => expected["toolConfig"] = json!({"functionCallingConfig":{"mode":"NONE"}}),
                Family::Anthropic => expected["tool_choice"] = json!({"type":"none"}),
                _ => expected["tool_choice"] = json!("none"),
            }
            prop_assert_eq!(actual, expected);
            // Reconstructing the header with reordered declarations is the
            // forbidden alternative: the exact same prefix oracle detects it.
            let mut changed = fields.clone();
            let list = changed.get_mut("tools").unwrap();
            let list = if family == Family::Gemini { &mut list[0]["functionDeclarations"] } else { list };
            list.as_array_mut().unwrap().reverse();
            let changed = Header::new(family, "producing-provider".into(), [7;32], RENDERER_VERSION, &changed).unwrap();
            let mut negative = Vec::new();
            render_summary(&changed, &items, &instruction, &mut negative).unwrap();
            prop_assert!(!negative.starts_with(prefix));
        }
    }
}

#[test]
fn summary_refuses_uncontrolled_tools_before_consuming_items_or_writing() {
    let fields = json!({"tools":[{"google_search":{}}]});
    let server_header = Header::new(
        Family::Gemini,
        "provider".into(),
        [7; 32],
        RENDERER_VERSION,
        fields.as_object().unwrap(),
    )
    .unwrap();
    let mut output = Vec::new();
    let mut visited = 0;
    let source = item(Family::Gemini, ItemKind::Message, "source");
    let selected = std::iter::once(&source).inspect(|_| visited += 1);
    assert!(render_summary(&server_header, selected, "summary", &mut output).is_err());
    assert_eq!(visited, 0);
    assert!(output.is_empty());
    // The exact same header remains valid for ordinary generation.
    render(&server_header, [&source], Tail, &mut output).unwrap();
    assert!(!output.is_empty());
    output.clear();
    assert!(
        render_summary(
            &header(Family::Responses),
            std::iter::empty(),
            "",
            &mut output
        )
        .is_err()
    );
    assert!(output.is_empty());
    for fields in [json!({"tool_choice":"auto"}), json!({"toolConfig":{}})] {
        assert!(
            Header::new(
                Family::Gemini,
                "provider".into(),
                [7; 32],
                RENDERER_VERSION,
                fields.as_object().unwrap()
            )
            .is_err()
        );
    }
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
