use super::*;

fn probes() -> Vec<(Adapter, Value, &'static str)> {
    let reasoning =
        json!({"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"opaque"});
    let answer = json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]});
    let mut probes = vec![
        (
            Adapter::Responses,
            json!({"store":false,"input":[reasoning, {"role":"user","content":"interrupt"}]}),
            "OAR-RSN-02",
        ),
        (
            Adapter::Responses,
            json!({"store":false,"input":[reasoning, reasoning, answer]}),
            "OAR-RSN-02",
        ),
        (
            Adapter::Chat,
            json!({"messages":[{"role":"user","content":[{"type":"text"}]}]}),
            "OAC-SEQ-02",
        ),
        (
            Adapter::Responses,
            json!({"input":[{"type":"message","role":"assistant","content":[{"type":"output_text"}]}]}),
            "OAR-SEQ-03",
        ),
        (
            Adapter::Anthropic,
            json!({"messages":[{"role":"user","content":"question"},{"role":"assistant","content":"answer"}]}),
            "ANT-SEQ-06",
        ),
        (
            Adapter::Gemini,
            json!({"contents":[{"role":"model","parts":[{"text":"answer"}]}]}),
            "GEM-SEQ-05",
        ),
    ];
    for (adapter, body, rule) in [
        (
            Adapter::Chat,
            json!({"messages":[{"role":"user","content":[{"type":"text","text":false}]}]}),
            "OAC-SEQ-02",
        ),
        (
            Adapter::Responses,
            json!({"input":[{"role":"assistant","content":[{"type":"output_text","text":false}]}]}),
            "OAR-SEQ-03",
        ),
    ] {
        probes.push((adapter, body, rule));
    }
    let (_, mut numeric_chat, _) = fixtures()[2].clone();
    numeric_chat["messages"][1]["content"] = json!(123);
    probes.push((Adapter::Chat, numeric_chat, "OAC-SEQ-03"));
    probes.push((
        Adapter::Chat,
        json!({"messages":[{"role":"user","content":[{"type":"file","file":{}}]}]}),
        "OAC-SEQ-02",
    ));
    for part in [
        json!({"type":"input_text"}),
        json!({"type":"output_text","text":"wrong position"}),
    ] {
        let (_, mut body, _) = fixtures()[1].clone();
        body["input"][2]["output"] = json!([part]);
        probes.push((Adapter::Responses, body, "OAR-TOOL-05"));
    }
    let (_, mut chat, _) = fixtures()[2].clone();
    chat["messages"][1]["tool_calls"][0]
        .as_object_mut()
        .unwrap()
        .remove("type");
    probes.push((Adapter::Chat, chat, "OAC-TOOL-03"));
    for role in ["user", "assistant"] {
        let (_, mut anthropic, _) = fixtures()[0].clone();
        anthropic["messages"]
            .as_array_mut()
            .unwrap()
            .insert(2, json!({"role":role,"content":"interruption"}));
        probes.push((Adapter::Anthropic, anthropic, "ANT-TOOL-04"));
    }
    for (adapter, mut body, rule) in [
        (Adapter::Anthropic, fixtures()[0].1.clone(), "ANT-TOOL-07"),
        (Adapter::Responses, fixtures()[1].1.clone(), "OAR-TOOL-04"),
        (Adapter::Chat, fixtures()[2].1.clone(), "OAC-TOOL-01"),
    ] {
        match adapter {
            Adapter::Anthropic => {
                let calls = body["messages"][1]["content"].as_array_mut().unwrap();
                calls.push(calls[1].clone());
            }
            Adapter::Responses => {
                let calls = body["input"].as_array_mut().unwrap();
                calls.insert(2, calls[1].clone());
            }
            Adapter::Chat => {
                let calls = body["messages"][1]["tool_calls"].as_array_mut().unwrap();
                calls.push(calls[0].clone());
            }
            Adapter::Gemini => unreachable!(),
        }
        probes.push((adapter, body, rule));
    }
    probes.push((
        Adapter::Responses,
        json!({"input":"hello","tools":[{"type":"function","name":"bad name"}]}),
        "OAR-TOOL-01",
    ));
    probes
}

fn assert_rule(adapter: Adapter, body: &Value, rule: &'static str, omit: bool) {
    let mut errors = validate_input(adapter, body);
    if omit {
        errors.retain(|error| *error != rule);
    }
    assert!(errors.contains(&rule), "missing {rule} for {adapter:?}");
}

#[test]
fn required_successors_fields_and_portable_boundaries_are_checked() {
    for (adapter, body, rule) in probes() {
        assert_rule(adapter, &body, rule, false);
    }
}

#[test]
fn omitting_each_adjacency_shape_or_boundary_check_fails_the_same_oracle() {
    for (adapter, body, rule) in probes() {
        assert!(
            std::panic::catch_unwind(|| assert_rule(adapter, &body, rule, true)).is_err(),
            "negative control did not detect missing {rule}"
        );
    }
}

#[test]
fn former_shape_shortcuts_fail_the_regressions() {
    for (adapter, body, rule) in probes()
        .into_iter()
        .filter(|(_, _, rule)| matches!(*rule, "OAC-SEQ-03" | "OAC-SEQ-02" | "OAR-TOOL-05"))
    {
        let old_accepts = match rule {
            "OAC-SEQ-03" => body["messages"][1]["tool_calls"].is_array(),
            "OAC-SEQ-02" => {
                let part = &body["messages"][0]["content"][0];
                part["type"] == "text" || part["file"].is_object()
            }
            "OAR-TOOL-05" => body["input"][2]["output"].is_array(),
            _ => unreachable!(),
        };
        assert!(old_accepts, "negative control must admit the bad fixture");
        assert_rule(adapter, &body, rule, false);
    }
}

#[test]
fn only_null_or_absent_chat_content_can_use_the_tool_call_exception() {
    let (_, body, _) = fixtures()[2].clone();
    assert!(validate_input(Adapter::Chat, &body).is_empty());
    let mut absent = body.clone();
    absent["messages"][1]
        .as_object_mut()
        .unwrap()
        .remove("content");
    assert!(validate_input(Adapter::Chat, &absent).is_empty());
    for scalar in [json!(42), json!(true), json!({})] {
        let mut invalid = body.clone();
        invalid["messages"][1]["content"] = scalar;
        assert!(validate_input(Adapter::Chat, &invalid).contains(&"OAC-SEQ-03"));
    }
    let mut without_calls = body;
    without_calls["messages"][1]["tool_calls"] = json!([]);
    assert!(validate_input(Adapter::Chat, &without_calls).contains(&"OAC-SEQ-03"));
}
