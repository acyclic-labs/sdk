#![no_main]
use harness_mock_model::{Engine, Script, Step, bind_result, events, last_content};

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(body) = serde_json::from_slice::<serde_json::Value>(bytes) {
        let template: Step = serde_json::from_value(serde_json::json!({"json":{"$request":""},"text":["{{request:}}"],"custom_tools":[{"id":"patch","name":"apply_patch","input":"{{request:}}"}]})).unwrap();
        let bound = bind_result(&template, &body).unwrap();
        assert_eq!(bound.json.as_ref(), Some(&body));
        let expected = body
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| body.to_string());
        assert_eq!(bound.text, [expected.clone()]);
        assert_eq!(bound.custom_tools[0].input, expected);
        let _ = last_content(&body, false);
        let result = last_content(&body, true);
        let step: Step = serde_json::from_value(serde_json::json!({"tools":[{"id":"one","name":"edit","arguments":{"file":{"$tool_result":"/file"}}}]})).unwrap();
        let bound = bind_result(&step, &body);
        let file = serde_json::from_str::<serde_json::Value>(&result)
            .ok()
            .and_then(|value| value.get("file").cloned());
        assert_eq!(bound.is_ok(), file.is_some());
        if let (Ok(bound), Some(file)) = (bound, file) {
            assert_eq!(bound.tools[0].arguments["file"], file);
        }
    }
    let Ok(script) = serde_json::from_slice::<Script>(bytes) else {
        return;
    };
    let copy: Script = serde_json::from_slice(&serde_json::to_vec(&script).unwrap()).unwrap();
    assert_eq!(script, copy);
    for case in script.cases.values() {
        for step in &case.steps {
            let _ = bind_result(step, &serde_json::json!({"input":[]}));
            for path in ["responses", "chat/completions", "messages"] {
                for event in events(path, step).into_iter().flatten() {
                    for data in event.lines().filter_map(|line| line.strip_prefix("data: ")) {
                        if data != "[DONE]" {
                            serde_json::from_str::<serde_json::Value>(data).unwrap();
                        }
                    }
                }
            }
        }
    }
    let names: Vec<_> = script.cases.keys().cloned().collect();
    let mut engine = Engine::new(script);
    for name in names {
        let _ = engine.take(&name, "responses", &serde_json::json!({}));
    }
});
