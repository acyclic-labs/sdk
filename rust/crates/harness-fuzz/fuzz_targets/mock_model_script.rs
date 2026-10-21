#![no_main]
use harness_mock_model::{Engine, Script, bind_result, events};

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(script) = serde_json::from_slice::<Script>(bytes) else {
        return;
    };
    let copy: Script = serde_json::from_slice(&serde_json::to_vec(&script).unwrap()).unwrap();
    assert_eq!(script, copy);
    for case in script.cases.values() {
        for step in &case.steps {
            let _ = bind_result(step, &serde_json::json!({"input":[]}));
            for path in ["responses", "chat/completions", "messages"] {
                for event in events(path, step).unwrap() {
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
