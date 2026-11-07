#![no_main]

// The installed binary and this target share admission before workspace lookup.
#[path = "native_input/actions.rs"]
mod actions;
#[path = "native_input/commands.rs"]
mod commands;
#[path = "../../../../plugin/src/input.rs"]
mod input;

// Use the same OS pipe input as the native entry. Dropping the original
// producer closes the write endpoint and completes read_to_end with EOF.
fn read_pipe(bytes: &[u8]) -> input::Result<serde_json::Value> {
    use std::io::Write;
    std::thread::scope(|scope| {
        let (reader, mut writer) = std::io::pipe().expect("create native input pipe");
        let producer = scope.spawn(move || {
            for chunk in bytes.chunks(4096) {
                writer.write_all(chunk)?;
            }
            Ok::<(), std::io::Error>(())
        });
        let decoded = input::read_json(reader);
        if let Err(error) = producer.join().expect("join native input producer") {
            // A refused reader may close before the producer finishes. The
            // original-value oracle still checks whether that refusal is valid.
            assert!(
                decoded.is_err() && error.kind() == std::io::ErrorKind::BrokenPipe,
                "native input producer failed before admission: {error}"
            );
        }
        decoded
    })
}

fn read_failure() {
    use std::io::Read;
    let directory = tempfile::tempdir().expect("create native input failure directory");
    let file = std::fs::File::options()
        .write(true)
        .create_new(true)
        .open(directory.path().join("stdin"))
        .expect("open native write-only input");
    let original = (&file)
        .read(&mut [0_u8; 1])
        .expect_err("write-only input must refuse an OS read");
    let error = input::read_json(file).expect_err("native input read failure must be refused");
    let actual = error
        .downcast_ref::<std::io::Error>()
        .expect("native input read failure must retain its OS error");
    assert_eq!(actual.kind(), original.kind());
    assert_eq!(actual.raw_os_error(), original.raw_os_error());
}

fn check(bytes: &[u8]) {
    let admitted = read_pipe(bytes);
    if bytes.len() > 65_536 {
        assert!(
            admitted.is_err(),
            "native input exceeds its admitted byte bound"
        );
    } else {
        match (admitted, serde_json::from_slice::<serde_json::Value>(bytes)) {
            (Ok(actual), Ok(original)) => assert_eq!(actual, original),
            (Err(_), Err(_)) => {}
            _ => panic!("native input admission changed its original JSON"),
        }
    }
    if let Ok(text) = std::str::from_utf8(bytes)
        && let Ok(task) = input::parse_task(text)
    {
        assert_eq!(task.as_str(), text, "selected task identity changed");
    }
    if let Ok(original) = serde_json::from_slice::<serde_json::Value>(bytes) {
        check_agy_hooks(&original);
    }
}

fn check_agy_hooks(original: &serde_json::Value) {
    use serde_json::{Value, json};
    for workspace in [None, Some("selected native workspace")] {
        for (event, mapped) in [
            ("SessionStart", "SessionStart"),
            ("PreInvocation", "SessionStart"),
            ("PostToolUse", "ToolBoundary"),
            ("Stop", "Stop"),
            ("unknown", ""),
        ] {
            let mut actual = original.clone();
            let result = input::agy_hook(event, &mut actual, workspace);
            if event == "Stop" && original.get("fullyIdle").and_then(Value::as_bool) == Some(false)
            {
                assert_eq!(result.unwrap(), None, "running native work was retired");
                assert_eq!(&actual, original);
                continue;
            }
            let id = original
                .get("conversationId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty());
            let cwd = workspace.or_else(|| {
                let paths = original.get("workspacePaths")?.as_array()?;
                if paths.len() != 1 {
                    return None;
                }
                paths.first()?.as_str().filter(|path| !path.is_empty())
            });
            if !mapped.is_empty()
                && (event != "Stop"
                    || original.get("fullyIdle").and_then(Value::as_bool) == Some(true))
                && let (Some(id), Some(cwd)) = (id, cwd)
            {
                assert_eq!(result.unwrap(), Some(mapped));
                let mut expected = original.clone();
                expected["cwd"] = json!(cwd);
                expected["session_id"] = json!(id);
                assert_eq!(
                    actual, expected,
                    "native identity or unrelated input changed"
                );
            } else {
                assert!(
                    result.is_err(),
                    "ambiguous native identity or retirement admitted"
                );
                assert_eq!(&actual, original, "refusal changed the original input");
            }
        }
    }
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    check(bytes);
    actions::check(bytes);
    commands::check(bytes);
    static BOUNDARIES: std::sync::Once = std::sync::Once::new();
    BOUNDARIES.call_once(|| {
        read_failure();
        // Valid JSON on both sides of the bound distinguishes admission from
        // incidental parse refusal. The original spaces-only example did not.
        for length in [65_535, 65_536, 65_537] {
            let mut encoded = b"null".to_vec();
            encoded.resize(length, b' ');
            check(&encoded);
        }
        check(b"not-json");
    });
    let agent = acyclic_harness::agent::AgentPath::root(
        &acyclic_harness::agent::AgentId::from_bytes([13; 16]),
    );
    assert_eq!(input::parse_task(agent.as_str()).unwrap(), agent);
    let encoded = serde_json::to_vec(&serde_json::json!({
        "input": String::from_utf8_lossy(bytes),
    }))
    .unwrap();
    if encoded.len() <= 65_536 {
        read_pipe(&encoded).expect("an in-bound JSON input must be admitted");
    }
});
