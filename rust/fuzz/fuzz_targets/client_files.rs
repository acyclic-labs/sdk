#![no_main]
use acyclic_harness_client::{AgentCommand, AgentReply, PreparedCommand, decode_agent_reply};
use acyclic_harness_core::{command_request::layout_view, conversation::validate_content_path};
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeSet;

// A standalone layout, or an original command, NUL, and its encoded reply.
// JSON encoders escape NUL, so the separator cannot split a protocol string.
fuzz_target!(|data: &[u8]| {
    if let Ok(layout) = serde_json::from_slice::<serde_json::Value>(data) {
        if let Ok(view) = layout_view(&layout) {
            assert_eq!(
                layout.get("version").and_then(serde_json::Value::as_u64),
                Some(1),
                "layout version admitted outside v1"
            );
            assert!(layout.is_object());
            assert_eq!(
                view,
                layout.get("open_view").and_then(serde_json::Value::as_str)
            );
            assert!(
                layout
                    .get("open_view")
                    .is_none_or(|v| v.is_null() || v.is_string())
            );
        }
    }
    let Some(split) = data.iter().position(|byte| *byte == 0) else {
        return;
    };
    let (command, reply) = data.split_at(split);
    let Ok(command) = PreparedCommand::decode_whole(command) else {
        return;
    };
    let encoded = PreparedCommand::new(&command).expect("admitted command encodes");
    assert_eq!(
        serde_json::to_value(&command).unwrap(),
        serde_json::to_value(encoded.decoded().unwrap()).unwrap()
    );
    match &command {
        AgentCommand::ClientFiles { paths, .. } => {
            assert!(!paths.is_empty());
            assert_eq!(paths.len(), paths.iter().collect::<BTreeSet<_>>().len());
            for path in paths {
                assert!(validate_content_path(path, &mut None).is_ok());
            }
        }
        AgentCommand::PublishClientFiles { changes, .. } => {
            assert!(!changes.is_empty());
            assert_eq!(
                changes.len(),
                changes
                    .iter()
                    .map(|c| &c.path)
                    .collect::<BTreeSet<_>>()
                    .len()
            );
            for change in changes {
                assert!(validate_content_path(&change.path, &mut None).is_ok());
                assert!(
                    change
                        .expected
                        .as_ref()
                        .is_none_or(|source| source.path() == change.path)
                );
            }
        }
        _ => return,
    }
    let Ok(Ok(reply)) = decode_agent_reply(&reply[1..], &command) else {
        return;
    };
    match (reply, command) {
        (
            AgentReply::ClientFiles { snapshot },
            AgentCommand::ClientFiles {
                paths,
                after,
                agent,
                session,
            },
        ) => {
            assert_ne!(Some(&snapshot.generation), after.as_ref());
            assert_eq!(snapshot.files.len(), paths.len());
            for path in paths {
                let file = snapshot.files.get(&path).expect("requested path retained");
                if let Some(file) = file {
                    assert_eq!(
                        file.file.path(),
                        path,
                        "reply file path did not match request"
                    );
                    assert_eq!(
                        &acyclic_harness_core::client_files::volume(
                            file.file.volume().provider().clone(),
                            &agent,
                            session
                        )
                        .unwrap(),
                        file.file.volume()
                    );
                    if path == "layout.json" {
                        // Contents are untrusted: the TUI's decoder, not transport,
                        // decides whether a committed file can be rendered.
                        if let Ok(layout) = serde_json::from_str::<serde_json::Value>(&file.text) {
                            if layout_view(&layout).is_ok() {
                                assert_eq!(
                                    layout.get("version").and_then(serde_json::Value::as_u64),
                                    Some(1)
                                );
                            }
                        }
                    }
                }
            }
        }
        (AgentReply::ClientFilesPublished { .. }, AgentCommand::PublishClientFiles { .. }) => {}
        _ => panic!("reply accepted for another command"),
    }
});
