//! Independent lifecycle oracle for the canonical gc-agents status file codec.
use acyclic_harness::task_volume::{ChildFile, ChildState, ChildStatus, Component, FileKey, Query};

pub fn encoded(task: [u8; 16], selected: u8, inputs: u64, settled: bool) -> ChildStatus {
    ChildStatus {
        task: acyclic_harness::TaskId::from_bytes(task),
        inputs,
        state: [
            ChildState::Running,
            ChildState::Paused,
            ChildState::Interrupted,
            ChildState::Cancelled,
            ChildState::Archived,
        ][selected as usize % 5],
        settled,
    }
}
pub fn check(status: ChildStatus, broken: bool) {
    let bytes = serde_json::to_vec(&status).unwrap();
    let valid = status.inputs > 0
        && (status.settled || matches!(status.state, ChildState::Running | ChildState::Paused));
    let parsed = ChildStatus::parse(&bytes);
    assert_eq!(
        parsed.is_ok(),
        valid,
        "status codec accepted invalid quiescence"
    );
    if let Ok(parsed) = parsed {
        assert_eq!(parsed, status);
        let expected = !matches!(status.state, ChildState::Archived | ChildState::Cancelled);
        let actual = if broken { true } else { parsed.active() };
        assert_eq!(actual, expected, "archived membership was retained");
        assert_eq!(serde_json::to_vec(&parsed).unwrap(), bytes);
    }
    let reference = Component::new("worker").unwrap();
    for file in [
        ChildFile::Request,
        ChildFile::Inbox(Component::new("mail").unwrap()),
        ChildFile::Control,
        ChildFile::Status,
        ChildFile::Result(1),
    ] {
        let key = FileKey::Child {
            reference: reference.clone(),
            file,
        };
        assert_eq!(FileKey::parse(&key.path()).unwrap(), key);
        assert_eq!(
            Query::ActiveAgents.matches(&key),
            matches!(
                key,
                FileKey::Child {
                    file: ChildFile::Status,
                    ..
                }
            )
        );
    }
}
pub fn exercise(bytes: &[u8]) {
    if let Ok(status) = ChildStatus::parse(bytes) {
        check(status, false);
    }
    if let Some((&selected, rest)) = bytes.split_first() {
        let mut task = [0; 16];
        for (target, byte) in task.iter_mut().zip(rest) {
            *target = *byte;
        }
        check(encoded(task, selected, 1, selected % 2 == 0), false);
    }
}
