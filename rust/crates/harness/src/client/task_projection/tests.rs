use super::*;
use proptest::prelude::*;
fn chunk(task: &str, sequence: u64, bytes: &[u8]) -> FileValue {
    FileValue {
        requester: None,
        publication: None,
        key: FileKey::Task {
            task: crate::task_volume::Component::new(task).unwrap(),
            file: TaskFile::Chunk { turn: 0, sequence },
        },
        channel: Some(Channel::Text),
        bytes: bytes.to_vec().into(),
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Real fold work is linear in new segment bytes, independent of retained
    // tasks. Removing the cursor (reopening every prefix) violates the bound.
    #[test]
    fn appended_segments_have_linear_work_and_completed_tasks_are_frozen(
        pieces in prop::collection::vec("[a-z]{8,16}", 8..32),
    ) {
        let mut views = TaskFileViews::default();
        let mut work = 0;
        let mut broken = 0;
        let mut bytes = 0;
        for (index, text) in pieces.iter().enumerate() {
            let file = chunk("active", index as u64, text.as_bytes());
            work += append(&mut views, &file);
            bytes += text.len();
            prop_assert_eq!(work, bytes);
            prop_assert_eq!(append(&mut views, &file), 0, "cursor must reject replay");
            let mut reopened = TaskFileViews::default();
            for (sequence, prior) in pieces[..=index].iter().enumerate() {
                broken += append(&mut reopened, &chunk("active", sequence as u64, prior.as_bytes()));
            }
        }
        prop_assert!(broken > bytes * 2, "negative control must exceed linear work bound");
        let completed = FileValue::new(FileKey::Task {
            task: crate::task_volume::Component::new("active").unwrap(), file: TaskFile::Completion(0),
        }, b"{{}}".to_vec());
        append(&mut views, &completed);
        let frozen = views.transcripts["AGGREGATE_KIND_TASK:active"].clone();
        prop_assert_eq!(append(&mut views, &chunk("active", pieces.len() as u64, b"late")), 0);
        for index in 0..pieces.len() {
            append(&mut views, &chunk("another", index as u64, b"new"));
            prop_assert_eq!(&views.transcripts["AGGREGATE_KIND_TASK:active"], &frozen);
        }
        prop_assert_eq!(views.transcripts["AGGREGATE_KIND_TASK:active"].streaming_text.as_str(), pieces.concat());
    }
}

#[test]
fn tool_channels_keep_utf8_boundaries_independent_of_interleaving() {
    let task = crate::task_volume::Component::new("tool-task").unwrap();
    let call = crate::task_volume::Component::new("call").unwrap();
    let values: Vec<_> = [
        (ToolFile::Stdout(0), vec![0xe6]),
        (ToolFile::Stderr(0), b"error".to_vec()),
        (ToolFile::Stdout(1), vec![0x97, 0xa5]),
    ]
    .into_iter()
    .map(|(file, bytes)| {
        FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::Tool {
                    turn: 0,
                    call: call.clone(),
                    file,
                },
            },
            bytes,
        )
    })
    .collect();
    let mut live = TaskFileViews::default();
    for file in &values {
        append(&mut live, file);
    }
    let mut sorted = values.clone();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    let mut reopened = TaskFileViews::default();
    for file in &sorted {
        append(&mut reopened, file);
    }
    assert_eq!(live.tools, reopened.tools);
    let crate::model::ToolResultContent::Parts { parts } =
        &live.tools["AGGREGATE_KIND_TASK:tool-task"]["call"].output
    else {
        panic!("text parts");
    };
    assert_eq!(
        parts,
        &vec![
            crate::model::ModelDataPart::Text { text: "日".into() },
            crate::model::ModelDataPart::Text {
                text: "error".into()
            }
        ]
    );
}

#[test]
fn older_completion_does_not_commit_the_current_turns_provisional_suffix() {
    let mut first = chunk("attempts", 0, b"old");
    let mut next = chunk("attempts", 1, &[0xe6]);
    if let FileKey::Task {
        file: TaskFile::Chunk { turn, .. },
        ..
    } = &mut next.key
    {
        *turn = 1;
    }
    let completion = FileValue::new(
        FileKey::Task {
            task: crate::task_volume::Component::new("attempts").unwrap(),
            file: TaskFile::Completion(0),
        },
        b"{}".to_vec(),
    );
    for channel in [Channel::Text, Channel::Reasoning] {
        first.channel = Some(channel);
        next.channel = Some(channel);
        let mut live = TaskFileViews::default();
        for file in [&first, &completion, &next] {
            append(&mut live, file);
        }
        let mut reopened = TaskFileViews::default();
        for file in [&first, &next, &completion] {
            append(&mut reopened, file);
        }
        assert_eq!(live.transcripts, reopened.transcripts);
        let transcript = &live.transcripts["AGGREGATE_KIND_TASK:attempts"];
        assert_eq!(transcript.pending(channel == Channel::Reasoning), 3);
    }
}
