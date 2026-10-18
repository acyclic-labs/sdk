use super::*;
use crate::{
    client::CommandResolution,
    task_volume::{Component, FileValue, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

/// A command-session view holding one pending prompt request.
fn pending(prompt: &str) -> (FileQueryView, OutputOwner) {
    futures::executor::block_on(async {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("prompt-idle").unwrap(),
        )
        .await
        .unwrap();
        let session = Component::new(uuid::Uuid::from_u128(9).to_string()).unwrap();
        let key = FileKey::Session {
            session: session.clone(),
            file: TaskFile::Request(
                Component::new(crate::OperationId::from_bytes([1; 16]).to_string()).unwrap(),
            ),
        };
        let file = FileValue::new(
            key,
            serde_json::to_vec(&CommandResolution::Prompt {
                text: prompt.to_owned(),
                source: None,
                mentions: Vec::new(),
            })
            .unwrap(),
        );
        volume
            .append(volume.head().await.unwrap(), &file)
            .await
            .unwrap();
        let query = Query::CommandSession {
            session: session.clone(),
            recent: 4,
        };
        let snapshot = volume.snapshot(&query, 16).await.unwrap();
        (
            FileQueryView::from_snapshot(query, snapshot, 16, 8192).unwrap(),
            OutputOwner::Session(session),
        )
    })
}

fn painted(archive: &Archive) -> Vec<String> {
    archive.rows().iter().map(ToString::to_string).collect()
}

#[test]
fn idle_pending_prompt_projection_allocates_nothing() {
    let prompt = format!("same{}", "\u{301}".repeat(40));
    let (files, owner) = pending(&prompt);
    let mut archive = Archive::default();
    archive.update(Some((&files, owner.clone())));
    assert_eq!(painted(&archive), [format!("You: {prompt}")]);
    for count in [32, 64] {
        let mut changed = usize::MAX;
        let allocation = acyclic_test_support::measure(|| {
            for _ in 0..count {
                changed = changed.min(archive.update(Some((&files, owner.clone()))));
            }
        });
        assert_eq!(
            (allocation.count_total, allocation.bytes_total),
            (0, 0),
            "unchanged prompt projection reuses its rows"
        );
        assert_eq!(changed, archive.rows().len());
    }
    assert_eq!(painted(&archive), [format!("You: {prompt}")]);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    /// Every line of a multi-line prompt is its own sanitized row, whatever the
    /// line ending; controls inside a line stay visible escapes.
    #[test]
    fn multiline_prompts_paint_one_sanitized_row_per_line(
        lines in prop::collection::vec("[a-z日\u{301}\t\u{1b}\u{202e} ]{0,6}", 1..5),
        crlf in any::<bool>(),
    ) {
        let expected: Vec<String> = lines
            .iter()
            .enumerate()
            .map(|(index, line)| {
                format!(
                    "{}{}",
                    if index == 0 { "You: " } else { "     " },
                    line.replace('\u{1b}', "\\u{1b}").replace('\u{202e}', "\\u{202e}")
                )
            })
            .collect();
        // A CR is a line delimiter only immediately before LF. In particular,
        // trailing standalone CRs must remain visible, including after an
        // empty final line. Check every suffix for each generated prompt.
        for suffix in ["", "\r", "\r\r"] {
            let text = format!("{}{suffix}", lines.join(if crlf { "\r\n" } else { "\n" }));
            let (files, owner) = pending(&text);
            let mut archive = Archive::default();
            archive.update(Some((&files, owner)));
            let mut expected = expected.clone();
            expected.last_mut().unwrap().push_str(&suffix.replace('\r', "\\r"));
            prop_assert_eq!(painted(&archive), expected);
        }
    }
}
