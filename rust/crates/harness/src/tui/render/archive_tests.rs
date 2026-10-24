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
    #![proptest_config(ProptestConfig { rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_000c), ..ProptestConfig::with_cases(32) })]
    /// Every line of a multi-line prompt is its own sanitized row, whatever the
    /// line ending; controls inside a line stay visible escapes.
    #[test]
    fn multiline_prompts_paint_one_sanitized_row_per_line(
        lines in prop::collection::vec("[a-z日\u{301}\t\r\u{1b}\u{202e} ]{0,6}", 1..5),
        crlf in any::<bool>(),
    ) {
        let expected: Vec<String> = lines
            .iter()
            .enumerate()
            .map(|(index, line)| {
                // With LF separators, the last CR before LF belongs to CRLF.
                let line = if !crlf && index + 1 < lines.len() {
                    line.strip_suffix('\r').unwrap_or(line)
                } else {
                    line
                };
                format!(
                    "{}{}",
                    if index == 0 { "You: " } else { "     " },
                    line.replace('\r', "\\r").replace('\u{1b}', "\\u{1b}").replace('\u{202e}', "\\u{202e}")
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

fn evict_outcome(
    retained: &str,
    rebuild: bool,
) -> (usize, acyclic_test_support::AllocationInfo, u128) {
    let volume = futures::executor::block_on(TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("archive-prefix").unwrap(),
    ))
    .unwrap();
    let snapshot = futures::executor::block_on(volume.snapshot(&Query::Root, 16)).unwrap();
    let mut files = FileQueryView::from_snapshot(Query::Root, snapshot, 16, 1_000_000).unwrap();
    let session = Component::new("s").unwrap();
    let owner = OutputOwner::Session(session);
    let mut archive = Archive::default();
    let mut cost = acyclic_test_support::AllocationInfo::default();
    let mut elapsed = 0;
    for (index, text) in ["old", retained, "new"].iter().enumerate() {
        let mut cursor = files.cursor().clone();
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        files
            .apply(crate::task_volume::Change {
                cursor,
                file: Some(FileValue::new(
                    FileKey::parse(&format!(".sessions/s/executions/{index}/result.json")).unwrap(),
                    serde_json::to_vec(
                        &serde_json::json!({"sequence":index + 1,"ok":{"text":text}}),
                    )
                    .unwrap(),
                )),
                files: vec![],
                removed: if index == 2 {
                    vec![FileKey::parse(".sessions/s/executions/0/result.json").unwrap()]
                } else {
                    vec![]
                },
            })
            .unwrap();
        if rebuild {
            archive = Archive::default();
        }
        archive.stream.work = 0;
        let start = std::time::Instant::now();
        cost = acyclic_test_support::measure(|| {
            archive.update(Some((&files, owner.clone())));
        });
        elapsed = start.elapsed().as_micros();
        let mut fresh = Archive::default();
        fresh.update(Some((&files, owner.clone())));
        assert_eq!(archive.rows(), fresh.rows());
    }
    (archive.stream.work, cost, elapsed)
}
#[test]
fn archived_outcome_eviction_preserves_retained_markdown_without_reparse() {
    for body in [
        "retained ".repeat(100),
        "```rust\nlet x=1;".into(),
        "[ref]: /x\n\n[ref]".into(),
        "".into(),
    ] {
        assert_eq!(evict_outcome(&body, false).0, 3);
    }
    assert!(evict_outcome(&"retained ".repeat(100), true).0 > 3);
}
#[test]
#[ignore = "manual archived prefix measurement"]
fn archive_prefix_measurement() {
    for bytes in [1_000, 10_000, 50_000] {
        let (parsed, cost, elapsed) = evict_outcome(&"x".repeat(bytes), false);
        assert_eq!(parsed, 3);
        eprintln!(
            "CLIENT_ARCHIVE_PREFIX os={} retained_bytes={bytes} parsed_bytes={parsed} elapsed_us={elapsed} allocations={} allocated_bytes={}",
            std::env::consts::OS,
            cost.count_total,
            cost.bytes_total
        );
    }
}
