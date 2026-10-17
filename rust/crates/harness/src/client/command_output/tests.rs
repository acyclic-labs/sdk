use super::*;
use proptest::prelude::*;
proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Archive prefixes are copied once, independent of live chunk count.
    // Resetting the projection every frame is the broken negative control.
    #[test]
    fn live_updates_copy_only_the_new_suffix(prefix in "[a-z]{32,256}", count in 8usize..32) {
        let task = crate::TaskId::new();
        let key = format!("AGGREGATE_KIND_TASK:{task}");
        let mut views = super::super::TaskFileViews::default();
        let mut projection = Projection { owner: "owner".into(), after: 1,
            revision: 1, segments: vec![std::sync::Arc::from(prefix.as_str())], ..Default::default() };
        let mut actual = 0;
        let mut broken = 0;
        for index in 0..count {
            views.transcripts.entry(key.clone()).or_default().streaming_text.push_str("abcdefgh");
            actual += projection.apply(&mut views, Some(task));
            let mut reset = Projection { owner: "broken".into(), after: 1,
                revision: 1, segments: vec![std::sync::Arc::from(prefix.as_str())], ..Default::default() };
            broken += reset.apply(&mut views, Some(task));
            prop_assert_eq!(actual, prefix.len() + (index + 1) * 8);
        }
        prop_assert!(broken > actual * 2, "negative control must expose archive and live prefix copying");
        prop_assert_eq!(&views.transcripts["owner"].streaming_text, &format!("{prefix}\n\n{}", "abcdefgh".repeat(count)));
    }
}

#[cfg(all(feature = "tui", feature = "terminal"))]
fn check_partition(text: &str, cuts: &[bool], prefix: &str) {
    use crate::{
        client::{TaskFileViews, task_projection},
        task_volume::{Channel, Component, FileKey, TaskFile},
        tui::ClientRenderer,
        wire,
    };
    let task = crate::TaskId::from_bytes([42; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "composed".into(),
    };
    let mut views = TaskFileViews::default();
    let mut projection = Projection {
        owner: "AGGREGATE_KIND_TASK:composed".into(),
        after: u64::from(!prefix.is_empty()),
        revision: 1,
        segments: if prefix.is_empty() {
            Vec::new()
        } else {
            vec![std::sync::Arc::from(prefix)]
        },
        ..Default::default()
    };
    let mut renderer = ClientRenderer::default();
    let mut fold_work = 0;
    let mut compose_work = 0;
    let mut chunks = 0;
    let mut start = 0;
    for end in 1..=text.len() {
        if end != text.len() && !cuts.get(end - 1).copied().unwrap_or(false) {
            continue;
        }
        let file = FileValue {
            requester: None,
            publication: None,
            key: FileKey::Task {
                task: Component::new(task.to_string()).unwrap(),
                file: TaskFile::Chunk {
                    turn: 0,
                    sequence: chunks,
                },
            },
            channel: Some(Channel::Text),
            bytes: text.as_bytes()[start..end].to_vec().into(),
        };
        fold_work += task_projection::append(&mut views, &file);
        compose_work += projection.apply(&mut views, Some(task));
        renderer.update(&views, &owner);
        let composed = &views.transcripts["AGGREGATE_KIND_TASK:composed"];
        let head = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}\n\n")
        };
        assert_eq!(
            composed.streaming_text,
            format!("{head}{}", String::from_utf8_lossy(&text.as_bytes()[..end]))
        );
        start = end;
        chunks += 1;
    }
    let mut unsplit = TaskFileViews::default();
    let transcript = unsplit
        .transcripts
        .entry("AGGREGATE_KIND_TASK:composed".into())
        .or_default();
    transcript.streaming_text = if prefix.is_empty() {
        text.into()
    } else {
        format!("{prefix}\n\n{text}")
    };
    transcript.streaming_turn = views.transcripts["AGGREGATE_KIND_TASK:composed"].streaming_turn;
    let mut expected = ClientRenderer::default();
    expected.update(&unsplit, &owner);
    assert_eq!(renderer.screen().transcript, expected.screen().transcript);
    assert!(fold_work <= text.len() + 3 * chunks as usize);
    assert!(compose_work <= prefix.len() + text.len() + 6 * chunks as usize);
    assert_eq!(
        renderer.text_work(),
        prefix.len() + usize::from(!prefix.is_empty()) * 2 + text.len(),
        "stable Markdown bytes must be fed exactly once"
    );
}

#[cfg(all(feature = "tui", feature = "terminal"))]
#[test]
fn every_partition_repairs_same_length_utf8_in_composition_and_rendering() {
    // The old length-only cursor cannot distinguish these equal-size strings.
    let provisional = String::from_utf8_lossy(&[0xe6]).into_owned();
    let repaired = "日";
    let broken = format!("{provisional}{}", &repaired[provisional.len()..]);
    assert_ne!(
        broken, repaired,
        "length-only negative control must miss the repair"
    );
    let text = "日a€";
    for mask in 0..(1usize << (text.len() - 1)) {
        let cuts: Vec<_> = (0..text.len() - 1)
            .map(|bit| mask & (1 << bit) != 0)
            .collect();
        check_partition(text, &cuts, "");
        check_partition(text, &cuts, "retained archive");
    }
}

#[cfg(all(feature = "tui", feature = "terminal"))]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Any UTF-8 byte partition preserves composed text, Markdown styling and
    // linear work, including repairs shorter than the provisional replacement.
    #[test]
    fn utf8_byte_partitions_preserve_rendered_text_and_linear_work(
        text in "[a-z日é€🙂\\n*# ]{1,40}", cuts in prop::collection::vec(any::<bool>(), 0..200)
    ) { check_partition(&text, &cuts, ""); check_partition(&text, &cuts, "retained archive"); }
}

#[cfg(all(feature = "tui", feature = "terminal"))]
#[test]
fn archive_row_loses_provisional_glyph_when_utf8_completes() {
    let seed = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../harness-fuzz/corpus/ui_prompt/utf8-archive-row-repair"
    ));
    check_partition(seed, &[false, true, false], "retained archive");
}
