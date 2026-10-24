use super::tail_tests::{TASK, answer, fixture, owner, put};
use super::*;

fn visits<const FULL: bool>(tools: usize) {
    let mut views = fixture(tools);
    answer(&mut views, 0);
    let mut renderer = ClientRenderer::default();
    renderer.update(&views, &owner());
    let before = renderer.task.tool_visits;
    for sequence in 1..=8 {
        answer(&mut views, sequence);
        if FULL {
            renderer.task.cut = 0;
        }
        renderer.update(&views, &owner());
    }
    assert_eq!(
        renderer.task.tool_visits, before,
        "answer appends must not visit unchanged tools"
    );
}

#[test]
fn answer_appends_skip_every_unchanged_tool() {
    for tools in [8, 64, 128] {
        visits::<false>(tools);
    }
}
#[test]
#[should_panic(expected = "answer appends must not visit unchanged tools")]
fn negative_control_refreshing_the_full_tool_index_visits_every_tool() {
    visits::<true>(64);
}

#[test]
fn one_changed_tool_matches_fresh_projection() {
    let mut views = fixture(64);
    let mut renderer = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    renderer.update(&views, &owner());
    let before = renderer.task.tool_visits;
    put(
        &mut views,
        &format!(".tasks/{TASK}/turns/0/tools/call031/stdout/1"),
        b" appended".to_vec(),
        None,
    );
    renderer.update(&views, &owner());
    assert_eq!(renderer.task.tool_visits - before, 1);
    let mut fresh = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    fresh.update(&views, &owner());
    assert_eq!(renderer.screen().tools, fresh.screen().tools);
}

fn bounded_tool_edit<const FULL: bool>(tools: usize) {
    let mut views = fixture(tools);
    let mut renderer = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    renderer.update(&views, &owner());
    renderer.wrapped_tools.reflow(80);
    renderer.wrapped_tools.view(super::super::Top::Tail, 10);
    let before = renderer.task.tool_copies();
    put(
        &mut views,
        &format!(".tasks/{TASK}/turns/0/tools/call000/stdout/1"),
        b" appended".to_vec(),
        None,
    );
    if FULL {
        renderer.task.cut = 0;
    }
    super::super::WrappedLines::take_work();
    renderer.update(&views, &owner());
    renderer.wrapped_tools.view(super::super::Top::Tail, 10);
    assert!(
        renderer.task.tool_copies() - before <= 2,
        "one changed tool must copy only its own rows"
    );
    assert_eq!(
        super::super::WrappedLines::take_work().2,
        0,
        "unchanged tail tools must retain their layouts"
    );
    let mut fresh = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    fresh.update(&views, &owner());
    fresh.wrapped_tools.reflow(80);
    fresh.wrapped_tools.view(super::super::Top::Tail, 10);
    assert_eq!(renderer.screen().tools, fresh.screen().tools);
    assert_eq!(renderer.wrapped_tools.shown(), fresh.wrapped_tools.shown());
}

#[test]
fn changed_tool_retains_every_unchanged_tool_layout() {
    for tools in [8, 64, 128] {
        bounded_tool_edit::<false>(tools);
    }
}

#[test]
#[should_panic(expected = "one changed tool must copy only its own rows")]
fn negative_control_full_tool_refresh_copies_every_tool() {
    bounded_tool_edit::<true>(64);
}

#[test]
fn mixed_tool_insert_remove_and_detail_edits_equal_fresh_layouts() {
    let mut views = fixture(16);
    let mut renderer = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    renderer.update(&views, &owner());
    for step in 0..6 {
        match step {
            0 => {
                for call in ["call004", "call012"] {
                    put(
                        &mut views,
                        &format!(".tasks/{TASK}/turns/0/tools/{call}/stdout/1"),
                        "\n**styled** 界".as_bytes().to_vec(),
                        None,
                    );
                }
            }
            1 => {
                for call in ["aaa", "call004a", "zzz"] {
                    put(
                        &mut views,
                        &format!(".tasks/{TASK}/turns/0/tools/{call}/request.json"),
                        br#"{"name":"new","arguments":{}}"#.to_vec(),
                        None,
                    );
                }
            }
            2 => {
                let selection = views
                    .selections()
                    .find(|(_, files)| *files.query() == crate::task_volume::Query::Root)
                    .unwrap()
                    .1;
                let mut cursor = selection.cursor().clone();
                let removed = selection
                    .files()
                    .keys()
                    .filter(|key| key.path().contains("/call004/") || key.path().contains("/aaa/"))
                    .cloned()
                    .collect();
                cursor.position += 1;
                cursor.accepted = cursor.position;
                cursor.durable = cursor.position;
                views
                    .query_change(crate::task_volume::Change {
                        cursor,
                        file: None,
                        files: vec![],
                        removed,
                    })
                    .unwrap();
            }
            3 => renderer.tool_details = false,
            4 => renderer.tool_details = true,
            _ => put(
                &mut views,
                &format!(".tasks/{TASK}/turns/0/tools/call015/result.json"),
                br#"{"success":false,"exit_code":1}"#.to_vec(),
                None,
            ),
        }
        renderer.update(&views, &owner());
        let mut fresh = ClientRenderer {
            tool_details: renderer.tool_details,
            ..Default::default()
        };
        fresh.update(&views, &owner());
        assert_eq!(renderer.screen().tools, fresh.screen().tools, "step {step}");
        for width in [7, 80] {
            renderer.wrapped_tools.reflow(width);
            fresh.wrapped_tools.reflow(width);
            renderer.wrapped_tools.wrap_all();
            fresh.wrapped_tools.wrap_all();
            assert_eq!(
                renderer.wrapped_tools.rows(),
                fresh.wrapped_tools.rows(),
                "step {step}"
            );
            assert_eq!(
                renderer.wrapped_tools.ranges(),
                fresh.wrapped_tools.ranges()
            );
        }
    }
}

#[test]
#[ignore = "manual changed-tool measurement"]
fn changed_tool_measurement() {
    for tools in [8, 64, 128] {
        let mut views = fixture(tools);
        let mut renderer = ClientRenderer {
            tool_details: true,
            ..Default::default()
        };
        renderer.update(&views, &owner());
        renderer.wrapped_tools.reflow(80);
        renderer.wrapped_tools.view(super::super::Top::Tail, 10);
        let before = renderer.task.tool_copies();
        put(
            &mut views,
            &format!(".tasks/{TASK}/turns/0/tools/call000/stdout/1"),
            b" appended".to_vec(),
            None,
        );
        super::super::WrappedLines::take_work();
        let start = std::time::Instant::now();
        let allocation = acyclic_test_support::measure(|| {
            renderer.update(&views, &owner());
            renderer.wrapped_tools.view(super::super::Top::Tail, 10);
        });
        eprintln!(
            "CLIENT_TOOL os={} tools={tools} elapsed_us={} allocations={} allocated_bytes={} copies={} work={:?}",
            std::env::consts::OS,
            start.elapsed().as_micros(),
            allocation.count_total,
            allocation.bytes_total,
            renderer.task.tool_copies() - before,
            super::super::WrappedLines::take_work()
        );
    }
}

#[test]
fn turn_eviction_keeps_text_reasoning_and_wrapped_caches_equal_to_fresh_projections() {
    use crate::task_volume::{Change, Channel, Component, FileKey, FileValue, TaskFile};
    for retained in [
        "plain ".repeat(100),
        "```rust\nlet x=1;".into(),
        "[ref]: /x\n\n[ref]".into(),
        "".into(),
        "日🙂\n\n**bold**".into(),
    ] {
        let mut views = fixture(0);
        let mut renderer = ClientRenderer::default();
        let mut keys: Vec<Vec<FileKey>> = Vec::new();
        for (turn, body) in ["old", retained.as_str(), "new", "next"]
            .into_iter()
            .enumerate()
        {
            let mut cursor = views.selections().next().unwrap().1.cursor().clone();
            cursor.position += 1;
            cursor.accepted = cursor.position;
            cursor.durable = cursor.position;
            let files: Vec<_> = [Channel::Text, Channel::Reasoning]
                .into_iter()
                .map(|channel| FileValue {
                    requester: None,
                    publication: None,
                    key: FileKey::Task {
                        task: Component::new(TASK).unwrap(),
                        file: TaskFile::TextHead {
                            turn: turn as u64,
                            channel,
                        },
                    },
                    channel: Some(channel),
                    bytes: body.as_bytes().to_vec().into(),
                })
                .collect();
            let current = files.iter().map(|file| file.key.clone()).collect();
            views
                .query_change(Change {
                    cursor,
                    file: None,
                    files,
                    removed: if turn >= 2 {
                        keys[turn - 2].clone()
                    } else {
                        vec![]
                    },
                })
                .unwrap();
            keys.push(current);
            renderer.update(&views, &owner());
            let mut fresh = ClientRenderer::default();
            fresh.update(&views, &owner());
            assert_eq!(renderer.screen().transcript, fresh.screen().transcript);
            assert_eq!(renderer.task.reasoning().1, fresh.task.reasoning().1);
            for width in [5, 80] {
                for (actual, expected) in [
                    (
                        &mut renderer.wrapped_transcript,
                        &mut fresh.wrapped_transcript,
                    ),
                    (
                        &mut renderer.wrapped_reasoning,
                        &mut fresh.wrapped_reasoning,
                    ),
                ] {
                    actual.reflow(width);
                    expected.reflow(width);
                    for line in 0..expected.source_len() {
                        let position = crate::tui::RowPosition { line, row: 0 };
                        actual.view_from(position, 8);
                        expected.view_from(position, 8);
                        assert_eq!(actual.shown(), expected.shown());
                    }
                    assert_eq!(actual.source_len(), expected.source_len());
                }
            }
        }
    }
}

fn evicted_joined_cache<const KEEP_OLD: bool>() {
    use crate::task_volume::{Change, Channel, Component, FileKey, FileValue, TaskFile};
    let mut views = fixture(0);
    let mut renderer = ClientRenderer::default();
    let mut keys: Vec<FileKey> = Vec::new();
    for (turn, body) in ["old", "retained", ""].into_iter().enumerate() {
        let key = FileKey::Task {
            task: Component::new(TASK).unwrap(),
            file: TaskFile::TextHead {
                turn: turn as u64,
                channel: Channel::Text,
            },
        };
        let mut cursor = views.selections().next().unwrap().1.cursor().clone();
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        views
            .query_change(Change {
                cursor,
                file: Some(FileValue {
                    requester: None,
                    publication: None,
                    key: key.clone(),
                    channel: Some(Channel::Text),
                    bytes: body.as_bytes().to_vec().into(),
                }),
                files: vec![],
                removed: if turn == 2 {
                    vec![keys[0].clone()]
                } else {
                    vec![]
                },
            })
            .unwrap();
        keys.push(key);
        let _ = renderer.screen();
        let stale = renderer
            .transcript
            .get()
            .expect("joined cache initialized")
            .clone();
        renderer.update(&views, &owner());
        if KEEP_OLD && turn == 2 {
            renderer.transcript = OnceLock::from(stale);
        }
        let mut fresh = ClientRenderer::default();
        fresh.update(&views, &owner());
        assert_eq!(
            renderer.screen().transcript,
            fresh.screen().transcript,
            "prefix eviction must release joined rows even without new visible text"
        );
    }
}
#[test]
fn empty_turn_after_eviction_releases_the_joined_cache() {
    evicted_joined_cache::<false>();
}
#[test]
#[should_panic(expected = "prefix eviction must release joined rows")]
fn negative_control_retaining_evicted_joined_rows_is_detected() {
    evicted_joined_cache::<true>();
}
