use super::*;
use crate::task_volume::{
    Change, Channel, Component, FileKey, FileValue, Generation, Query, Snapshot, query_id,
};
use proptest::prelude::*;

const TASK: &str = "00000000-0000-0000-0000-000000000001";
const SESSION: &str = "00000000-0000-0000-0000-000000000002";

fn owner() -> wire::Authority {
    wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: TASK.into(),
    }
}

fn put(views: &mut ClientViews, path: &str, bytes: Vec<u8>, channel: Option<Channel>) {
    let mut cursor = views
        .selections()
        .find(|(_, view)| *view.query() == Query::Root)
        .unwrap()
        .1
        .cursor()
        .clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    views
        .query_change(Change {
            cursor,
            file: Some(FileValue {
                requester: None,
                key: FileKey::parse(path).unwrap(),
                bytes: bytes.into(),
                channel,
                publication: None,
            }),
            files: vec![],
            removed: vec![],
        })
        .unwrap();
}

fn fixture(tools: usize) -> ClientViews {
    let mut views = crate::test_support::empty_client_views();
    for index in 0..tools {
        let base = format!(".tasks/{TASK}/turns/0/tools/call{index:03}");
        for (file, bytes) in [
            (
                "request.json",
                format!(r#"{{"name":"tool{index:03}","arguments":{{}}}}"#),
            ),
            ("stdout/0", format!("output{index:03}")),
            ("result.json", r#"{"success":true,"exit_code":0}"#.into()),
        ] {
            put(
                &mut views,
                &format!("{base}/{file}"),
                bytes.into_bytes(),
                None,
            );
        }
    }
    views
}

fn answer(views: &mut ClientViews, sequence: usize) {
    put(
        views,
        &format!(".tasks/{TASK}/turns/0/chunks/{sequence}"),
        b"x".to_vec(),
        Some(Channel::Text),
    );
}

fn complete(views: &mut ClientViews, text: &str) {
    put(
        views,
        &format!(".tasks/{TASK}/status.json"),
        br#"{"state":"completed"}"#.to_vec(),
        None,
    );
    let query = Query::CommandSession {
        session: Component::new(SESSION).unwrap(),
        recent: 32,
    };
    let mut cursor = views.selections().next().unwrap().1.cursor().clone();
    cursor.query = query_id(&query).unwrap();
    let generation = Generation {
        volume: cursor.volume,
        epoch: cursor.epoch,
        position: cursor.position,
        commit: cursor.commit,
    };
    // The stock command writer returns both task and text in this envelope.
    let result = FileValue::new(
        FileKey::parse(&format!(
            ".sessions/{SESSION}/executions/answer/result.json"
        ))
        .unwrap(),
        serde_json::to_vec(&serde_json::json!({"sequence":1,"ok":{"task":TASK,"text":text}}))
            .unwrap(),
    );
    views
        .query_snapshot(
            query,
            Snapshot {
                files: vec![result],
                cursor,
                generation,
            },
            1024,
            8_388_608,
        )
        .unwrap();
}

// Independent display oracle: no candidate decoder, Markdown or render cache.
fn expected(tools: usize, text: &str, details: bool) -> Vec<String> {
    let mut rows = vec![Line::raw(text.to_owned())];
    for index in 0..tools {
        rows.push(Line::from(vec![
            Span::styled(
                format!("tool{index:03}"),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(" · completed"),
            Span::raw(" (exit 0)"),
        ]));
        if details {
            rows.push(Line::raw(format!("output{index:03}")));
        }
    }
    rows.iter().map(|row| format!("{row:?}")).collect()
}

fn displayed(renderer: &ClientRenderer, hide_tools: bool) -> Vec<String> {
    let screen = renderer.screen();
    screen
        .transcript
        .iter()
        .chain(if hide_tools { &[][..] } else { screen.tools })
        .map(|row| format!("{row:?}"))
        .collect()
}

fn completion(tools: usize, details: bool, broken: bool) {
    let mut views = fixture(tools);
    answer(&mut views, 0);
    let mut live = ClientRenderer {
        tool_details: details,
        ..Default::default()
    };
    live.update(&views, &owner());
    assert_eq!(displayed(&live, false), expected(tools, "x", details));
    complete(&mut views, "x");
    live.update(&views, &owner());
    let mut replay = ClientRenderer {
        tool_details: details,
        ..Default::default()
    };
    replay.update(&views, &owner());
    for renderer in [&live, &replay] {
        assert_eq!(
            displayed(renderer, broken),
            expected(tools, "x", details),
            "the session owns the answer only; tool files remain visible"
        );
    }
}

fn streaming(tools: usize, updates: usize, broken: bool) {
    // Both runs initialize from plain inputs independently. The baseline restores
    // the former two copies; the candidate uses the production followed renderer.
    let [baseline, candidate] = acyclic_test_support::differential::pair::<_, _, usize>(
        || (tools, updates),
        |(tools, updates), old| {
            if old {
                let mut output = Vec::new();
                for sequence in 0..updates {
                    let text = "x".repeat(sequence + 1);
                    let task = expected(tools, &text, true);
                    output.push(task.clone());
                    acyclic_test_support::differential::record(sequence);
                }
                return output;
            }
            let mut views = fixture(tools);
            let mut renderer = ClientRenderer {
                tool_details: true,
                ..Default::default()
            };
            renderer.update(&views, &owner());
            renderer.wrap_tools(80);
            let tail = renderer.screen().tools.as_ptr().addr();
            let wrapped_tail = renderer.wrapped_tools.rows().as_ptr().addr();
            let copies = renderer.task.tool_copies();
            let mut output = Vec::new();
            for sequence in 0..updates {
                answer(&mut views, sequence);
                renderer.update(&views, &owner());
                let mut extra = 0;
                let allocation = acyclic_test_support::measure(|| {
                    renderer.wrap_tools(80);
                    if broken {
                        // Former TaskRows flatten followed by the outer copy.
                        let task = renderer.task.tools().to_vec();
                        let outer = task.clone();
                        extra += task.len() + outer.len();
                        std::hint::black_box(outer);
                    } else {
                        std::hint::black_box(renderer.task.tools());
                    }
                });
                assert_eq!(
                    renderer.task.tool_copies() - copies + extra,
                    0,
                    "answer deltas materialize zero retained tool rows"
                );
                assert_eq!(
                    allocation.count_total, 0,
                    "borrowing the cached tool tail allocates nothing"
                );
                assert_eq!(
                    renderer.screen().tools.as_ptr().addr(),
                    tail,
                    "outer renderer borrows the same tail"
                );
                assert_eq!(renderer.wrapped_tools.rows().as_ptr().addr(), wrapped_tail);
                output.push(displayed(&renderer, false));
                acyclic_test_support::differential::record(sequence);
            }
            output
        },
    );
    assert_eq!(
        baseline, candidate,
        "full styled rows and update observer order"
    );
}

proptest! {
    #![proptest_config(ProptestConfig { rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_0010), ..ProptestConfig::with_cases(24) })]
    #[test]
    fn completion_and_replay_hide_only_the_duplicate_answer(tools in 1usize..8, details in any::<bool>()) {
        completion(tools, details, false);
    }
    #[test]
    fn answer_delta_tool_work_is_independent_of_retained_rows(tools in 1usize..24, updates in 1usize..12) {
        streaming(tools, updates, false);
    }
}

#[test]
fn hide_whole_task_negative_control() {
    completion(2, true, false);
    assert!(std::panic::catch_unwind(|| completion(2, true, true)).is_err());
}

#[test]
fn recopied_tool_tail_negative_control() {
    streaming(32, 4, false);
    assert!(std::panic::catch_unwind(|| streaming(32, 4, true)).is_err());
}
