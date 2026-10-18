use super::*;
use crate::{
    client::ClientViews,
    task_volume::{Change, FileKey, FileValue, TaskFile, ToolFile},
    tui::render::{
        line,
        task::{AnswerOwner, TaskRows},
    },
};
use acyclic_markdown::Markdown;
use proptest::prelude::*;

fn put(views: &mut ClientViews, file: ToolFile, bytes: Vec<u8>) {
    let mut cursor = views.selections().next().unwrap().1.cursor().clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    views
        .query_change(Change {
            cursor,
            file: Some(FileValue::new(
                FileKey::Task {
                    task: Component::new("task").unwrap(),
                    file: TaskFile::Tool {
                        turn: 0,
                        call: Component::new("call").unwrap(),
                        file,
                    },
                },
                bytes,
            )),
            files: Vec::new(),
            removed: Vec::new(),
        })
        .unwrap();
}

fn result_file(bytes: Vec<u8>) -> HeadFile {
    HeadFile {
        key: FileKey::parse(".tasks/task/turns/0/tools/call/result.json").unwrap(),
        length: bytes.len() as u64,
        segments: vec![bytes.into()],
        channel: None,
        publication: None,
        through_sequence: None,
    }
}

fn expected(text: &str, limit: usize) -> Vec<Line<'static>> {
    let mut parser = Markdown::default();
    parser.push(&text.lines().take(limit).collect::<Vec<_>>().join("\n"));
    parser.lines().iter().map(line).collect()
}

fn completed(out: usize, err: usize, chunk: usize, success: bool, duplicate: bool) {
    let stdout = (0..out)
        .map(|n| format!("out{n:02}日\n"))
        .collect::<String>();
    let stderr = (0..err)
        .map(|n| format!("err{n:02}日\n"))
        .collect::<String>();
    let mut views = crate::test_support::empty_client_views();
    put(
        &mut views,
        ToolFile::Request,
        br#"{"name":"bash","arguments":{}}"#.to_vec(),
    );
    for (index, bytes) in stdout.as_bytes().chunks(chunk).enumerate() {
        put(&mut views, ToolFile::Stdout(index as u64), bytes.to_vec());
    }
    for (index, bytes) in stderr.as_bytes().chunks(chunk).enumerate() {
        put(&mut views, ToolFile::Stderr(index as u64), bytes.to_vec());
    }
    let task = Component::new("task").unwrap();
    let mut live = TaskRows::default();
    let project = |views: &ClientViews, renderer: &mut TaskRows, details| {
        renderer.update(
            Some(views.selections().next().unwrap().1.task(&task)),
            details,
            AnswerOwner::Task,
        );
    };
    project(&views, &mut live, true);
    let mut oracle = expected(&stdout, 20);
    oracle.extend(expected(&stderr, 20 - out.min(20)));
    oracle.truncate(20);
    assert_eq!(&live.tools()[1..], oracle);
    let result = serde_json::json!({"success":success,"exit_code":if success {0} else {7},"stdout":stdout,"stderr":stderr});
    put(
        &mut views,
        ToolFile::Result,
        serde_json::to_vec(&result).unwrap(),
    );
    project(&views, &mut live, true);
    let mut actual = live.tools().to_vec();
    if duplicate {
        // Former renderer: echo the result JSON, then append the streamed files.
        actual.insert(1, Line::raw(result.to_string()));
    }
    assert_eq!(
        &actual[1..],
        oracle,
        "one source-line budget across both channels"
    );
    let title = actual[0].to_string();
    assert!(title.contains(if success { "completed" } else { "failed" }));
    assert!(title.contains(if success { "exit 0" } else { "exit 7" }));
    let mut replay = TaskRows::default();
    project(&views, &mut replay, true);
    assert_eq!(live.tools(), replay.tools());
    project(&views, &mut live, false);
    assert_eq!(live.tools().len(), 1);
    project(&views, &mut live, true);
    assert_eq!(live.tools(), replay.tools());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Completing/replaying a tool changes status, never duplicates output or
    // lets escaped JSON newlines bypass the shared stdout/stderr source limit.
    #[test]
    fn result_status_preserves_one_bounded_output(out in 0usize..35, err in 0usize..35, chunk in 1usize..24, success in any::<bool>()) {
        completed(out,err,chunk,success,false);
    }

    // The metadata codec preserves every supported status value exactly.
    #[test]
    fn status_metadata_round_trips(success in proptest::option::of(any::<bool>()), exit_code in proptest::option::of(any::<i32>())) {
        let value = Status {success,exit_code};
        let encoded = serde_json::to_vec(&value).unwrap();
        prop_assert_eq!(serde_json::from_slice::<Status>(&encoded).unwrap(),value);
    }

    // Even metadata rejected by the borrowed decoder cannot expose a second
    // process-output source through the generic JSON fallback.
    #[test]
    fn process_fields_cannot_enter_fallback(text in ".*", stderr in any::<bool>()) {
        let channel = if stderr {"stderr"} else {"stdout"};
        let encoded = serde_json::to_string(&text).unwrap();
        for source in [format!(r#"{{"{channel}":null}}"#), format!(r#"{{"{channel}":null,"{channel}":{encoded}}}"#)] {
            let file = result_file(source.into_bytes());
            prop_assert!(result_rows(Some(&file)).is_empty());
        }
    }

    // A nonstreaming tool keeps its canonical result, through the same source cap.
    #[test]
    fn nonstreaming_text_results_are_bounded(lines in 0usize..40) {
        let text = (0..lines).map(|n|format!("data{n:02}\n")).collect::<String>();
        let bytes=serde_json::to_vec(&text).unwrap();
        let file = result_file(bytes);
        prop_assert_eq!(result_rows(Some(&file)),expected(&text,20));
    }
}

#[test]
fn result_echo_negative_control() {
    completed(21, 7, 3, false, false);
    assert!(std::panic::catch_unwind(|| completed(21, 7, 3, false, true)).is_err());
}

#[test]
fn stream_admission_retires_a_nonstreaming_fallback_before_its_first_byte() {
    let mut views = crate::test_support::empty_client_views();
    put(
        &mut views,
        ToolFile::Request,
        br#"{"name":"read"}"#.to_vec(),
    );
    put(&mut views, ToolFile::Result, br#""result body""#.to_vec());
    let task = Component::new("task").unwrap();
    let mut rows = TaskRows::default();
    let render = |rows: &mut TaskRows, views: &ClientViews| {
        rows.update(
            Some(views.selections().next().unwrap().1.task(&task)),
            true,
            AnswerOwner::Task,
        );
    };
    render(&mut rows, &views);
    assert_eq!(rows.tools().len(), 2);
    put(&mut views, ToolFile::Stdout(0), Vec::new());
    render(&mut rows, &views);
    assert_eq!(rows.tools().len(), 1);
    put(&mut views, ToolFile::Stdout(1), b"streamed body".to_vec());
    render(&mut rows, &views);
    assert_eq!(rows.tools()[1].to_string(), "streamed body");
}
