use super::*;
use crate::{
    client::{CommandFileRoot, CommandSession},
    task_volume::{Change, Component, FileKey, FileValue, Generation, Query, TaskVolume, query_id},
    tui::{Editor, PromptInput, RowPosition, TranscriptAnchor, TranscriptPart},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

type FixtureResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn archive() -> FixtureResult<ClientViews> {
    let mut views = crate::test_support::empty_client_views();
    let mut cursor = views
        .selections()
        .next()
        .ok_or("missing selection")?
        .1
        .cursor()
        .clone();
    let query = Query::CommandSession {
        session: Component::new("s")?,
        recent: 16,
    };
    cursor.query = query_id(&query)?;
    let generation = Generation {
        volume: cursor.volume,
        epoch: cursor.epoch,
        position: cursor.position,
        commit: cursor.commit,
    };
    let files = [(0, 10), (10, 110)]
        .into_iter()
        .enumerate()
        .map(|(index, (start, end))| -> FixtureResult<FileValue> {
            let text = format!(
                "```text\n{}```\n",
                (start..end)
                    .map(|row| format!("message {row}\n"))
                    .collect::<String>()
            );
            Ok(FileValue::new(
                FileKey::parse(&format!(".sessions/s/executions/{index}/result.json"))?,
                serde_json::to_vec(&serde_json::json!({"sequence":index + 1,"ok":{"text":text}}))?,
            ))
        })
        .collect::<FixtureResult<_>>()?;
    views.query_snapshot(
        query,
        crate::task_volume::Snapshot {
            files,
            cursor,
            generation,
        },
        128,
        65536,
    )?;
    Ok(views)
}

fn remove_first(views: &mut ClientViews) -> FixtureResult<()> {
    let mut cursor = views
        .selections()
        .find(|(_, files)| matches!(files.query(), Query::CommandSession { .. }))
        .ok_or("missing command selection")?
        .1
        .cursor()
        .clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    views.query_change(Change {
        cursor,
        file: None,
        files: vec![],
        removed: vec![FileKey::parse(".sessions/s/executions/0/result.json")?],
    })?;
    Ok(())
}

async fn reading_position<const SOURCE: bool>(kind: u8) -> FixtureResult<()> {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("saved-anchor")?,
        )
        .await?,
    );
    let session = uuid::Uuid::from_u128(400);
    let mut files = CommandSession::open(volume.clone(), session, 65536, 128).await?;
    let mut views = if kind == 0 {
        archive()?
    } else {
        task_views(kind, false)?
    };
    let owner = super::tail_tests::owner();
    let input = PromptInput::new(Editor::new(""));
    let mut renderer = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    renderer.update(&views, &owner);
    renderer.wrapped_transcript.reflow(60);
    renderer.wrapped_reasoning.reflow(60);
    renderer.wrapped_tools.reflow(60);
    renderer.reasoning_visible = true;
    renderer.select_view(&files, &input, 8)?;
    let mut anchor = renderer
        .move_view(
            Some(TranscriptAnchor {
                origin: None,
                part: if kind == 2 {
                    TranscriptPart::Reasoning
                } else if matches!(kind, 4 | 7) {
                    TranscriptPart::Tools
                } else {
                    TranscriptPart::Answer
                },
                position: RowPosition {
                    line: if kind == 6 {
                        160
                    } else if kind == 7 {
                        12
                    } else {
                        50
                    },
                    row: 0,
                },
            }),
            0,
        )
        .ok_or("missing moved anchor")?;
    if !SOURCE {
        anchor.origin = None;
    }
    files
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &serde_json::json!({"transcript_top":anchor}),
        )
        .await?;
    renderer.select_view(&files, &input, 8)?;
    let expected = first_row(&renderer, kind)?;
    assert!(!expected.is_empty());
    if kind == 0 {
        remove_first(&mut views)?;
    } else if matches!(kind, 5 | 6) {
        views = task_views(kind, true)?;
    } else {
        remove_task_prefix(&mut views, kind)?;
    }
    renderer.update(&views, &owner);
    renderer.select_view(&files, &input, 8)?;
    assert_eq!(
        first_row(&renderer, kind)?,
        expected,
        "eviction preserves the saved source row"
    );
    let reopened = CommandSession::open(volume, session, 65536, 128).await?;
    let mut fresh = ClientRenderer {
        tool_details: true,
        ..Default::default()
    };
    fresh.update(&views, &owner);
    fresh.wrapped_transcript.reflow(60);
    fresh.wrapped_reasoning.reflow(60);
    fresh.wrapped_tools.reflow(60);
    fresh.reasoning_visible = true;
    fresh.select_view(&reopened, &input, 8)?;
    assert_eq!(
        first_row(&fresh, kind)?,
        expected,
        "reopening preserves the saved source row"
    );
    Ok(())
}

#[test]
fn saved_reading_position_survives_prefix_eviction_and_reopen() -> FixtureResult<()> {
    futures::executor::block_on(reading_position::<true>(0))
}

#[test]
#[should_panic(expected = "missing field `origin`")]
fn negative_control_bare_retained_window_index_cannot_reopen() {
    let result = futures::executor::block_on(reading_position::<false>(0));
    assert!(result.is_ok(), "{result:?}");
}

fn first_row(renderer: &ClientRenderer, kind: u8) -> FixtureResult<String> {
    let rows = if kind == 2 {
        &renderer.wrapped_reasoning
    } else if matches!(kind, 4 | 7) {
        &renderer.wrapped_tools
    } else {
        &renderer.wrapped_transcript
    };
    Ok(rows
        .shown()
        .first()
        .ok_or("missing visible row")?
        .to_string())
}

fn task_views(kind: u8, after: bool) -> FixtureResult<ClientViews> {
    use super::tail_tests::{TASK, fixture, put};
    use crate::task_volume::{Channel, Snapshot};
    if kind == 4 {
        return Ok(fixture(64));
    }
    if kind == 7 {
        let mut views = fixture(1);
        for (sequence, start, end) in [(0, 0, 5), (8, 5, 105)] {
            put(
                &mut views,
                &format!(".tasks/{TASK}/turns/0/tools/call000/stdout/{sequence}"),
                format!(
                    "```text\n{}```\n",
                    (start..end)
                        .map(|row| format!("message {row}\n"))
                        .collect::<String>()
                )
                .into_bytes(),
                None,
            );
        }
        return Ok(views);
    }
    let mut views = fixture(0);
    let count = if kind == 6 { 3 } else { 2 };
    let mut files = Vec::new();
    for index in 0..count {
        if after && index < if kind == 6 { 2 } else { 1 } {
            continue;
        }
        let (start, end) = if index == 0 { (0, 10) } else { (10, 110) };
        let text = format!(
            "```text\n{}```\n",
            (start..end)
                .map(|row| format!("message {row}\n"))
                .collect::<String>()
        );
        let turn = if kind == 3 || kind >= 5 { 0 } else { index };
        let key = FileKey::parse(&format!(
            ".tasks/{TASK}/turns/{turn}/chunks/{}",
            index * 8 + 3
        ))?;
        let channel = if kind == 2 {
            Channel::Reasoning
        } else {
            Channel::Text
        };
        if kind < 5 {
            put(&mut views, &key.path(), text.into_bytes(), Some(channel));
        } else {
            let mut value = FileValue::new(key, text.into_bytes());
            value.channel = Some(channel);
            files.push(value);
        }
    }
    if kind >= 5 {
        let mut cursor = views
            .selections()
            .next()
            .ok_or("missing selection")?
            .1
            .cursor()
            .clone();
        cursor.position += u64::from(after);
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        let generation = Generation {
            volume: cursor.volume,
            epoch: cursor.epoch,
            position: cursor.position,
            commit: cursor.commit,
        };
        let heads = Snapshot {
            files,
            cursor,
            generation,
        }
        .into_heads()?;
        // Exercise the public remote codec as well as the native head fold.
        let encoded = crate::client::jsonl::snapshot(&Query::Root, &heads)?;
        let (_, heads) = crate::client::jsonl::parse_snapshot(&encoded)?;
        let root = views.layout().root().clone();
        let agent = StreamPath::new(format!("{root}/selected"))?;
        // A remote-only fold starts without the native root selection. Both
        // fixtures retain the same one-agent bound.
        views = ClientViews::new(
            root.clone(),
            crate::client::ClientViewLimits {
                agents: 1,
                selected: 1,
                overview_values: 1024,
                overview_bytes: 8_388_608,
                query: crate::client::QueryViewLimits {
                    files: 1024,
                    segments: 1024,
                    bytes: 8_388_608,
                },
            },
        )?;
        let layout = crate::client::ClientInterests::from_layout(root, [agent.clone()], [], 1)?;
        views.apply(crate::client::SwarmUpdate::Layout(layout))?;
        views.apply(crate::client::SwarmUpdate::Snapshot {
            agent,
            snapshot: heads,
        })?;
    }
    Ok(views)
}

fn remove_task_prefix(views: &mut ClientViews, kind: u8) -> FixtureResult<()> {
    use super::tail_tests::TASK;
    let mut cursor = views
        .selections()
        .next()
        .ok_or("missing selection")?
        .1
        .cursor()
        .clone();
    cursor.position += 1;
    cursor.accepted = cursor.position;
    cursor.durable = cursor.position;
    let removed = if kind == 4 {
        views
            .selections()
            .next()
            .ok_or("missing selection")?
            .1
            .files()
            .keys()
            .filter(|key| (0..8).any(|index| key.path().contains(&format!("/call{index:03}/"))))
            .cloned()
            .collect()
    } else if kind == 7 {
        vec![FileKey::parse(&format!(
            ".tasks/{TASK}/turns/0/tools/call000/stdout/0"
        ))?]
    } else {
        vec![FileKey::parse(&format!(".tasks/{TASK}/turns/0/chunks/3"))?]
    };
    views.query_change(Change {
        cursor,
        removed,
        files: vec![],
        file: None,
    })?;
    Ok(())
}

#[test]
fn saved_answer_and_reasoning_positions_survive_turn_eviction_and_reopen() -> FixtureResult<()> {
    for kind in [1, 2] {
        futures::executor::block_on(reading_position::<true>(kind))?;
    }
    Ok(())
}

#[test]
fn saved_position_survives_partial_turn_chunk_eviction_and_reopen() -> FixtureResult<()> {
    futures::executor::block_on(reading_position::<true>(3))
}

#[test]
fn saved_tool_position_survives_earlier_tool_removal_and_reopen() -> FixtureResult<()> {
    futures::executor::block_on(reading_position::<true>(4))
}

#[test]
fn materialized_and_remote_heads_preserve_chunk_origins_after_eviction() -> FixtureResult<()> {
    for kind in [5, 6] {
        futures::executor::block_on(reading_position::<true>(kind))?;
    }
    Ok(())
}

#[test]
fn saved_tool_output_position_survives_stdout_prefix_eviction_and_reopen() -> FixtureResult<()> {
    futures::executor::block_on(reading_position::<true>(7))
}
