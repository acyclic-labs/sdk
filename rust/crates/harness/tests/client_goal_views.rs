//! Native Rust and exported WASM file-query folds consume identical updates.
#![cfg(target_arch = "wasm32")]

use acyclic_harness::{
    client::{ClientViews, GoalFileView, Limits, WasmClientViews},
    goal::GoalStatus,
    task_volume::{Change, Cursor, FileKey, FileValue, Generation, Query, SCHEMA, Snapshot},
};
use js_sys::{Function, Reflect};
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::wasm_bindgen_test;

fn js(value: &impl Serialize) -> JsValue {
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_maps_as_objects(true)
                .serialize_missing_as_null(true)
                .serialize_large_number_types_as_bigints(true),
        )
        .unwrap()
}

fn same(native: &ClientViews, browser: &WasmClientViews) {
    let actual: serde_json::Value =
        serde_wasm_bindgen::from_value(browser.views().unwrap()).unwrap();
    assert_eq!(actual, serde_json::to_value(native).unwrap());
}

fn cursor(key: &FileKey, position: u64) -> Cursor {
    Cursor {
        volume: if *key == FileKey::Goal {
            [1; 32]
        } else {
            [2; 32]
        },
        epoch: uuid::Uuid::from_bytes([1; 16]),
        schema: SCHEMA,
        query: *blake3::hash(&serde_json::to_vec(&Query::File(key.clone())).unwrap()).as_bytes(),
        position,
        accepted: position,
        durable: position,
        commit: [1; 32],
    }
}

fn snapshot(
    native: &mut ClientViews,
    browser: &mut WasmClientViews,
    key: FileKey,
    position: u64,
    bytes: Option<Vec<u8>>,
) {
    let cursor = cursor(&key, position);
    let snapshot = Snapshot {
        generation: Generation {
            volume: cursor.volume,
            epoch: cursor.epoch,
            position,
            commit: cursor.commit,
        },
        cursor,
        files: bytes
            .map(|bytes| FileValue::new(key.clone(), bytes))
            .into_iter()
            .collect(),
    };
    let query = Query::File(key);
    browser
        .query_snapshot(js(&query), js(&snapshot), 1, 4096)
        .unwrap();
    native.query_snapshot(query, snapshot, 1, 4096).unwrap();
    same(native, browser);
}

fn change(
    native: &mut ClientViews,
    browser: &mut WasmClientViews,
    key: FileKey,
    position: u64,
    bytes: &[u8],
) {
    let change = Change {
        cursor: cursor(&key, position),
        file: Some(FileValue::new(key, bytes.to_vec())),
        files: vec![],
        removed: vec![],
    };
    let input = js(&change);
    let file = Reflect::get(&input, &JsValue::from_str("file")).unwrap();
    Reflect::set(
        &file,
        &JsValue::from_str("bytes"),
        &js_sys::Uint8Array::from(bytes),
    )
    .unwrap();
    let changed = browser.query_change(input).unwrap();
    assert_eq!(changed, native.query_change(change).unwrap());
    same(native, browser);
}

fn head(count: u32) -> Vec<u8> {
    let mut value: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../harness-fuzz/corpus/records/turn")).unwrap();
    value["automatic_turns"] = count.into();
    serde_json::to_vec(&value).unwrap()
}

#[wasm_bindgen_test]
fn native_and_exported_wasm_goal_views_match_on_updates_replay_and_reset() {
    let mut native = ClientViews::default();
    let limits = Limits {
        records: 4,
        branches: 4,
        edges: 8,
        bytes: 65536,
        work: 256,
        retention: 10,
        visible: 4,
    };
    let callback = Function::new_no_args(
        "throw new Error('file queries must not invoke a hypothesis callback')",
    );
    let mut browser = WasmClientViews::new(
        &JsValue::from_str("1"),
        &JsValue::from_str("2"),
        js(&0u64),
        &js(&limits),
        callback.clone(),
        callback.clone(),
        callback,
        None,
    )
    .unwrap();
    let start = (1u64 << 53) + 17;
    snapshot(&mut native, &mut browser, FileKey::Goal, start, None);
    snapshot(&mut native, &mut browser, FileKey::RequestHead, start, None);
    let goal = cursor(&FileKey::Goal, start);
    let request = cursor(&FileKey::RequestHead, start);
    let goal_id = (goal.volume, goal.query);
    let request_id = (request.volume, request.query);
    browser
        .bind_goal_sources(js(&goal_id), js(&request_id))
        .unwrap();
    native.bind_goal_sources(goal_id, request_id).unwrap();
    same(&native, &browser);
    change(
        &mut native,
        &mut browser,
        FileKey::Goal,
        start + 1,
        b"Finish",
    );
    change(
        &mut native,
        &mut browser,
        FileKey::RequestHead,
        start + 1,
        &head(0),
    );
    // Replay cannot increment the retained counter or advance either cursor.
    change(
        &mut native,
        &mut browser,
        FileKey::RequestHead,
        start + 1,
        &head(0),
    );
    change(
        &mut native,
        &mut browser,
        FileKey::RequestHead,
        start + 2,
        &head(3),
    );
    assert_eq!(
        native
            .goal_view(goal.volume, goal.query)
            .unwrap()
            .progress
            .unwrap()
            .continuations,
        3
    );
    change(
        &mut native,
        &mut browser,
        FileKey::Goal,
        start + 2,
        "---\nstatus: blocked\nreason: waiting\n---\n調査".as_bytes(),
    );
    // Independently reset both sources, including a backwards cut on the head.
    snapshot(
        &mut native,
        &mut browser,
        FileKey::RequestHead,
        start,
        Some(head(1)),
    );
    snapshot(
        &mut native,
        &mut browser,
        FileKey::Goal,
        start + 8,
        Some(b"---\nstatus: done\n---\nShipped".to_vec()),
    );
    let view = native.goal_view(goal.volume, goal.query).unwrap();
    assert_eq!(view.progress.unwrap().continuations, 1);
    assert!(matches!(view.goal, GoalFileView::Present { goal } if goal.status == GoalStatus::Done));
    snapshot(
        &mut native,
        &mut browser,
        FileKey::RequestHead,
        start + 1,
        None,
    );
    assert!(
        native
            .goal_view(goal.volume, goal.query)
            .unwrap()
            .progress
            .is_none()
    );
    change(
        &mut native,
        &mut browser,
        FileKey::RequestHead,
        start + 2,
        b"invalid",
    );
    assert!(
        native
            .goal_view(goal.volume, goal.query)
            .unwrap()
            .progress
            .is_none()
    );
    // Negative control: omit a real source update, then deliver it to converge.
    let missing = Change {
        cursor: cursor(&FileKey::RequestHead, start + 3),
        file: Some(FileValue::new(FileKey::RequestHead, head(7))),
        files: vec![],
        removed: vec![],
    };
    let update = js(&missing);
    native.query_change(missing).unwrap();
    let stale: serde_json::Value =
        serde_wasm_bindgen::from_value(browser.views().unwrap()).unwrap();
    assert_ne!(stale, serde_json::to_value(&native).unwrap());
    browser.query_change(update).unwrap();
    same(&native, &browser);
    let before = serde_json::to_value(&native).unwrap();
    let gap = Change {
        cursor: cursor(&FileKey::Goal, start + 99),
        file: Some(FileValue::new(FileKey::Goal, b"Invalid gap".to_vec())),
        files: vec![],
        removed: vec![],
    };
    assert!(browser.query_change(js(&gap)).is_err());
    assert!(native.query_change(gap).is_err());
    assert_eq!(serde_json::to_value(&native).unwrap(), before);
    same(&native, &browser);
}
