use super::{
    TestResult, drivers,
    task_query_case::{Expectations, File, TaskQueryCase},
};
use acyclic_harness::task_volume::Channel;
use futures::FutureExt as _;
use proptest::prelude::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn fixture(name: &str) -> TestResult<Value> {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    Ok(super::scenario::load_value(
        &root,
        &root.join(format!("task-query/{name}.json")),
    )?)
}

#[tokio::test]
async fn task_query_oracles_reject_corrupted_payloads_channels_order_and_heads() -> TestResult {
    for (name, mutations) in [
        (
            "integer-sequence-order-and-channels",
            vec!["text", "channel", "order", "heads", "extra", "missing"],
        ),
        (
            "a-rejected-event-preserves-earlier-individually-accepted-events",
            vec!["files", "errors"],
        ),
        (
            "filtered-history-advances-durable-checkpoint",
            vec!["checkpoint"],
        ),
    ] {
        let baseline = fixture(name)?;
        drivers::run(baseline.clone()).await?;
        for field in mutations {
            let mut case = baseline.clone();
            match field {
                "text" => case["writes"][0]["text"] = json!("corrupted"),
                "channel" => case["writes"][0]["channel"] = json!("Reasoning"),
                "order" => case["expect"]["order"]
                    .as_array_mut()
                    .ok_or("order")?
                    .reverse(),
                "heads" => case["expect"]["heads"][".tasks/t/turns/2/text.md"] = json!("corrupted"),
                "extra" => case["expect"]["changes"]
                    .as_array_mut()
                    .ok_or("changes")?
                    .push(Value::Null),
                "missing" => {
                    case["writes"].as_array_mut().ok_or("writes")?.pop();
                }
                "files" => case["expect"]["files"][".tasks/t/status.json"] = json!("wrong"),
                "errors" => case["expect"]["errors"] = json!([]),
                "checkpoint" => {
                    case["expect"]["changes"]
                        .as_array_mut()
                        .ok_or("changes")?
                        .remove(0);
                }
                _ => return Err("unknown mutation".into()),
            }
            // The driver drains the real accepted cut even when expectations are shorter.
            assert!(
                drivers::run(case).await.is_err(),
                "accepted {name}: {field}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn task_query_schema_rejects_unknown_and_inapplicable_fields() -> TestResult {
    let baseline = fixture("request-admission-and-status")?;
    for (pointer, value) in [
        ("/turns", json!([])),
        ("/expect/events", json!([])),
        ("/writes/0/typo", json!(true)),
        ("/writes/0/channel", json!("unknown")),
        ("/expect/changes/0/extra", json!(true)),
        ("/expect/errors", json!(["unknown"])),
        ("/expect/files", Value::Null),
        ("/expect/heads", json!([])),
    ] {
        let mut case = baseline.clone();
        let (parent, key) = pointer.rsplit_once('/').ok_or("pointer")?;
        case.pointer_mut(parent)
            .ok_or("parent")?
            .as_object_mut()
            .ok_or("object")?
            .insert(key.into(), value);
        assert!(drivers::run(case).await.is_err(), "accepted {pointer}");
    }
    Ok(())
}

fn generated_case(records: Vec<(String, bool, u8)>, chunks: bool, selection: u8) -> TaskQueryCase {
    let mut files = BTreeMap::new();
    let mut heads = BTreeMap::new();
    let mut writes = Vec::new();
    let mut changes = Vec::new();
    let folder = match selection {
        0 => "",
        1 => "plans",
        2 => ".tasks/t/turns/0/chunks",
        _ => ".tasks/other",
    };
    let mut all_files = BTreeMap::new();
    for (index, (text, reasoning, slot)) in records.into_iter().enumerate() {
        let path = if chunks {
            format!(".tasks/t/turns/0/chunks/{index}")
        } else {
            format!("plans/p{slot}.md")
        };
        let channel = chunks.then_some(if reasoning {
            Channel::Reasoning
        } else {
            Channel::Text
        });
        let matches = folder.is_empty() || path.starts_with(&format!("{folder}/"));
        let previous = all_files.insert(path.clone(), text.clone());
        let changed = previous.as_ref() != Some(&text);
        if matches {
            files.insert(path.clone(), text.clone());
            let head = if chunks {
                format!(
                    ".tasks/t/turns/0/{}.md",
                    if reasoning { "reasoning" } else { "text" }
                )
            } else {
                path.clone()
            };
            if chunks {
                heads
                    .entry(head)
                    .or_insert_with(String::new)
                    .push_str(&text);
            } else {
                heads.insert(head, text.clone());
            }
        }
        let file = File {
            path,
            text,
            channel,
        };
        writes.push(file.clone());
        if changed {
            changes.push(matches.then_some(file));
        }
    }
    TaskQueryCase {
        folder: folder.into(),
        writes,
        expect: Expectations {
            files,
            changes,
            heads: chunks.then_some(heads),
            errors: vec![],
            order: None,
        },
    }
}

fn history() -> impl Strategy<Value = Vec<(String, bool, u8)>> {
    let text = prop::collection::vec(any::<char>(), 0..16)
        .prop_map(|characters| characters.into_iter().collect::<String>());
    prop::collection::vec((text, any::<bool>(), 0..4_u8), 0..8)
}

#[test]
fn task_query_rejects_pending_or_ended_published_streams() {
    use acyclic_harness::task_volume::ChangeStream;
    let streams: [ChangeStream; 2] = [
        Box::pin(futures::stream::pending()),
        Box::pin(futures::stream::empty()),
    ];
    for mut changes in streams {
        assert!(super::task_queries::next_published(&mut changes).is_err());
    }
}

proptest! {
    #[test]
    fn task_query_codec_preserves_unicode_channels_and_exact_expectations(
        records in history(),
        chunks in any::<bool>(), selection in 0..4_u8,
    ) {
        let case = generated_case(records, chunks, selection);
        let bytes = serde_json::to_vec(&case)?;
        let decoded: TaskQueryCase = serde_json::from_slice(&bytes)?;
        prop_assert_eq!(decoded, case);
    }

    #[test]
    fn task_query_every_snapshot_follow_cut_equals_uninterrupted_history(
        records in history(),
        chunks in any::<bool>(), selection in 0..4_u8,
    ) {
        let case = generated_case(records, chunks, selection);
        let result = super::task_queries::run(case).now_or_never();
        prop_assert!(matches!(result, Some(Ok(()))), "{result:?}");
    }
}

#[test]
fn task_query_codec_seed_corpus() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness")
        .join("../harness-fuzz/corpus/task_query_case");
    for entry in std::fs::read_dir(root)? {
        let bytes = std::fs::read(entry?.path())?;
        if let Ok(case) = serde_json::from_slice::<TaskQueryCase>(&bytes) {
            let decoded: TaskQueryCase = serde_json::from_slice(&serde_json::to_vec(&case)?)?;
            assert_eq!(case, decoded);
            for input in &case.writes {
                let _ = input.write();
            }
        }
    }
    Ok(())
}
