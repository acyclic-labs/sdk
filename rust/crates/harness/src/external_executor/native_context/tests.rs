use super::*;
use proptest::prelude::*;
use serde_json::{Value, json};

mod source;

fn header() -> Value {
    json!({"type":"session","version":3,
    "id":"00000000-0000-0000-0000-000000000001","timestamp":"2026-10-17T00:00:00Z","cwd":"/project"})
}

fn row(id: &str, parent: Option<&str>, kind: &str, extra: Value) -> Value {
    let mut value =
        json!({"id":id,"parentId":parent,"timestamp":"2026-10-17T00:00:00Z","type":kind});
    value
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    value
}

fn user(id: &str, parent: Option<&str>, text: &str) -> Value {
    row(
        id,
        parent,
        "message",
        json!({"message":{"role":"user","content":text,"timestamp":1}}),
    )
}

fn assistant(id: &str, parent: Option<&str>, calls: &[(&str, &str)]) -> Value {
    row(
        id,
        parent,
        "message",
        json!({"message":{"role":"assistant","content":calls.iter().map(|(id,name)|
        json!({"type":"toolCall","id":id,"name":name,"arguments":{}})).collect::<Vec<_>>(),
        "timestamp":1,"provider":"test","model":"probe","stopReason":"toolUse"}}),
    )
}

fn result(id: &str, parent: Option<&str>, call: &str, name: &str) -> Value {
    row(
        id,
        parent,
        "message",
        json!({"message":{"role":"toolResult","toolCallId":call,"toolName":name,
        "content":[{"type":"text","text":"retained exact result"}],"timestamp":1,"isError":false}}),
    )
}

fn bytes(header: &Value, rows: &[Value]) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(header).unwrap();
    bytes.push(b'\n');
    for row in rows {
        bytes.extend(serde_json::to_vec(row).unwrap());
        bytes.push(b'\n')
    }
    bytes
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Exact cuts retain every selected raw record, exclude the whole invoking
    // assistant, survive branch reloads, and reject changes of source or leaf.
    #[test]
    fn branch_cuts_round_trip_and_reject_swapped_sources(
        history in 0usize..8, alternatives in 0usize..4, compact in any::<bool>(),
        edit in any::<bool>(), text in "[^\\x00]{1,48}",
    ) {
        let mut rows=Vec::new();let mut parent=None;
        for n in 0..history {
            let id=format!("user-{n}");rows.push(user(&id,parent.as_deref(),&text));parent=Some(id);
        }
        let completed=format!("completed-\"{text}\\");
        rows.push(assistant("completed",parent.as_deref(),&[(&completed,"read")]));
        rows.push(result("result",Some("completed"),&completed,"read"));
        parent=Some("result".to_owned());
        if compact {
            let kept=if history==0 {"completed"} else {"user-0"};
            rows.push(row("compact",parent.as_deref(),"compaction",json!({
                "firstKeptEntryId":kept,"summary":"native summary","tokensBefore":20000,
                "details":{"provenance":"exact original detail"}})));
            parent=Some("compact".into());
        }
        if edit && history>0 {
            rows.push(row("edit",parent.as_deref(),"context_edit",json!({"targetId":"user-0",
                "replacement":{"content":format!("edited {text}")}})));
            parent=Some("edit".into());
        }
        let selected=rows.iter().map(|row|serde_json::to_vec(row).unwrap()).collect::<Vec<_>>();
        for n in 0..alternatives {rows.push(user(&format!("alternative-{n}"),parent.as_deref(),"unselected branch"));}
        rows.push(assistant("invoking",parent.as_deref(),&[("spawn","acyclic_spawn"),("sibling","write")]));
        rows.push(result("sibling-result",Some("invoking"),"sibling","write"));
        let original=bytes(&header(),&rows);
        let file=SessionFile::decode(&original,original.len() as u64)?;
        let cut=file.cut(Some("sibling-result"),"spawn")?;
        prop_assert_eq!(&cut.tool,"acyclic_spawn");
        prop_assert_eq!(&cut.through,&parent);
        let encoded=serde_json::to_vec(&cut).unwrap();
        let restored:ContextCut=serde_json::from_slice(&encoded).unwrap();
        prop_assert_eq!(&cut,&restored);
        prop_assert_eq!(file.require_cut(Some("sibling-result"),&restored)?,selected.iter().map(Vec::as_slice).collect::<Vec<_>>());
        let mut child_header=header();child_header["id"]=json!("00000000-0000-0000-0000-000000000003");
        let child=file.child_file(Some("sibling-result"),&cut,&serde_json::to_vec(&child_header).unwrap(),std::env::temp_dir().as_path())?;
        let child=SessionFile::decode(&child,child.len() as u64)?;
        prop_assert_eq!(child.rows.iter().map(|row|row.bytes).collect::<Vec<_>>(),selected.iter().map(Vec::as_slice).collect::<Vec<_>>());
        rows.push(user("later",Some("sibling-result"),"changed current branch"));
        let changed=bytes(&header(),&rows);
        let file=SessionFile::decode(&changed,changed.len() as u64)?;
        prop_assert!(file.require_cut(Some("later"),&cut).is_err());
        prop_assert_eq!(file.cut(Some("sibling-result"),"spawn")?,cut.clone(),"unselected later history changed a pinned branch");
        for (key,value) in [("cwd",json!("/foreign-project")),("id",json!("00000000-0000-0000-0000-000000000002"))] {
            let mut foreign=header();foreign[key]=value;
            let bytes=bytes(&foreign,&rows);
            prop_assert!(SessionFile::decode(&bytes,bytes.len() as u64)?.require_cut(Some("sibling-result"),&cut).is_err());
        }
        prop_assert!(SessionFile::decode(&original,original.len() as u64-1).is_err());
    }
}

#[test]
fn edits_cannot_hide_half_an_exchange_and_duplicate_invocations_reject() -> Result<()> {
    for (target, replacement) in [
        ("result", Value::Null),
        ("call", json!({"content":"tool call removed"})),
    ] {
        let rows = [
            assistant("call", None, &[("completed", "read")]),
            result("result", Some("call"), "completed", "read"),
            row(
                "edit",
                Some("result"),
                "context_edit",
                json!({"targetId":target,"replacement":replacement}),
            ),
            assistant("invoking", Some("edit"), &[("spawn", "acyclic_spawn")]),
        ];
        let bytes = bytes(&header(), &rows);
        let file = SessionFile::decode(&bytes, bytes.len() as u64)?;
        assert!(file.cut(Some("invoking"), "spawn").is_err());
    }
    let rows = [assistant(
        "invoking",
        None,
        &[("spawn", "acyclic_spawn"), ("spawn", "acyclic_spawn")],
    )];
    let bytes = bytes(&header(), &rows);
    assert!(
        SessionFile::decode(&bytes, bytes.len() as u64)?
            .cut(Some("invoking"), "spawn")
            .is_err()
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Every pending result must precede the next conversational message;
    // completing the same IDs later cannot make an interleaved cut admissible.
    #[test]
    fn complete_exchanges_cannot_be_repaired_after_an_intervening_message(
        calls in 1usize..5, completed in 0usize..5, text in ".{0,48}",
        message_kind in 0u8..3,
    ) {
        let completed=completed.min(calls);
        let ids=(0..calls).map(|i|format!("read-{i}")).collect::<Vec<_>>();
        let names=ids.iter().map(|id|(id.as_str(),"read")).collect::<Vec<_>>();
        let mut rows=vec![assistant("calls",None,&names)];
        let mut parent="calls".to_owned();
        for id in &ids[..completed] {
            rows.push(result(id,Some(&parent),id,"read"));parent=id.clone();
        }
        rows.push(match message_kind {
            0 => user("intervening",Some(&parent),&text),
            1 => row("intervening",Some(&parent),"message",json!({"message":{
                "role":"assistant","content":[{"type":"text","text":text}],"timestamp":1}})),
            _ => row("intervening",Some(&parent),"custom_message",json!({
                "customType":"selected-import","content":text,"display":true})),
        });
        parent="intervening".into();
        for id in &ids[completed..] {
            rows.push(result(id,Some(&parent),id,"read"));parent=id.clone();
        }
        rows.push(assistant("invocation",Some(&parent),&[("spawn","acyclic_spawn")]));
        let original=bytes(&header(),&rows);
        let source=SessionFile::decode(&original,original.len() as u64)?;
        prop_assert_eq!(source.cut(Some("invocation"),"spawn").is_ok(),completed==calls);
    }
}

#[test]
fn missing_forward_and_cyclic_parent_references_are_unrepresentable() {
    for rows in [
        vec![user("root", Some("missing"), "x")],
        vec![
            user("root", Some("later"), "x"),
            user("later", Some("root"), "x"),
        ],
        vec![user("root", Some("root"), "x")],
        vec![user("root", None, "x"), user("root", None, "duplicate")],
    ] {
        let bytes = bytes(&header(), &rows);
        assert!(SessionFile::decode(&bytes, bytes.len() as u64).is_err());
    }
    let mut missing = user("root", None, "x");
    missing.as_object_mut().unwrap().remove("parentId");
    let bytes = bytes(&header(), &[missing]);
    assert!(SessionFile::decode(&bytes, bytes.len() as u64).is_err());
}

#[test]
fn changing_the_excluded_invocation_still_invalidates_its_source_guard() -> Result<()> {
    let mut rows = vec![
        user("root", None, "retained"),
        assistant("invoking", Some("root"), &[("spawn", "acyclic_spawn")]),
    ];
    let source = bytes(&header(), &rows);
    let file = SessionFile::decode(&source, source.len() as u64)?;
    let cut = file.cut(Some("invoking"), "spawn")?;
    rows[1]["message"]["content"][0]["arguments"] = json!({"id":"swapped-child"});
    let changed = bytes(&header(), &rows);
    assert!(
        SessionFile::decode(&changed, changed.len() as u64)?
            .require_cut(Some("invoking"), &cut)
            .is_err()
    );
    Ok(())
}

#[test]
fn compaction_does_not_resurrect_a_summarized_incomplete_exchange() -> Result<()> {
    let rows = [
        assistant("old", None, &[("summarized-call", "read")]),
        row(
            "compact",
            Some("old"),
            "compaction",
            json!({"firstKeptEntryId":"compact","summary":"old exchange summarized","tokensBefore":10}),
        ),
        assistant("invoking", Some("compact"), &[("spawn", "acyclic_spawn")]),
    ];
    let bytes = bytes(&header(), &rows);
    let file = SessionFile::decode(&bytes, bytes.len() as u64)?;
    let cut = file.cut(Some("invoking"), "spawn")?;
    assert_eq!(
        file.require_cut(Some("invoking"), &cut)?.len(),
        2,
        "raw compaction provenance was lost"
    );
    Ok(())
}

#[test]
fn records_keep_their_original_member_order_and_provenance_bytes() -> Result<()> {
    let compact=br#"{"timestamp":"now","type":"compaction","details":{"z":1,"a":2},"id":"c","parentId":null,"firstKeptEntryId":"c","summary":"native","tokensBefore":5}"#;
    let mut input = serde_json::to_vec(&header()).unwrap();
    input.push(b'\n');
    input.extend_from_slice(compact);
    input.push(b'\n');
    input.extend(
        serde_json::to_vec(&assistant(
            "invoking",
            Some("c"),
            &[("spawn", "acyclic_spawn")],
        ))
        .unwrap(),
    );
    let file = SessionFile::decode(&input, input.len() as u64)?;
    let cut = file.cut(Some("invoking"), "spawn")?;
    assert_eq!(
        file.require_cut(Some("invoking"), &cut)?,
        vec![compact.as_slice()]
    );
    Ok(())
}

#[test]
fn checked_context_corpus_covers_compaction_edits_and_incomplete_exchanges() -> Result<()> {
    for (bytes, valid) in [
        (
            include_bytes!(
                "../../../../harness-fuzz/corpus/external-hooks/native-context-complete.jsonl"
            )
            .as_slice(),
            true,
        ),
        (
            include_bytes!(
                "../../../../harness-fuzz/corpus/external-hooks/native-context-missing-result.jsonl"
            )
            .as_slice(),
            false,
        ),
        (
            include_bytes!(
                "../../../../harness-fuzz/corpus/external-hooks/native-context-interleaved.jsonl"
            )
            .as_slice(),
            false,
        ),
    ] {
        let file = SessionFile::decode(bytes, bytes.len() as u64)?;
        assert_eq!(file.cut(Some("s"), "spawn").is_ok(), valid);
        fuzz(bytes);
    }
    Ok(())
}
