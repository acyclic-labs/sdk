use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn cell_splices_preserve_unrelated_json_and_reject_stale_ids(
        sources in prop::collection::vec(".{0,80}", 1..12),
        new_source in ".{0,80}",
        position in any::<usize>(),
    ) {
        let index = position % sources.len();
        let cells: Vec<_> = sources.iter().enumerate().map(|(index, source)| json!({
            "id":format!("cell-{index}"), "cell_type":"code", "source":[source],
            "outputs":[{"output_type":"stream","name":"stdout","text":[source]}],
            "execution_count":17, "metadata":{"preserve":source}, "extension":[1,2,3],
        })).collect();
        let original = json!({"nbformat":4,"nbformat_minor":5,"cells":cells,"metadata":{"keep":true},"extra":"retain"});
        let source = serde_json::to_string(&original).unwrap();
        let id = format!("cell-{index}");
        let replace = NotebookEdit::Replace { cell:NotebookCell::Index {index:index as u64, id:Some(id.clone())}, source:new_source.clone(), cell_type:None };
        let (edited, _hold, facts) = edit(&source, &replace, None).unwrap();
        prop_assert_eq!(facts.index, index as u64);
        let prior = facts.prior.as_ref().unwrap();
        prop_assert_eq!(prior.id.as_deref(), Some(id.as_str()));
        prop_assert_eq!(prior.cell_type.as_ref(), "code");
        prop_assert_eq!(serde_json::from_str::<Value>(prior.source.get()).unwrap(), json!([sources[index]]));
        let actual: Value = serde_json::from_str(&edited).unwrap();
        let mut expected = original.clone();
        expected["cells"][index]["source"] = json!(new_source);
        expected["cells"][index]["outputs"] = json!([]);
        expected["cells"][index]["execution_count"] = Value::Null;
        prop_assert_eq!(actual, expected.clone());
        let by_id = NotebookEdit::Replace {
            cell: NotebookCell::Id { id: id.clone() }, source:new_source.clone(), cell_type:None,
        };
        let (edited, _hold, _facts) = edit(&source, &by_id, None).unwrap();
        prop_assert_eq!(serde_json::from_str::<Value>(&edited).unwrap(), expected);
        prop_assert!(edit(&source, &NotebookEdit::Delete {cell:NotebookCell::Index {index:index as u64,id:Some("stale".into())}}, None).is_err(), "stale cell id accepted");

        // Inserting and deleting at every boundary exercises both comma
        // directions, including the formerly empty notebook.
        let insert_at = position % (sources.len() + 1);
        let insert = NotebookEdit::Insert { cell:NotebookCell::Index {index:insert_at as u64,id:None},after:false,id:"inserted".into(),cell_type:"markdown".into(),source:new_source };
        let (inserted, _insert_hold, facts) = edit(&source, &insert, None).unwrap();
        prop_assert_eq!(facts.index, insert_at as u64);
        prop_assert!(facts.prior.is_none());
        let (removed, _remove_hold, facts) = edit(&inserted, &NotebookEdit::Delete {cell:NotebookCell::Id {id:"inserted".into()}}, None).unwrap();
        prop_assert_eq!(facts.index, insert_at as u64);
        prop_assert_eq!(facts.prior.as_ref().unwrap().id.as_deref(), Some("inserted"));
        prop_assert_eq!(&serde_json::from_str::<Value>(&removed).unwrap(), &original);
        prop_assert!(edit(&source, &NotebookEdit::Insert {cell:NotebookCell::Index {index:0,id:None},after:false,id,cell_type:"raw".into(),source:String::new()}, None).is_err(), "duplicate cell id accepted");

        let after = NotebookEdit::Insert {
            cell: NotebookCell::Id {id:format!("cell-{index}")}, after:true,
            id:"after".into(),cell_type:"code".into(),source:"inserted".into(),
        };
        let (inserted, _hold, facts) = edit(&source, &after, None).unwrap();
        prop_assert_eq!(facts.index, index as u64 + 1);
        prop_assert!(facts.prior.is_none());
        let inserted: Value = serde_json::from_str(&inserted).unwrap();
        prop_assert_eq!(&inserted["cells"][index + 1]["id"], &json!("after"));
        let mut expected = original.clone();
        expected["cells"].as_array_mut().unwrap().insert(index + 1, json!({
            "id":"after","cell_type":"code","source":"inserted","metadata":{},
            "outputs":[],"execution_count":null,
        }));
        prop_assert_eq!(inserted, expected);

        let convert = NotebookEdit::Replace {
            cell: NotebookCell::Id {id:format!("cell-{index}")},
            source:"markdown".into(),cell_type:Some("markdown".into()),
        };
        let (converted, _hold, _facts) = edit(&source, &convert, None).unwrap();
        let mut expected = original.clone();
        let selected = expected["cells"][index].as_object_mut().unwrap();
        selected.insert("source".into(), json!("markdown"));
        selected.insert("cell_type".into(), json!("markdown"));
        selected.remove("outputs");
        selected.remove("execution_count");
        prop_assert_eq!(serde_json::from_str::<Value>(&converted).unwrap(), expected.clone());
        let restore = NotebookEdit::Replace {
            cell: NotebookCell::Id {id:format!("cell-{index}")},
            source:"code".into(),cell_type:Some("code".into()),
        };
        let (restored, _hold, facts) = edit(&converted, &restore, None).unwrap();
        prop_assert_eq!(facts.prior.as_ref().unwrap().cell_type.as_ref(), "markdown");
        let selected = expected["cells"][index].as_object_mut().unwrap();
        selected.insert("source".into(), json!("code"));
        selected.insert("cell_type".into(), json!("code"));
        selected.insert("outputs".into(), json!([]));
        selected.insert("execution_count".into(), Value::Null);
        prop_assert_eq!(serde_json::from_str::<Value>(&restored).unwrap(), expected);

        // A stable ID must select exactly one cell, never the first duplicate.
        let mut duplicate = original;
        let selected = duplicate["cells"][index].clone();
        duplicate["cells"].as_array_mut().unwrap().push(selected);
        prop_assert!(edit(&serde_json::to_string(&duplicate).unwrap(), &by_id, None).is_err(), "ambiguous cell id accepted");
        prop_assert!(edit(&source, &NotebookEdit::Delete {cell:NotebookCell::Id {id:"absent".into()}}, None).is_err(), "missing cell id accepted");
    }
}

#[test]
fn empty_notebook_insert_delete_retains_metadata() {
    let source = r#"{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"keep":true}}"#;
    let (inserted, _hold, _facts) = edit(
        source,
        &NotebookEdit::Insert {
            cell: NotebookCell::Index { index: 0, id: None },
            after: false,
            id: "first".into(),
            cell_type: "code".into(),
            source: "print(1)".into(),
        },
        None,
    )
    .unwrap();
    let cell: Value = serde_json::from_str(&inserted).unwrap();
    assert_eq!(cell["cells"][0]["execution_count"], Value::Null);
    assert_eq!(cell["cells"][0]["outputs"], json!([]));
    let (removed, _hold, _facts) = edit(
        &inserted,
        &NotebookEdit::Delete {
            cell: NotebookCell::Id { id: "first".into() },
        },
        None,
    )
    .unwrap();
    assert_eq!(removed, source);
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn borrowed_cell_decode_keeps_original_credit_through_publication_bytes() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};

    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_048_576, 1_048_576),
        &cancellation,
    )
    .unwrap();
    // Escaped field names and IDs exercise serde's owned escape scratch;
    // the raw source/output spans remain borrowed from this same document.
    let source = r#"{"nbformat":4,"cells":[{"i\u0064":"\u0061","cell_type":"code","source":["x\n"],"outputs":[],"execution_count":1}],"metadata":{"keep":true}}"#;
    let baseline = native.live_allocation_bytes();
    let (text, hold, facts) = edit(
        source,
        &NotebookEdit::Replace {
            cell: NotebookCell::Id { id: "a".into() },
            source: "\"\\\nλ".into(),
            cell_type: None,
        },
        Some(&native),
    )
    .unwrap();
    assert!(native.live_allocation_bytes() > baseline);
    assert_eq!(facts.index, 0);
    let prior = facts.prior.as_ref().unwrap();
    assert_eq!(prior.id.as_deref(), Some("a"));
    assert_eq!(prior.cell_type, "code");
    assert_eq!(
        serde_json::from_str::<Value>(prior.source.get()).unwrap(),
        json!(["x\n"])
    );
    let actual: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(actual["cells"][0]["source"], "\"\\\nλ");
    assert_eq!(actual["cells"][0]["execution_count"], Value::Null);
    drop(facts);
    drop(text);
    drop(hold);
    assert_eq!(native.live_allocation_bytes(), baseline);

    for (source, kind) in [
        (source, "markdown"),
        // A removed final field shares its comma with a removed neighbor.
        (
            r#"{"nbformat":4,"cells":[{"outputs":[],"execution_count":1,"source":[],"cell_type":"code","id":"a"}]}"#,
            "raw",
        ),
        (
            r#"{"nbformat":4,"cells":[{"id":"a","source":[],"cell_type":"code","execution_count":1,"metadata":{"keep":true},"outputs":[]}]}"#,
            "markdown",
        ),
        // Escaped type/attachment labels use the same borrowed codec and spans.
        (
            r#"{"nbformat":4,"cells":[{"id":"a","cell_type":"markdown","source":[],"att\u0061chments":{"x":{"image/png":"ignored"}}}]}"#,
            "code",
        ),
    ] {
        let (text, hold, facts) = edit(
            source,
            &NotebookEdit::Replace {
                cell: NotebookCell::Id { id: "a".into() },
                source: "converted".into(),
                cell_type: Some(kind.into()),
            },
            Some(&native),
        )
        .unwrap();
        assert_eq!(facts.index, 0);
        assert_eq!(facts.prior.as_ref().unwrap().id.as_deref(), Some("a"));
        let value: Value = serde_json::from_str(&text).unwrap();
        let cell = &value["cells"][0];
        assert_eq!(cell["cell_type"], kind);
        assert_eq!(cell["source"], "converted");
        if kind == "code" {
            assert_eq!(cell["outputs"], json!([]));
            assert_eq!(cell["execution_count"], Value::Null);
            assert!(cell.get("attachments").is_none());
        } else {
            assert!(cell.get("outputs").is_none());
            assert!(cell.get("execution_count").is_none());
        }
        drop(facts);
        drop(text);
        drop(hold);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }

    // A differently encoded spelling of the same stable ID must conflict.
    let error = edit(
        source,
        &NotebookEdit::Insert {
            cell: NotebookCell::Index { index: 0, id: None },
            after: false,
            id: "a".into(),
            cell_type: "raw".into(),
            source: String::new(),
        },
        Some(&native),
    )
    .err()
    .expect("duplicate ID refused");
    assert!(matches!(error, Error::ToolRejected { .. }));
    drop(error);
    assert_eq!(native.live_allocation_bytes(), baseline);

    // Typed field diagnostics retain the same recoverable classification and
    // original ownership on malformed cells, missing fields and duplicates.
    for malformed in [
        r#"[4,[{"id":"a","cell_type":"code","source":[]}]]"#,
        r#"{"cells":[]}"#,
        r#"{"nbformat":4,"cells":null}"#,
        r#"{"nbformat":4,"cells":[],"cells":[]}"#,
        r#"{"nbformat":4,"cells":[{"source":[]}] }"#,
        r#"{"nbformat":4,"cells":[["a","code",[],null,null]]}"#,
        r#"{"nbformat":4,"cells":[{"cell_type":"code","source":null,"source":[]}] }"#,
    ] {
        let operation = NotebookEdit::Replace {
            cell: NotebookCell::Index { index: 0, id: None },
            source: "new".into(),
            cell_type: None,
        };
        assert!(edit(malformed, &operation, None).is_err());
        let error = edit(malformed, &operation, Some(&native))
            .err()
            .expect("malformed notebook refused");
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
        assert!(error.is_tool_failure(), "{error:?}");
        drop(error);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }

    // Refusal happens on the original ledger before a decoded value escapes.
    cancellation.cancel();
    assert!(
        edit(
            source,
            &NotebookEdit::Delete {
                cell: NotebookCell::Index { index: 0, id: None }
            },
            Some(&native)
        )
        .is_err()
    );
    assert_eq!(native.live_allocation_bytes(), baseline);
}
