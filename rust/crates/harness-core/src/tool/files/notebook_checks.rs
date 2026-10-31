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
        let replace = NotebookEdit::Replace { index:index as u64, id:Some(id.clone()), source:new_source.clone() };
        let (edited, _hold) = edit(&source, &replace, None).unwrap();
        let actual: Value = serde_json::from_str(&edited).unwrap();
        let mut expected = original.clone();
        expected["cells"][index]["source"] = json!(new_source);
        expected["cells"][index]["outputs"] = json!([]);
        expected["cells"][index]["execution_count"] = Value::Null;
        prop_assert_eq!(actual, expected);
        prop_assert!(edit(&source, &NotebookEdit::Delete {index:index as u64,id:Some("stale".into())}, None).is_err(), "stale cell id accepted");

        // Inserting and deleting at every boundary exercises both comma
        // directions, including the formerly empty notebook.
        let insert_at = position % (sources.len() + 1);
        let insert = NotebookEdit::Insert { index:insert_at as u64,id:"inserted".into(),cell_type:"markdown".into(),source:new_source };
        let (inserted, _insert_hold) = edit(&source, &insert, None).unwrap();
        let (removed, _remove_hold) = edit(&inserted, &NotebookEdit::Delete {index:insert_at as u64,id:Some("inserted".into())}, None).unwrap();
        prop_assert_eq!(serde_json::from_str::<Value>(&removed).unwrap(), original);
        prop_assert!(edit(&source, &NotebookEdit::Insert {index:0,id,cell_type:"raw".into(),source:String::new()}, None).is_err(), "duplicate cell id accepted");
    }
}

#[test]
fn empty_notebook_insert_delete_retains_metadata() {
    let source = r#"{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"keep":true}}"#;
    let (inserted, _hold) = edit(
        source,
        &NotebookEdit::Insert {
            index: 0,
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
    let (removed, _hold) = edit(
        &inserted,
        &NotebookEdit::Delete {
            index: 0,
            id: Some("first".into()),
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
    let (text, hold) = edit(
        source,
        &NotebookEdit::Replace {
            index: 0,
            id: Some("a".into()),
            source: "\"\\\nλ".into(),
        },
        Some(&native),
    )
    .unwrap();
    assert!(native.live_allocation_bytes() > baseline);
    let actual: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(actual["cells"][0]["source"], "\"\\\nλ");
    assert_eq!(actual["cells"][0]["execution_count"], Value::Null);
    drop(text);
    drop(hold);
    assert_eq!(native.live_allocation_bytes(), baseline);

    // A differently encoded spelling of the same stable ID must conflict.
    let error = edit(
        source,
        &NotebookEdit::Insert {
            index: 0,
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

    // Refusal happens on the original ledger before a decoded value escapes.
    cancellation.cancel();
    assert!(
        edit(
            source,
            &NotebookEdit::Delete { index: 0, id: None },
            Some(&native)
        )
        .is_err()
    );
    assert_eq!(native.live_allocation_bytes(), baseline);
}
