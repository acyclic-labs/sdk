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
        for (index, expected) in cells.iter().enumerate() {
            let (text, page, _hold) = read(&source, index as u64, Some(1), u64::MAX, None).unwrap();
            prop_assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), expected.clone());
            prop_assert_eq!(&source[page.start as usize..page.end as usize], text);
        }
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
