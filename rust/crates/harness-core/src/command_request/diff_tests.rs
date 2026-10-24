use super::*;

fn check<const OPEN: bool>(selection: &str, index: usize, operation: OperationId) {
    let modes = ["unified", "split", "stat"];
    let settings = json!({"diff_modes":modes,"diff_mode":modes[index],"retained":selection});
    let layout = json!({"selected_task":selection,"history_page":42,"open_view":"help"});
    let snapshot = BTreeMap::from([
        (
            CommandFileRoot::Config,
            BTreeMap::from([("settings.json".into(), settings.clone())]),
        ),
        (
            CommandFileRoot::Session,
            BTreeMap::from([("layout.json".into(), layout.clone())]),
        ),
    ]);
    let command = CommandResolution::Select {
        query: CommandQuery::Diff,
    };
    assert_eq!(
        command.file_inputs(),
        &[
            (CommandFileRoot::Config, "settings.json"),
            (CommandFileRoot::Session, "layout.json")
        ]
    );
    let mut changes = command.file_changes(&snapshot, operation).unwrap();
    if !OPEN {
        changes.retain(|change| change.root != CommandFileRoot::Session);
    }
    assert_eq!(changes.len(), 2, "diff must publish both mode and view");
    for change in &changes {
        assert_eq!(change.operation, operation);
        assert!(!change.create_only);
    }
    let mut expected = settings;
    expected["diff_mode"] = json!(modes[(index + 1) % modes.len()]);
    assert_eq!(changes[0].contents, expected);
    let mut expected = layout;
    expected["open_view"] = json!("diff");
    expected["view_input"] = Value::Null;
    expected["view_request"] = json!(operation);
    assert_eq!(changes[1].contents, expected);
}

proptest::proptest! {
    #[test]
    fn diff_cycles_and_opens_without_losing_selection(selection in ".{0,64}", index in 0usize..3, id in proptest::array::uniform16(proptest::num::u8::ANY)) {
        check::<true>(&selection,index,OperationId::from_bytes(id));
    }
}

#[test]
#[should_panic(expected = "diff must publish both mode and view")]
fn settings_only_negative_control() {
    check::<false>("retained", 0, OperationId::new());
}
