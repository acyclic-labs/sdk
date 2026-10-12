use super::*;
#[test]
fn selected_writeback_plan_cannot_smuggle_unselected_paths() -> Result<()> {
    use acyclic_fs::MaterializationEdit;
    let paths = vec!["selected".to_owned()];
    require_selected_edits(&[MaterializationEdit::Remove { path: "selected/file".into() }], &paths)?;
    assert!(require_selected_edits(&[MaterializationEdit::Remove { path: "selected-other/file".into() }], &paths).is_err());
    assert!(require_selected_edits(&[MaterializationEdit::Rename { from: "selected/file".into(), to: "foreign/file".into() }], &paths).is_err());
    Ok(())
}
