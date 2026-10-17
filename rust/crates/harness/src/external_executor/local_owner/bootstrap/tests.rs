use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Only aliases of the registered canonical workspace may reach connection
    // startup. A missing or foreign directory cannot acquire its credential.
    #[test]
    fn result_selection_accepts_aliases_and_rejects_other_workspaces(part in "[a-zA-Z0-9_-]{1,16}") {
        let fixture = tempfile::tempdir()?;
        let source = fixture.path().join(format!("source-{part}"));
        let foreign = fixture.path().join(format!("foreign-{part}"));
        std::fs::create_dir_all(source.join("nested"))?;
        std::fs::create_dir(&foreign)?;
        let canonical = std::fs::canonicalize(&source)?;
        let alias = source.join("nested/..");
        for selection in [None,Some(canonical.as_path()),Some(source.as_path()),Some(alias.as_path())] {
            prop_assert!(require_selection(&canonical,selection).is_ok());
        }
        prop_assert!(require_selection(&canonical,Some(&foreign)).is_err());
        prop_assert!(require_selection(&canonical,Some(&fixture.path().join("absent"))).is_err());
    }
}
