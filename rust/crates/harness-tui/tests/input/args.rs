use acyclic_harness_tui::{unix_editor_command, windows_editor_command};
use proptest::prelude::*;
use std::ffi::OsString;

#[test]
fn windows_paths_backslashes_and_native_quotes_are_preserved()
-> Result<(), Box<dyn std::error::Error>> {
    for (command, program, arguments) in [
        (
            r#"C:\Windows\System32\notepad.exe"#,
            r#"C:\Windows\System32\notepad.exe"#,
            vec![],
        ),
        (
            r#""C:\Program Files\Editor\edit.exe" --wait "C:\tmp\a b.txt" "" "a\"b" "tail\\""#,
            r#"C:\Program Files\Editor\edit.exe"#,
            vec!["--wait", r#"C:\tmp\a b.txt"#, "", "a\"b", "tail\\"],
        ),
        (
            r#"\\server\share\edit.exe O'Brien"#,
            r#"\\server\share\edit.exe"#,
            vec!["O'Brien"],
        ),
    ] {
        assert_eq!(
            windows_editor_command(command)?,
            (
                OsString::from(program),
                arguments.into_iter().map(OsString::from).collect()
            )
        );
    }
    assert!(windows_editor_command(r#""C:\Program Files\Editor\edit.exe"#).is_err());
    assert!(windows_editor_command(r#""" --wait"#).is_err());
    // Platform dispatch, without launching an editor or creating a console.
    #[cfg(windows)]
    assert_eq!(
        acyclic_harness_tui::editor_command(r#"C:\Windows\System32\notepad.exe"#)?.0,
        OsString::from(r#"C:\Windows\System32\notepad.exe"#)
    );
    assert_eq!(
        unix_editor_command("editor 'a b'")?.1,
        vec![OsString::from("a b")]
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]
    #[test]
    fn windows_command_words_round_trip(args in prop::collection::vec(
        prop::collection::vec(prop_oneof![Just('\\'), Just('"'), Just(' '), any::<char>()], 0..16)
            .prop_map(|characters| characters.into_iter().collect::<String>()), 0..6
    )) {
        let words: Vec<String> = std::iter::once(r#"C:\Program Files\Editor\edit.exe"#.to_owned()).chain(args.clone()).collect();
        let encoded = super::sequences::windows_command_line(&words);
        let (program, restored) = windows_editor_command(&encoded)?;
        prop_assert_eq!(program, OsString::from(&words[0]));
        prop_assert_eq!(restored, args.into_iter().map(OsString::from).collect::<Vec<_>>());
    }
}
