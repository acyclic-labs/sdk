#[cfg(unix)]
use acyclic_harness_tui::edit_external;
use acyclic_harness_tui::{
    ClipboardContent, Editor, InputKeymap, load_draft, load_history, prepare_image, save_draft,
    save_history,
};

#[test]
fn draft_history_and_keymap_views_follow_their_injected_files()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("private/question-1.md");
    let other = directory.path().join("private/question-2.md");
    assert!(load_draft(&path)?.text().is_empty());
    let mut editor = Editor::new("a界e\u{301}");
    editor.set_cursor(1, true);
    save_draft(&path, &editor)?;
    save_draft(&other, &Editor::new("separate"))?;
    assert_eq!(load_draft(&path)?.text(), "a界e\u{301}");
    assert_eq!(load_draft(&other)?.text(), "separate");
    save_draft(&path, &Editor::new("fresh"))?;
    assert_eq!(load_draft(&path)?.text(), "fresh");
    std::fs::write(&path, "external\r\nchange\r")?;
    assert_eq!(load_draft(&path)?.text(), "external\nchange\n");
    assert_eq!(
        std::fs::read_dir(path.parent().ok_or("missing parent")?)?.count(),
        2
    );
    let history_path = directory.path().join("config/input-history.json");
    let mut history = load_history(&history_path)?;
    assert!(
        history.generation() > 0,
        "missing history files still produce a loaded generation"
    );
    history.push("first\r\nsecond".into());
    save_history(&history_path, &history)?;
    assert_eq!(load_history(&history_path)?.entries(), &["first\nsecond"]);
    std::fs::write(&history_path, r#"["external\rhistory"]"#)?;
    assert_eq!(
        load_history(&history_path)?.entries(),
        &["external\nhistory"]
    );
    let keymap_path = directory.path().join("config/keymap.json");
    InputKeymap::parse(r#"{"bindings":{"Ctrl+l":"cursor.line-start"}}"#)
        .unwrap()
        .save(&keymap_path)?;
    assert!(
        InputKeymap::load(&keymap_path)?
            .help()
            .contains("Ctrl+l\tLine start")
    );
    std::fs::write(&keymap_path, r#"{"bindings":{"Ctrl+l":"cursor.line-end"}}"#)?;
    assert!(
        InputKeymap::load(&keymap_path)?
            .help()
            .contains("Ctrl+l\tLine end")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path)?.permissions().mode() & 0o777,
            0o600
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn external_editor_success_cancellation_and_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("prompt with spaces.txt");
    assert_eq!(
        edit_external(
            "original",
            &path,
            Some("sh -c 'printf edited > \"$1\"' editor-test")
        )?,
        Some("edited".into())
    );
    assert!(!path.exists());
    assert_eq!(
        edit_external("original", &path, Some("sh -c 'exit 7' editor-test"))?,
        None
    );
    assert!(!path.exists());
    std::fs::write(&path, "existing")?;
    assert!(edit_external("original", &path, Some("sh -c 'exit 0' editor-test")).is_err());
    assert_eq!(std::fs::read_to_string(&path)?, "existing");
    Ok(())
}

#[test]
fn clipboard_image_dimensions_are_validated() -> Result<(), Box<dyn std::error::Error>> {
    let ClipboardContent::Image {
        jpeg,
        width,
        height,
    } = prepare_image(1600, 1, vec![255; 1600 * 4])?
    else {
        return Err("expected image".into());
    };
    assert_eq!((width, height), (1568, 1));
    let decoded = image::load_from_memory(&jpeg)?;
    assert_eq!((decoded.width(), decoded.height()), (width, height));
    assert!(prepare_image(u32::MAX, u32::MAX, vec![]).is_err());
    assert!(prepare_image(1, 1, vec![0; 3]).is_err());
    Ok(())
}
