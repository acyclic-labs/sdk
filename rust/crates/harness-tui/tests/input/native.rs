#[cfg(unix)]
use acyclic_harness_tui::edit_external;
use acyclic_harness_tui::{ClipboardContent, prepare_image};

#[cfg(unix)]
#[test]
fn external_editor_success_cancellation_and_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("prompt with spaces.txt");
    assert_eq!(
        edit_external(
            "original",
            &path,
            Some(
                "sh -c 'IFS= read -r original < \"$1\"; [ \"$original\" = original ] && printf edited > \"$1\"' editor-test"
            )
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
