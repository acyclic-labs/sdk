use acyclic_fs::CancellationToken;
use acyclic_fs::kernel::{LogicalName, NameEncoding, TreePageError};
use acyclic_fs_work::performance::WorkBudget;

#[test]
fn windows_names_validate_utf16_without_lossy_normalization()
-> Result<(), Box<dyn std::error::Error>> {
    let emoji: Vec<u8> = "😀.rs".encode_utf16().flat_map(u16::to_le_bytes).collect();
    let name = LogicalName::new(NameEncoding::WindowsUtf16Le, emoji.clone())?;
    assert_eq!(name.as_bytes(), emoji);
    // Native Windows names are code units, not necessarily Unicode scalars.
    for opaque in [
        vec![0x00, 0xd8],
        vec![0x00, 0xdc],
        vec![0x00, 0xd8, b'x', 0],
    ] {
        let name = LogicalName::new(NameEncoding::WindowsUtf16Le, opaque.clone())?;
        assert_eq!(name.as_bytes(), opaque);
        assert!(
            name.unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .ok()
                .map(|receipt| receipt.value)
                .is_none(),
            "opaque names must not be normalized into Unicode"
        );
    }

    for invalid in [
        vec![b'/', 0],
        vec![b'\\', 0],
        vec![b'.', 0],
        vec![b'.', 0, b'.', 0],
    ] {
        assert!(matches!(
            LogicalName::new(NameEncoding::WindowsUtf16Le, invalid),
            Err(TreePageError::InvalidName)
        ));
    }
    Ok(())
}
