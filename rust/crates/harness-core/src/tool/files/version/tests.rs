use super::*;

#[test]
fn version_tokens_exclude_display_metadata_and_retain_native_identity_custody() -> Result<()> {
    let original = crate::tool::tests::file(b"original")?;
    let renamed = FileRef::new(
        original.volume().clone(),
        original.path().into(),
        original.version().into(),
        original.descriptor().clone(),
        "A human readable label".into(),
        &mut None,
    )?;
    let version = encode_version(&renamed, None)?.0;
    assert_eq!(version, encode_version(&original, None)?.0);
    assert!(!version.contains("display_name"));
    assert!(!version.contains("A human readable label"));
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    for native in [false, true] {
        let (decoded, custody) =
            decode_version(original.path(), &version, native.then_some(&work))?;
        let SelectedVersion::Content(file) = decoded else {
            panic!("content version")
        };
        assert!(file.same_identity(&renamed));
        assert_eq!(
            file.read_capability(&mut None)?,
            renamed.read_capability(&mut None)?
        );
        assert!(
            SelectedVersion::Content(file.clone())
                .matches_source(&FileReadSource::Content(renamed.clone()))
        );
        assert!(decode_version("another.txt", &version, native.then_some(&work)).is_err());
        // Negative control: a provider revision is not a whole FileRef, nor is
        // presentation metadata accepted as an immutable selection field.
        let mut wire: Value = serde_json::from_str(version.strip_prefix(CONTENT).unwrap()).unwrap();
        wire["display_name"] = json!("invented metadata");
        let malformed = format!("{CONTENT}{wire}");
        assert!(decode_version(original.path(), &malformed, native.then_some(&work)).is_err());
        drop(file);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    // The actual fuzz entry exercises both decoders and cancellation on this
    // valid input as well as the ordinary arbitrary-byte corpus.
    check_version(version.as_bytes());
    Ok(())
}
