use super::*;
use proptest::prelude::*;

/// State selection observes the held root only. Ordinary edits preserve its
/// incarnation, and replacement at the same path cannot attach the old owner.
#[test]
fn checkout_state_is_external_and_follows_native_incarnation() -> Result<()> {
    let fixture = tempfile::tempdir().map_err(storage)?;
    let path = fixture.path().join("workspace");
    fs::create_dir(&path).map_err(storage)?;
    let first = Checkout::capture(&path)?;
    assert_eq!(fs::read_dir(&path).map_err(storage)?.count(), 0);
    fs::write(path.join("edit"), b"user edit").map_err(storage)?;
    let same = Checkout::capture(&path)?;
    assert_eq!(first.incarnation, same.incarnation);
    fs::rename(&path, fixture.path().join("old")).map_err(storage)?;
    fs::create_dir(&path).map_err(storage)?;
    let replacement = Checkout::capture(&path)?;
    assert_ne!(first.incarnation, replacement.incarnation);
    assert_eq!(fs::read_dir(&path).map_err(storage)?.count(), 0);
    Ok(())
}

/// Checkout incarnation versioning preserves every configured volume's
/// original state key, including its owner credentials and journals.
#[test]
fn configured_volumes_keep_their_existing_state() {
    let directory = tempfile::tempdir().unwrap();
    let paths = [
        directory.path().to_path_buf(),
        directory.path().join("nested"),
    ];
    fs::create_dir(&paths[1]).unwrap();
    let provider = crate::resources::ProviderRef::new(
        "local".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )
    .unwrap();
    let volume = |name, owner| {
        VolumeRef::new(
            provider.clone(),
            name,
            VolumeClass::Project,
            VolumeOwner::Project(owner),
            &mut None,
        )
        .unwrap()
    };
    // Persistence follows the same path for every volume label. Exercise both
    // directory layouts once; generate all key-selection operands below.
    for path in &paths {
        let project = LocalProject::Volume {
            directory: path.clone(),
            volume: volume("project".into(), "owner".into()),
        };
        let state = state_directory_in(directory.path(), &project, None).unwrap();
        let state = self::directory(&state).unwrap();
        let original =
            Owner::open(&state, &project, None, SigningCustody::Configured, None).unwrap();
        let reopened =
            Owner::open(&state, &project, None, SigningCustody::Configured, None).unwrap();
        assert_eq!(original.key_ref, reopened.key_ref);
        assert_eq!(original.task, reopened.task);
    }
    proptest!(ProptestConfig::with_cases(32), |(
        name in "[a-z0-9]{1,32}", owner in "[a-z0-9]{1,32}", nested in proptest::bool::ANY,
    )| {
        let path = &paths[usize::from(nested)];
        let canonical = fs::canonicalize(path).unwrap();
        let volume = volume(name, owner);
        // Original volume-key algorithm: checkout incarnations do not rekey it.
        let mut legacy = blake3::Hasher::new();
        legacy.update(b"acyclic.local.state/v1\0");
        legacy.update(b"volume\0");
        let bytes = canonical.as_os_str().as_encoded_bytes();
        legacy.update(&(bytes.len() as u64).to_le_bytes());
        legacy.update(bytes);
        legacy.update(volume.storage_name(&mut None).unwrap().as_bytes());
        let expected = directory.path().join(legacy.finalize().to_hex().as_str());
        let project = LocalProject::Volume { directory: path.clone(), volume };
        let state = state_directory_in(directory.path(), &project, None).unwrap();
        prop_assert_eq!(&state, &expected);
        // Restore the production defect and apply the same key detector.
        REKEY_VOLUMES.set(true);
        let mutant = state_directory_in(directory.path(), &project, None);
        REKEY_VOLUMES.set(false);
        prop_assert_ne!(mutant.unwrap(), expected);
    });
}
