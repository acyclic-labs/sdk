//! Owner publication, custody selection and older-format refusal.
use super::*;

fn volume_project(directory: &Path) -> Result<LocalProject> {
    Ok(LocalProject::Volume {
        directory: directory.to_owned(),
        volume: VolumeRef::new(
            crate::resources::ProviderRef::new(
                "local".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
            &mut None,
        )?,
    })
}

/// The same selected file store owns creation, reopening and authority bootstrap.
/// Owners in fresh state directories own distinct entries; another store
/// never stands in for it.
#[test]
fn explicit_file_custody_reopens_store_owned_seeds_without_native_fallback() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let selected = root.path().join("signing-custody");
    let custody = SigningCustody::PrivateFile(&selected);
    let project = volume_project(root.path())?;
    let directories = (0..3)
        .map(|index| super::directory(&root.path().join(format!("state-{index}"))))
        .collect::<Result<Vec<_>>>()?;
    let owners = directories
        .iter()
        .map(|directory| Owner::open(directory, &project, None, custody, None))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        fs::read_dir(&selected).map_err(storage)?.count(),
        owners.len(),
        "each durable store must own exactly one signing entry"
    );
    assert_ne!(owners[0].key_ref, owners[1].key_ref);
    let directory = &directories[0];
    let owner = &owners[0];
    let path = directory.join(RECORD);
    let before = fs::read(&path).map_err(storage)?;
    let audience = crate::core::Authority {
        kind: crate::core::AggregateKind::Task,
        id: owner.task.to_string(),
    };
    let bootstrap = |custody| {
        crate::core::AuthorityRoot::bootstrap(
            "acyclic.local",
            &owner.key_ref,
            custody,
            audience.clone(),
        )
    };
    bootstrap(custody)?;
    let reopened = Owner::open(directory, &project, None, custody, None)?;
    assert_eq!(reopened.key_ref, owner.key_ref);
    assert_eq!(reopened.task, owner.task);
    let wrong = root.path().join("other-custody");
    for other in [
        SigningCustody::PrivateFile(&wrong),
        SigningCustody::Configured,
    ] {
        assert!(Owner::open(directory, &project, None, other, None).is_err());
    }
    assert!(bootstrap(SigningCustody::PrivateFile(&wrong)).is_err());
    assert!(bootstrap(SigningCustody::Configured).is_err());
    assert_eq!(fs::read(&path).map_err(storage)?, before);
    bootstrap(custody)?;
    Ok(())
}

/// Older owner formats refuse before decoding or credential access. Opening
/// and deleting share the same typed refusal and preserve the old bytes.
#[test]
fn older_state_directory_is_refused_without_migration() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let project = volume_project(root.path())?;
    let custody = root.path().join("signing-custody");
    for name in OLDER_RECORDS {
        let directory = super::directory(&root.path().join(name))?;
        let record = directory.join(name);
        let bytes = b"older owner format; must not reach a decoder";
        fs::write(&record, bytes).map_err(storage)?;
        let refusal = |error: Error| {
            assert!(matches!(
                error,
                Error::Conflict(message) if message == format!(
                    "local state at {} uses an older format; remove that directory to start fresh",
                    directory.display()
                )
            ));
        };
        match Owner::open(
            &directory,
            &project,
            None,
            SigningCustody::PrivateFile(&custody),
            None,
        ) {
            Ok(_) => return Err(Error::Invalid("an older owner format was adopted".into())),
            Err(error) => refusal(error),
        }
        refusal(Owner::remove(&directory, SigningCustody::PrivateFile(&custody)).unwrap_err());
        assert_eq!(fs::read(&record).map_err(storage)?, bytes);
        assert!(!directory.join(RECORD).exists());
        assert!(!custody.exists(), "refusal created signing custody");
    }
    Ok(())
}

/// A v3 owner can retain target/backup data from interrupted v5 preparation.
/// Admission must refuse the owner before lazy discovery or capture can import it.
#[tokio::test]
async fn legacy_native_preparation_refuses_before_checkout_attachment() -> Result<()> {
    use acyclic_native_runtime::signing_key::MemorySigningStore;

    let root = tempfile::tempdir().map_err(storage)?;
    let path = root.path().join("checkout");
    fs::create_dir(&path).map_err(storage)?;
    let checkout = Checkout::capture(&path)?;
    let preparation = path.join(format!(
        ".acyclic-r-{}",
        uuid::Uuid::from_bytes(checkout.root.identity().to_bytes()).simple()
    ));
    let operation = preparation.join(crate::identity::fresh_uuid().simple().to_string());
    for (name, bytes) in [
        ("target", b"pending".as_slice()),
        ("backup", b"preimage".as_slice()),
    ] {
        let directory = operation.join(name);
        fs::create_dir_all(&directory).map_err(storage)?;
        fs::write(directory.join("file"), bytes).map_err(storage)?;
    }
    let state = super::directory(&root.path().join("state"))?;
    let store = Arc::new(MemorySigningStore::new().map_err(storage)?);
    Owner::open(
        &state,
        &LocalProject::Workspace(path.clone()),
        Some(&checkout),
        SigningCustody::Store(store.as_ref()),
        None,
    )?;
    let old_record = state.join("owner.v4");
    fs::rename(state.join(RECORD), &old_record).map_err(storage)?;
    let original = fs::read(&old_record).map_err(storage)?;
    let config = LocalHarnessConfig::native_host(&path)
        .state_directory(&state)
        .signing_store(store.clone());
    assert!(matches!(
        LocalHarness::open(config).await,
        Err(Error::Conflict(message)) if message == format!(
            "local state at {} uses an older format; remove that directory to start fresh",
            state.display()
        )
    ));
    assert!(!state.join(RECORD).exists());
    assert!(!state.join("filesystem").exists());
    assert!(!state.join("workspace").exists());
    assert_eq!(fs::read(old_record).map_err(storage)?, original);
    assert_eq!(store.len(), 1, "refusal replaced the original signing seed");
    for (name, bytes) in [
        ("target", b"pending".as_slice()),
        ("backup", b"preimage".as_slice()),
    ] {
        assert_eq!(
            fs::read(operation.join(name).join("file")).map_err(storage)?,
            bytes
        );
    }
    Ok(())
}

#[cfg(windows)]
#[test]
fn store_deletion_resumes_both_before_and_after_owner_record_removal() -> Result<()> {
    use acyclic_native_runtime::signing_key::MemorySigningStore;
    use std::os::windows::fs::OpenOptionsExt as _;
    for hold_root in [false, true] {
        let root = tempfile::tempdir().map_err(storage)?;
        let directory = super::directory(&root.path().join("state"))?;
        let project = volume_project(root.path())?;
        let store = MemorySigningStore::new().map_err(storage)?;
        let custody = SigningCustody::Store(&store);
        Owner::open(&directory, &project, None, custody, None)?;
        let payload = directory.join("payload");
        fs::write(&payload, b"owned payload").map_err(storage)?;
        let held = if hold_root {
            // FILE_FLAG_BACKUP_SEMANTICS opens the directory itself. Sharing
            // read/write but not delete lets payload cleanup finish first.
            fs::OpenOptions::new()
                .read(true)
                .share_mode(3)
                .custom_flags(0x0200_0000)
                .open(&directory)
                .map_err(storage)?
        } else {
            fs::OpenOptions::new()
                .read(true)
                .share_mode(0)
                .open(&payload)
                .map_err(storage)?
        };
        assert!(Owner::remove(&directory, custody).is_err());
        assert_eq!(directory.join(RECORD).exists(), !hold_root);
        assert!(store.is_empty(), "failed cleanup retained signing custody");
        drop(held);
        Owner::remove(&directory, custody)?;
        assert!(!directory.exists());
    }
    Ok(())
}
