//! Harness owner custody uses the shared private-file publisher and reader.
//! Publication cuts and commit-only durability are covered by that primitive's
//! real filesystem controls; these tests cover harness identity and custody.
use super::*;

#[test]
fn private_legacy_owner_migrates_once_without_changing_its_original_identity() {
    let directory = tempfile::tempdir().unwrap();
    let private = directory.path().join("private");
    acyclic_native_runtime::private_file::create_directory_all(&private).unwrap();
    let path = private.join("owner.json");
    let key = vec![71_u8; 32];
    let owner: Owner = serde_json::from_value(serde_json::json!({
        "key": key, "agent": AgentId::from_bytes([72; 16]),
        "task": TaskId::from_bytes([73; 16]),
        "project": VolumeRef::new(
            crate::resources::ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            "project".into(), VolumeClass::Project, VolumeOwner::Project("project".into()), &mut None).unwrap(),
        "workspace": null, "workspace_identity": null, "workspace_incarnation": null
    })).unwrap();
    #[cfg(not(windows))]
    let mut file = acyclic_native_runtime::private_file::create(&path).unwrap();
    // Reproduce the former writer's inherited Windows DACL, not the new
    // private writer. The strict reader is the old rejected-upgrade control.
    #[cfg(windows)]
    let mut file = fs::File::create(&path).unwrap();
    let mut legacy = serde_json::to_value(&owner).unwrap();
    legacy.as_object_mut().unwrap().remove("key_ref");
    legacy
        .as_object_mut()
        .unwrap()
        .insert("key".into(), serde_json::json!(vec![71_u8; 32]));
    serde_json::to_writer(&mut file, &legacy).unwrap();
    drop(file);
    assert_eq!(fs::read(&path).unwrap().first(), Some(&b'{')); // Old plaintext reader control.
    #[cfg(windows)]
    assert!(acyclic_native_runtime::private_file::read(&path, 65_536).is_err());
    let before = fs::read(&path).unwrap();
    let explicit = private.join("explicit-custody");
    assert!(Owner::load(&path, SigningCustody::PrivateFile(&explicit)).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(
        !explicit.exists(),
        "explicit mode imported an ambient legacy seed"
    );
    let migrated = Owner::load(&path, SigningCustody::Configured).unwrap();
    assert!(migrated.key.is_none());
    assert!(
        migrated
            .key_ref
            .unwrap()
            .with_seed(SigningCustody::Configured, |seed| *seed == [71; 32])
            .unwrap()
    );
    assert_eq!(migrated.agent, owner.agent);
    assert_eq!(migrated.task, owner.task);
    let protected = fs::read(&path).unwrap();
    assert!(protected.starts_with(acyclic_native_runtime::owner_record::MAGIC));
    let plain =
        acyclic_native_runtime::owner_record::decode_with(&protected, 65_536, None, |bytes| {
            Ok::<_, std::io::Error>(bytes.to_vec())
        })
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&plain).unwrap();
    assert!(
        metadata.get("key").is_none(),
        "migration retained a plaintext seed"
    );
    assert!(metadata.get("key_ref").is_some());
    let reopened = Owner::load(&path, SigningCustody::Configured).unwrap();
    assert_eq!(reopened.key_ref, migrated.key_ref);
    assert_eq!(fs::read(&path).unwrap(), protected);
}

#[test]
fn concurrent_owner_migration_cannot_publish_another_callers_provisional_key() -> Result<()> {
    use acyclic_native_runtime::{
        FileLock, FileLockMode, owner_record, private_file, signing_key::SigningKeyReference,
    };
    // The native custody regression owns the shared-identity old/new control.
    // This caller also keeps owner-record publication itself serialized.
    {
        let directory = tempfile::tempdir().map_err(storage)?;
        let path = directory.path().join("owner.json");
        let owner = Owner {
            key: Some(owner_record::new_key().map_err(storage)?),
            key_ref: None,
            agent: AgentId::new(),
            task: TaskId::new(),
            project: VolumeRef::new(
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
            workspace: None,
            workspace_identity: None,
            workspace_incarnation: None,
        };
        let seed = owner
            .key
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing fixture seed".into()))?;
        let mut legacy =
            serde_json::to_value(&owner).map_err(|error| Error::Invalid(error.to_string()))?;
        legacy
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("fixture owner is not an object".into()))?
            .insert(
                "key".into(),
                serde_json::to_value(seed).map_err(|error| Error::Invalid(error.to_string()))?,
            );
        serde_json::to_writer(private_file::create(&path).map_err(storage)?, &legacy)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let lock = FileLock::acquire(
            private_file::open_or_create(&path.with_extension("lock")).map_err(storage)?,
            FileLockMode::Exclusive,
        )
        .map_err(storage)?;
        let first =
            SigningKeyReference::retain(seed, SigningCustody::Configured).map_err(storage)?;
        let first_ref = first.reference();
        TRY_OWNER_LOCK.set(true);
        let contender = Owner::load(&path, SigningCustody::Configured);
        TRY_OWNER_LOCK.set(false);
        assert!(
            matches!(contender, Err(Error::Storage(_))),
            "contender bypassed publication ownership"
        );
        assert_eq!(fs::read(&path).map_err(storage)?.first(), Some(&b'{'));
        drop(first);
        assert!(
            first_ref
                .with_seed(SigningCustody::Configured, |_| ())
                .is_err()
        );
        drop(lock);
        {
            let winner = Owner::load(&path, SigningCustody::Configured)?;
            let reference = winner
                .key_ref
                .ok_or_else(|| Error::Invalid("missing published key".into()))?;
            assert!(
                reference
                    .with_seed(SigningCustody::Configured, |stored| stored == seed)
                    .map_err(storage)?
            );
            assert_eq!(
                Owner::load(&path, SigningCustody::Configured)?.key_ref,
                Some(reference)
            );
        }
    }
    Ok(())
}

/// The same selected file store owns creation, reopening and authority bootstrap.
/// Wrong-store reopening must fail without touching the published owner.
#[test]
fn explicit_file_custody_reopens_the_original_owner_without_native_fallback() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let directory = super::directory(&root.path().join("state"))?;
    let selected = directory.join("signing-custody");
    let custody = SigningCustody::PrivateFile(&selected);
    let project = LocalProject::Volume {
        directory: root.path().to_owned(),
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
    };
    let owner = Owner::open(&directory, &project, None, custody)?;
    let path = directory.join("owner.json");
    let before = fs::read(&path).map_err(storage)?;
    let reference = owner
        .key_ref
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing original reference".into()))?;
    let audience = crate::core::Authority {
        kind: crate::core::AggregateKind::Task,
        id: owner.task.to_string(),
    };
    let _issuer =
        crate::core::AuthorityRoot::bootstrap("acyclic.local", reference, custody, audience)?;
    let reopened = Owner::open(&directory, &project, None, custody)?;
    assert_eq!(reopened.key_ref, owner.key_ref);
    assert_eq!(reopened.task, owner.task);
    let wrong = directory.join("other-custody");
    assert!(
        Owner::open(
            &directory,
            &project,
            None,
            SigningCustody::PrivateFile(&wrong)
        )
        .is_err()
    );
    assert!(Owner::open(&directory, &project, None, SigningCustody::Configured).is_err());
    assert_eq!(fs::read(&path).map_err(storage)?, before);
    assert_eq!(
        Owner::open(&directory, &project, None, custody)?.key_ref,
        owner.key_ref
    );
    Ok(())
}
