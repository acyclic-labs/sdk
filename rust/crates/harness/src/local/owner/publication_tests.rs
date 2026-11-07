//! Owner record v2 publication, custody selection and older-format refusal.
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
/// Owners in fresh state directories share its one root entry; another store
/// never stands in for it.
#[test]
fn explicit_file_custody_reopens_owners_from_one_root_without_native_fallback() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let selected = root.path().join("signing-custody");
    let custody = SigningCustody::PrivateFile(&selected);
    let project = volume_project(root.path())?;
    let directories = (0..3)
        .map(|index| super::directory(&root.path().join(format!("state-{index}"))))
        .collect::<Result<Vec<_>>>()?;
    let owners = directories
        .iter()
        .map(|directory| Owner::open(directory, &project, None, custody))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        fs::read_dir(&selected).map_err(storage)?.count(),
        1,
        "owners added signing entries"
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
    let reopened = Owner::open(directory, &project, None, custody)?;
    assert_eq!(reopened.key_ref, owner.key_ref);
    assert_eq!(reopened.task, owner.task);
    let wrong = root.path().join("other-custody");
    assert!(bootstrap(SigningCustody::PrivateFile(&wrong)).is_err());
    assert!(bootstrap(SigningCustody::Configured).is_err());
    assert_eq!(fs::read(&path).map_err(storage)?, before);
    bootstrap(custody)?;
    Ok(())
}

/// A v1 state directory is refused with one actionable error and left as is.
#[test]
fn older_state_directory_is_refused_without_migration() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let directory = super::directory(&root.path().join("state"))?;
    fs::write(directory.join(V1_RECORD), b"v1").map_err(storage)?;
    let project = volume_project(root.path())?;
    let custody = root.path().join("signing-custody");
    let error = match Owner::open(&directory, &project, None, SigningCustody::PrivateFile(&custody)) {
        Ok(_) => return Err(Error::Invalid("a v1 state directory was adopted".into())),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains(&format!(
            "local state at {} uses an older format; remove that directory to start fresh",
            directory.display()
        )),
        "{error}"
    );
    assert!(!directory.join(RECORD).exists());
    assert!(!custody.exists(), "refusal created signing custody");
    Ok(())
}
