use super::*;

#[tokio::test]
async fn retained_records_share_one_sync_on_the_native_io_service() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    acyclic_fs::watch::initialize_native_watch_services().map_err(storage)?;
    let root = tempfile::tempdir().map_err(storage)?;
    let skill = root.path().join("skills/greet2/SKILL.md");
    std::fs::create_dir_all(skill.parent().unwrap()).map_err(storage)?;
    std::fs::write(
        &skill,
        "---\nname: greet2\ndescription: Greet formally\n---\nbody",
    )
    .map_err(storage)?;
    std::fs::create_dir_all(root.path().join("skills/other")).map_err(storage)?;
    std::fs::write(
        root.path().join("skills/other/SKILL.md"),
        "---\nname: other\ndescription: Another skill\n---\nbody",
    )
    .map_err(storage)?;
    let provider = crate::resources::ProviderRef::new(
        "retained-record".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider,
        "journal".into(),
        crate::conversation::VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
        &mut None,
    )?;
    let sources = NativeSkills::new(
        root.path().join("retained"),
        volume,
        None,
        vec![root.path().join("skills")],
    )?;
    let skills = sources.user_catalog().await?;
    assert_eq!(skills.len(), 2);
    let mut records = RetainedBatch::default();
    let mut paths = Vec::new();
    for skill in &skills {
        let digest = hex::encode(skill.source.generation.as_resource().key());
        let path = sources.directory.join(format!("{digest}.json"));
        let record = retained::read_record(&std::fs::read(&path).map_err(storage)?, &digest)?;
        assert_eq!(records.retain(&sources.journal, record)?, skill.source);
        std::fs::remove_file(&path).map_err(storage)?;
        paths.push(path);
    }
    let runtime_thread = std::thread::current().id();
    let syncs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&syncs);
    records
        .publish_with(sources.directory.clone(), move |file| {
            assert_ne!(
                std::thread::current().id(),
                runtime_thread,
                "retained metadata sync blocked the runtime thread"
            );
            assert!(
                std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("acyclic-native-io-")),
                "retained metadata sync bypassed the native I/O service"
            );
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            file.sync_all().map_err(storage)
        })
        .await?;
    assert_eq!(syncs.load(std::sync::atomic::Ordering::SeqCst), 1);
    let identities = paths
        .iter()
        .map(|path| {
            let file = std::fs::File::open(path).map_err(storage)?;
            acyclic_fs::NativeRootIdentity::from_file(&file).map_err(storage)
        })
        .collect::<Result<Vec<_>>>()?;
    let [first, second] = identities.as_slice() else {
        return Err(Error::Invalid("expected two record files".into()));
    };
    assert_eq!(
        first, second,
        "retained records must share the synced inode"
    );
    for skill in &skills {
        let digest = hex::encode(skill.source.generation.as_resource().key());
        let bytes =
            std::fs::read(sources.directory.join(format!("{digest}.json"))).map_err(storage)?;
        retained::read_record(&bytes, &digest)?;
    }
    Ok(())
}
