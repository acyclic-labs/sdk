use super::*;

#[tokio::test]
async fn retained_records_are_written_off_the_runtime_thread() -> Result<()> {
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
    let [skill] = skills.as_slice() else {
        return Err(Error::Invalid("expected one retained skill".into()));
    };
    let path = sources.directory.join(format!(
        "{}.json",
        hex::encode(skill.source.generation.as_resource().key())
    ));
    let record =
        serde_json::from_slice(&std::fs::read(&path).map_err(storage)?).map_err(storage)?;
    std::fs::remove_file(&path).map_err(storage)?;
    let runtime_thread = std::thread::current().id();
    let retained = sources
        .retain_record_with(&record, move |directory, bytes, digest| {
            assert_ne!(
                std::thread::current().id(),
                runtime_thread,
                "retained metadata write blocked the runtime thread"
            );
            write_retained_record(directory, bytes, digest)
        })
        .await?;
    assert_eq!(retained, skill.source);
    assert!(path.is_file());
    Ok(())
}
