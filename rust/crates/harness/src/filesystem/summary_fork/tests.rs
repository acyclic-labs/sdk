use super::*;

#[test]
fn rewritten_context_retains_only_the_replacement_file() -> Result<()> {
    let provider = crate::resources::ProviderRef::new(
        "summary-budget".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    let source = FileRef::new(
        volume.clone(),
        "original.txt".into(),
        "1".repeat(64),
        FileDescriptor::from_bytes(b"payload", "text/plain", &mut None)?,
        "original.txt".into(),
        &mut None,
    )?;
    let replacement = FileRef::new(
        volume,
        "replacement.txt".into(),
        "2".repeat(64),
        source.descriptor().clone(),
        "replacement.txt".into(),
        &mut None,
    )?;
    let key = crate::contract::canonical_json_digest(&source, u64::MAX, &mut None)?;
    let mut context = Context::default();
    context.metadata.insert("payload".into(), source);
    let replacements = BTreeMap::from([(key, replacement)]);
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(10_000, 1_000_000),
        &cancellation,
    )?;
    let io = PreparationIo::from_native_work(&work)?;
    let rewritten = rewritten_context(&context, &replacements, Some(&io))?;
    assert_eq!(rewritten.metadata["payload"], replacements[&key]);
    let expected = PreparationIo::from_native_work(&work)?;
    let _copy = crate::context::copy_context(&rewritten, Some(&expected))?;
    assert_eq!(io.owned_bytes()?, expected.owned_bytes()?);
    Ok(())
}
