use super::*;
use crate::{
    Capabilities,
    context::native_skills::NativeSkills,
    core::{AggregateKind, AuthorityIssuer},
};
use futures::FutureExt as _;

#[tokio::test]
async fn concurrent_first_skill_bodies_share_one_journal_materializer() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    acyclic_fs::watch::initialize_native_watch_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let home = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let retained = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    for name in ["first", "second"] {
        let directory = home.path().join(name);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {name}\n---\n{name} body\n"),
        )
        .unwrap();
    }
    let provider = ProviderRef::new(
        "skill-race".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "journal".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let mut host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "skill-race",
        [3; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "owner".into(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]),
        Some(agent),
        None,
        &mut None,
    )?;
    let grant = ContentGrant::verify_directory_read(
        &issuer.verifier(&mut None)?,
        &scope,
        &volume,
        "inputs/skills",
        &mut None,
    )?;
    let sources = NativeSkills::new(
        retained.path().join("sources"),
        volume.clone(),
        volume.clone(),
        None,
        vec![home.path().into()],
    )?;
    let skills = sources.user_catalog().await?;
    assert_eq!(skills.len(), 2);
    host = host.with_native_skills(sources.clone())?;

    let (entered_a, at_a) = tokio::sync::oneshot::channel();
    let (entered_b, at_b) = tokio::sync::oneshot::channel();
    let (release_a, resume_a) = tokio::sync::oneshot::channel();
    let (release_b, resume_b) = tokio::sync::oneshot::channel();
    let pauses = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from([
        (entered_a, resume_a),
        (entered_b, resume_b),
    ])));
    Arc::get_mut(&mut host).unwrap().publication_pause = Some(Arc::new(move || {
        let (entered, resume) = pauses.lock().unwrap().pop_front().unwrap();
        Box::pin(async move {
            entered.send(()).unwrap();
            resume.await.unwrap();
        })
    }));
    let cancellation = acyclic_fs::CancellationToken::new();
    let work_a = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let work_b = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io_a = PreparationIo::from_native_work(&work_a)?;
    let io_b = PreparationIo::from_native_work(&work_b)?;
    let first = host.read_private_path(
        &volume,
        &grant,
        &skills[0].source.path,
        Some(&skills[0].source.generation),
        4096,
        Some(&io_a),
    );
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("first load completed before its pinned-transaction pause: {result:?}"),
        _ = at_a => {},
    }
    // An already-running reader and a replacement registration must still
    // share the journal owner and retained source directory.
    let replacement = NativeSkills::new(
        retained.path().join("replacement"),
        volume.clone(),
        volume.clone(),
        None,
        Vec::new(),
    )?;
    let rebound = host.clone().with_native_skills(replacement)?;
    let registered = rebound.skill_sources(&volume)?.unwrap();
    let second = rebound.read_private_path(
        &volume,
        &grant,
        &skills[1].source.path,
        Some(&skills[1].source.generation),
        4096,
        Some(&io_b),
    );
    tokio::pin!(second);
    // Poll the other task while the first is paused after pinning HEAD. With
    // serialization it waits for the owner. For the pre-fix negative control,
    // let it pin the same HEAD before allowing either publication to commit.
    assert!(futures::poll!(&mut second).is_pending());
    if registered.materializer.enter().now_or_never().is_some() {
        tokio::select! {
            result = &mut second => panic!("second load completed before its pause: {result:?}"),
            _ = at_b => {},
        }
    }
    release_a.send(()).unwrap();
    release_b.send(()).unwrap();
    let (first, second) = futures::join!(first, second);
    let (_, first) = first?;
    let (_, second) = second?;
    assert!(String::from_utf8_lossy(&first).contains("first body"));
    assert!(String::from_utf8_lossy(&second).contains("second body"));
    assert!(
        sources
            .materialized(&skills[0].source.generation)?
            .is_some()
    );
    assert!(
        sources
            .materialized(&skills[1].source.generation)?
            .is_some()
    );
    Ok(())
}
