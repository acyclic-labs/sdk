use super::*;
use crate::conversation::{VolumeClass, VolumeOwner};
use crate::core::{AggregateKind, AuthorityIssuer};
use crate::filesystem::workspace_ref;
use acyclic_fs::Fs;

#[tokio::test]
async fn dropping_pending_completion_releases_its_frame() -> Result<()> {
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(100_000, 16_000_000),
        &CancellationToken::new(),
    )?;
    let before = io.owned_bytes()?;
    let future = io.history_future(|| async {
        std::future::pending::<()>().await;
        Ok(())
    })?;
    assert!(io.owned_bytes()? > before);
    let mut completion = Box::pin(io.finish_future(future));
    assert!(futures::poll!(&mut completion).is_pending());
    drop(completion);
    assert_eq!(io.owned_bytes()?, before);
    Ok(())
}

#[tokio::test]
async fn finite_ordinary_owner_callback_retains_full_failure_and_real_sdk_prefix() -> Result<()> {
    let provider = crate::resources::ProviderRef::new(
        "ordinary-owner-control".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
    let volume = VolumeRef::new(
        provider,
        "owned",
        VolumeClass::Project,
        VolumeOwner::Project("ordinary-owner-control".into()),
    )?;
    let initial = host.create_volume(&volume).await?;
    let mutations = [WorkspaceMutation::PutFile {
        path: "/file".into(),
        bytes: b"owned".to_vec(),
    }];
    let cancellation = CancellationToken::new();
    let denied_io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(100_000, 16_000_000),
        &cancellation,
    )?;
    let denied = publish_generation(
        &host,
        &initial.workspace,
        Some(&initial.generation),
        &mutations,
        &IdempotencyKey::new("expired-owner-publication")?,
        Some(&denied_io),
        &mut |budget| {
            let host = &host;
            let initial = &initial;
            let cancellation = &cancellation;
            async move {
                let callback = PreparationIo::from_budget(budget, cancellation)
                    .map_err(OperationFailure::before_work)?;
                let result = async {
                    let _checked = open(&host, &initial.workspace, &callback).await?;
                    Err(Error::Indeterminate(crate::OperationId::from_bytes(
                        [113; 16],
                    )))
                }
                .await;
                callback.finish(result)
            }
        },
    )
    .await;
    assert!(
        matches!(denied, Err(Error::Indeterminate(id)) if id == crate::OperationId::from_bytes([113; 16]))
    );
    assert_eq!(
        host.resolve(&initial.workspace).await?.generation,
        initial.generation
    );
    let accepted_io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(100_000, 16_000_000),
        &cancellation,
    )?;
    let accepted = publish_generation(
        &host,
        &initial.workspace,
        Some(&initial.generation),
        &mutations,
        &IdempotencyKey::new("live-owner-publication")?,
        Some(&accepted_io),
        &mut |budget| {
            let host = &host;
            let initial = &initial;
            let cancellation = &cancellation;
            async move {
                let callback = PreparationIo::from_budget(budget, cancellation)
                    .map_err(OperationFailure::before_work)?;
                let result = open(&host, &initial.workspace, &callback).await.map(|_| ());
                callback.finish(result)
            }
        },
    )
    .await?;
    assert_eq!(
        host.read(
            &initial.workspace,
            Some(&host.generation_ref(&accepted)?),
            "/file",
            32
        )
        .await?
        .as_ref(),
        b"owned"
    );
    Ok(())
}

#[tokio::test]
async fn exact_read_gate_precedes_backend_and_pinned_fork_never_writes_back() -> Result<()> {
    let provider = crate::resources::ProviderRef::new(
        "source-read-control".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
    let project = |name: &str| {
        VolumeRef::new(
            provider.clone(),
            name,
            VolumeClass::Project,
            VolumeOwner::Project("source-read-control".into()),
        )
    };
    let source = project("selected")?;
    let caller = project("caller")?;
    let child = project("child")?;
    let denied_child = project("denied-child")?;
    let initial = host.create_volume(&source).await?;
    let caller_head = host.create_volume(&caller).await?;
    let selected_generation = host
        .apply(
            &initial.workspace,
            Some(&initial.generation),
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"selected".to_vec(),
            }],
            &IdempotencyKey::new("selected-generation")?,
        )
        .await?;
    let wrong_generation = host
        .apply(
            &caller_head.workspace,
            Some(&caller_head.generation),
            &[WorkspaceMutation::PutFile {
                path: "/other.txt".into(),
                bytes: b"other-source".to_vec(),
            }],
            &IdempotencyKey::new("other-source-generation")?,
        )
        .await?;
    let issuer = AuthorityIssuer::new(
        "source-read-control",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "caller".into(),
        },
    );
    let scope = issuer
        .issue(
            "exact-project-read",
            &crate::Capabilities::new([
                source.capability(VolumeOperation::Read, &mut None)?,
                caller.capability(VolumeOperation::Read, &mut None)?,
            ]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let selected_read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &source,
        VolumeOperation::Read,
        MAX_CAPTURE_BYTES,
        &mut None,
    )?;
    let caller_read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &caller,
        VolumeOperation::Read,
        MAX_CAPTURE_BYTES,
        &mut None,
    )?;
    assert!(matches!(
        fork(
            &host,
            &source,
            &caller_read,
            &selected_generation,
            &denied_child,
            None,
            &IdempotencyKey::new("denied-source-read")?,
            None,
            None,
            #[cfg(not(target_arch = "wasm32"))]
            None
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let denied_ref = workspace_ref(provider.clone(), &denied_child.storage_name(&mut None)?)?;
    assert!(host.resolve(&denied_ref).await.is_err());
    assert!(
        fork(
            &host,
            &source,
            &selected_read,
            &wrong_generation,
            &denied_child,
            None,
            &IdempotencyKey::new("wrong-generation")?,
            None,
            None,
            #[cfg(not(target_arch = "wasm32"))]
            None
        )
        .await
        .is_err()
    );
    assert!(host.resolve(&denied_ref).await.is_err());
    let later_generation = host
        .apply(
            &initial.workspace,
            Some(&selected_generation),
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"later".to_vec(),
            }],
            &IdempotencyKey::new("later-generation")?,
        )
        .await?;
    let forked = fork(
        &host,
        &source,
        &selected_read,
        &selected_generation,
        &child,
        None,
        &IdempotencyKey::new("pinned-selected-fork")?,
        None,
        None,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )
    .await?;
    assert_eq!(
        host.read(
            &forked.workspace,
            Some(&forked.generation),
            "/value.txt",
            32
        )
        .await?
        .as_ref(),
        b"selected"
    );
    host.apply(
        &forked.workspace,
        Some(&forked.generation),
        &[WorkspaceMutation::PutFile {
            path: "/value.txt".into(),
            bytes: b"child".to_vec(),
        }],
        &IdempotencyKey::new("isolated-child-edit")?,
    )
    .await?;
    assert_eq!(
        host.resolve(&initial.workspace).await?.generation,
        later_generation
    );
    assert_eq!(
        host.read(&initial.workspace, None, "/value.txt", 32)
            .await?
            .as_ref(),
        b"later"
    );
    assert_eq!(
        host.read(
            &initial.workspace,
            Some(&selected_generation),
            "/value.txt",
            32
        )
        .await?
        .as_ref(),
        b"selected"
    );
    Ok(())
}
