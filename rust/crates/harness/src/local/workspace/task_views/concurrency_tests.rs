use super::super::owner::ViewOwner;
use super::*;
use futures::channel::oneshot;

struct GatedOwner {
    gate: std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

impl ViewOwner for GatedOwner {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        let gate = self.gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.await.map_err(storage)?;
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn blocked_child_binding_keeps_sibling_and_root_independent() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    std::fs::write(checkout.path().join("source.txt"), b"inherited source").map_err(storage)?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(
            checkout.path(),
            (
                crate::model::Model::new("scripted".into(), "concurrency".into(), "1".into())?,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ),
        )?
        .state_directory(state.path())
        .workspace_mounting(WorkspaceMounting::Physical)
        .without_default_tools(),
    )
    .await?;
    let root = harness.workspace.as_ref().unwrap();
    let cancellation = CancellationToken::new();
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let mut forks = Vec::new();
    for index in 0..2_u8 {
        let project = VolumeRef::new(
            harness.project.provider().clone(),
            format!("child-{index}"),
            VolumeClass::Project,
            crate::conversation::VolumeOwner::Project("concurrency".into()),
            &mut None,
        )?;
        let (view, _) = root
            .view()
            .fork(
                &project.storage_name(&mut None)?,
                acyclic_fs::IdempotencyKey::from_bytes([index + 1; 16]),
                acyclic_fs::PublicationPermit::Unrestricted,
                &mut accounting,
            )
            .await
            .map_err(storage)?;
        forks.push((project, view));
    }
    let mut capabilities = harness
        .signed
        .capabilities()
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for (project, _) in &forks {
        for operation in [
            crate::conversation::VolumeOperation::Read,
            crate::conversation::VolumeOperation::Write,
        ] {
            capabilities.push(project.capability(operation, &mut None)?);
        }
    }
    let signed = harness
        .root_authority
        .as_ref()
        .expect("signing custody")
        .bootstrap_scope(
            "child-bindings",
            &crate::Capabilities::new(capabilities),
            None,
            &mut None,
        )?;
    let authority =
        crate::filesystem::NativeMountAuthority::admitted(harness.verifier.clone(), signed);
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let blocked = GatedOwner {
        gate: std::sync::Mutex::new(Some((entered_tx, release_rx))),
    };
    let ready = GatedOwner {
        gate: std::sync::Mutex::new(None),
    };
    let first = root.bound_task_view(
        root.spawned_project_view(&forks[0].0).await?,
        &forks[0].0,
        &[],
        &blocked,
        &authority,
    );
    let independent = async {
        entered.await.map_err(storage)?;
        // Child initialization must leave root commands available.
        assert!(
            root.state.try_lock().is_ok(),
            "child initialization locked root commands"
        );
        let sibling = root
            .bound_task_view(
                root.spawned_project_view(&forks[1].0).await?,
                &forks[1].0,
                &[],
                &ready,
                &authority,
            )
            .await?;
        assert_eq!(
            std::fs::read(sibling.path.join("source.txt")).map_err(storage)?,
            b"inherited source"
        );
        assert!(root.state.try_lock().is_ok());
        release.send(()).unwrap();
        Ok::<_, Error>(sibling)
    };
    let same = root.bound_task_view(
        root.spawned_project_view(&forks[0].0).await?,
        &forks[0].0,
        &[],
        &ready,
        &authority,
    );
    let (first, sibling, concurrent) = futures::try_join!(first, independent, same)?;
    assert!(
        Arc::ptr_eq(&first, &concurrent),
        "concurrent initializers duplicated the capture owner"
    );
    assert_ne!(first.path, sibling.path);
    let again = root
        .bound_task_view(
            root.spawned_project_view(&forks[0].0).await?,
            &forks[0].0,
            &[],
            &ready,
            &authority,
        )
        .await?;
    assert!(
        Arc::ptr_eq(&first, &again),
        "same project acquired another capture owner"
    );
    assert_eq!(
        std::fs::read(first.path.join("source.txt")).map_err(storage)?,
        b"inherited source"
    );
    drop((first, again, concurrent, sibling));

    // Cancellation before publication must release this key, so a subsequent
    // initializer can own it without a scan, retry loop or sticky failed cell.
    let (entered_tx, entered) = oneshot::channel();
    let (_release, release_rx) = oneshot::channel();
    let cancelled_owner = GatedOwner {
        gate: std::sync::Mutex::new(Some((entered_tx, release_rx))),
    };
    {
        let initialization = root.bound_task_view(
            root.spawned_project_view(&forks[0].0).await?,
            &forks[0].0,
            &[],
            &cancelled_owner,
            &authority,
        );
        tokio::pin!(initialization);
        tokio::select! {
            result = &mut initialization => panic!("initializer ended before its owner event; success={}", result.is_ok()),
            result = entered => result.map_err(storage)?,
        }
    }
    let recovered = root
        .bound_task_view(
            root.spawned_project_view(&forks[0].0).await?,
            &forks[0].0,
            &[],
            &ready,
            &authority,
        )
        .await?;
    assert_eq!(
        std::fs::read(recovered.path.join("source.txt")).map_err(storage)?,
        b"inherited source"
    );
    drop(recovered);
    Ok(())
}
