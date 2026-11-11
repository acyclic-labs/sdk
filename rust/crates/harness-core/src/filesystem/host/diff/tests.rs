use super::*;
use crate::{
    conversation::{VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityRoot},
};
use acyclic_fs::{CancellationToken, Fs, WorkBudget};
use futures::TryStreamExt as _;

#[tokio::test]
async fn changed_path_queries_suspend_at_each_row_and_keep_the_original_cut() -> Result<()> {
    let provider = ProviderRef::new("diff".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("diff".into()),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let cancellation = CancellationToken::new();
    let workspace = host
        .filesystem
        .open_workspace(
            volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let base = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    for index in 0..32 {
        workspace
            .write_text(&format!("/file-{index:02}"), "original")
            .await?;
    }
    let current = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let base = host.generation_ref_id(base.id(), None)?;
    let current = host.generation_ref_id(current.id(), None)?;
    let issuer = AuthorityRoot::new(
        "diff",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "diff".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "reader",
        &crate::Capabilities::new([volume.directory_read_capability("", &mut None)?]),
        Some(crate::AgentId::new()),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let grant = ContentGrant::verify_directory_read(&verifier, &scope, &volume, "", &mut None)?;
    let root = ContextRoot {
        volume: volume.clone(),
        directory: String::new(),
    };
    let roots = [(&root, &grant)];
    let io = PreparationIo::for_unmeasured(&cancellation)?;
    let first_work;
    {
        let steps = host.diff_private_steps(&roots, (&base, &current), &io, true);
        futures::pin_mut!(steps);
        assert_eq!(
            io.receipt(),
            Default::default(),
            "unpolled query performs no IO"
        );
        let first = steps.try_next().await?.expect("first changed row");
        assert_eq!(first.len(), 1);
        first_work = io.receipt();
        workspace
            .write_text("/later", "outside the selected cut")
            .await?;
        let mut names = vec![first[0].1.path.clone()];
        while let Some(step) = steps.try_next().await? {
            names.extend(step.into_iter().map(|(_, path)| path.path));
        }
        names.sort();
        assert_eq!(
            names,
            (0..32)
                .map(|index| format!("file-{index:02}"))
                .collect::<Vec<_>>()
        );
    }
    let complete_work = io.receipt();
    assert!(
        complete_work.items_examined > first_work.items_examined,
        "first row must leave the changed suffix unvisited"
    );
    let io = PreparationIo::for_unmeasured(&cancellation)?;
    {
        let steps = host.diff_private_steps(&roots, (&base, &current), &io);
        futures::pin_mut!(steps);
        assert!(steps.try_next().await?.is_some());
    }
    assert!(
        io.receipt().items_examined < complete_work.items_examined,
        "dropping a partially consumed query must not drain its suffix"
    );
    Ok(())
}
