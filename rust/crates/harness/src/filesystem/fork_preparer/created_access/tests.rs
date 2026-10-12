use super::*;
use crate::AgentId;
use crate::core::AggregateKind;

fn observation_work() -> Result<acyclic_fs::NativeMaterializationWork> {
    acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_048_576, 128 * 1_048_576),
        &CancellationToken::new(),
    )
    .map_err(|(cause, _)| Error::NativeWork {
        cause,
        custody: None,
    })
}

fn original_root_fixture_authority(
    agent: AgentId,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(
    (Authority, AuthorityIssuer, crate::core::Scope),
    acyclic_fs::NativeAllocation,
)> {
    use crate::store::history::HistoryIo as _;
    let configured_audience = Authority {
        kind: AggregateKind::Conversation,
        id: "genuine-original-root-bootstrap".into(),
    };
    let configured_issuer =
        AuthorityIssuer::new("created-owner", [80; 32], configured_audience.clone());
    let mut metadata = crate::store::history::native_work_io(work).map_err(Error::from)?;
    metadata.retain_owned(
        std::mem::size_of::<(Authority, AuthorityIssuer, crate::core::Scope)>() as u64,
    )?;
    let audience = metadata.clone_authority(&configured_audience)?;
    let issuer = configured_issuer.copy(&mut Some(&mut metadata))?;
    let scope = issuer.issue(
        "genuine-original-root-bootstrap",
        &Capabilities::new(["conversation:bind"]),
        Some(agent),
        None,
        &mut Some(&mut metadata),
    )?;
    let custody = metadata.into_custody();
    Ok(((audience, issuer, scope), custody))
}

async fn bind_genuine_original_root(
    fixture: &crate::durable_host::tests::created_resource_access::CreationFixture,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<crate::durable_host::TaskJournalOwner<acyclic_stream::MemoryStream>> {
    let owner = fixture
        .host
        .journal_owner(fixture.root, (&fixture.root_lease.reservation).into())
        .await?;
    let agent = owner
        .authenticated_binding_agent(&mut Some(&mut crate::store::history::native_work_io(work)?))?
        .ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "fixture has no original Root actor",
        })?;
    let (authority, custody) = original_root_fixture_authority(agent, work)?;
    let scratch = &fixture
        .original_root_births
        .as_ref()
        .expect("real pre-admission births")[1];
    crate::filesystem::bind_original_root_conversation_with_native_work(
        &fixture.stream,
        authority,
        custody,
        &owner,
        &scratch.volume,
        scratch.operation,
        fixture.root_scope.limits(),
        work,
    )
    .await?;
    Ok(owner)
}

use crate::durable_host::tests::created_resource_access::CreationFixture;
use crate::resource_preparation::RootBootstrapContentRole;
use crate::source_authorization::{
    ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
};

#[tokio::test]
async fn source_requires_its_current_protected_original_root_record() -> Result<()> {
    let work = observation_work()?;
    let fixture = CreationFixture::new_with_original_root_births(&work).await?;
    let creator = &fixture.filesystem_creator;
    let birth = &fixture.original_root_births.as_ref().unwrap()[0];
    let owner = bind_genuine_original_root(&fixture, &work).await?;
    let token = creator
        .protect_root_bootstrap_with_native_work(
            &owner,
            &birth.volume,
            RootBootstrapContentRole::Project,
            birth.operation,
            birth.config,
            birth.initial_generation,
            birth.selected_generation,
            None,
            &work,
        )?
        .await?
        .into_native_shared(&work)?;
    let admission_hold;
    let (admission, hold) = owner
        .original_root_admission_with_native_work(&work)
        .await?;
    admission_hold = hold;
    let bare = crate::filesystem::root_source::FsRootProjectSourceProvider::new(
        fixture.filesystem.clone(),
        creator.issuer.audience(),
        fixture.root,
        Some(&PreparationIo::from_native_work(&work)?),
    )?;
    let selection = ProjectSourceSnapshot {
        project: birth.volume.clone(),
        generation: token.request().generation.clone(),
        native_snapshot: None,
    };
    let context = || RootProjectSourceContext {
        owner: creator.issuer.audience(),
        caller: &admission,
        source: &admission,
        graph_root: fixture.root,
        operation_id: OperationId::from_bytes([183; 16]),
        maximum_work: 1_048_576,
        maximum_bytes: 1_048_576,
        cancellation: work.cancellation(),
        created_project_authenticated: false,
    };
    assert!(
        match bare.verify_with_native_work(context(), &selection, &work) {
            Ok(future) => future.await.is_err(),
            Err(_) => true,
        }
    );
    let policy = bare.with_bootstrap_source_with_native_work(token, &work)?;
    policy
        .verify_with_native_work(context(), &selection, &work)?
        .await?;
    let initial = ProjectSourceSnapshot {
        project: selection.project.clone(),
        generation: GenerationRef::new(
            &fixture.filesystem_provider,
            birth.initial_generation.digest().as_bytes(),
            None,
            None,
        )?.0,
        native_snapshot: None,
    };
    assert!(
        match policy.verify_with_native_work(context(), &initial, &work) {
            Ok(future) => future.await.is_err(),
            Err(_) => true,
        }
    );
    let journal = allocation_ref(&creator.host.provider, &birth.volume, None)?;
    let generation = fixture.filesystem.resolve(&journal).await?.generation;
    fixture
        .filesystem
        .apply(
            &journal,
            Some(&generation),
            &[WorkspaceMutation::PutFile {
                path: "/root-bootstrap.json".into(),
                bytes: b"superseded protected bootstrap".to_vec().into(),
            }],
            &IdempotencyKey::new("supersede-genuine-original-root-bootstrap")?,
        )
        .await?;
    assert!(
        policy
            .verify_with_native_work(context(), &selection, &work)?
            .await
            .is_err()
    );
    drop(admission);
    drop(admission_hold);
    Ok(())
}
