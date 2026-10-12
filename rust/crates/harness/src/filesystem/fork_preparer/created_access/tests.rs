use super::*;
use crate::AgentId;
use crate::conversation::{VolumeClass, VolumeOwner};
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
    let authority_custody;
    let (authority, custody) = original_root_fixture_authority(agent, work)?;
    authority_custody = custody;
    let scratch = &fixture
        .original_root_births
        .as_ref()
        .expect("real pre-admission births")[1];
    crate::filesystem::bind_original_root_conversation_with_native_work(
        &fixture.stream,
        authority,
        authority_custody,
        &owner,
        &scratch.volume,
        scratch.operation,
        fixture.root_scope.limits(),
        work,
    )
    .await?;
    Ok(owner)
}

#[tokio::test]
async fn real_original_root_births_bind_readonly_project_and_actual_private_output() -> Result<()> {
    use crate::resource_preparation::{
        CreatedResourceAccess, RootBootstrapContentContext, RootBootstrapContentRole,
    };
    use crate::source_authorization::{
        ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
    };
    let work = observation_work()?;
    let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
    let owner = bind_genuine_original_root(&fixture, &work).await?;
    let creator = &fixture.filesystem_creator;
    for (birth, role) in fixture.original_root_births.as_ref().unwrap().iter().zip([
        RootBootstrapContentRole::Project,
        RootBootstrapContentRole::Scratch,
    ]) {
        let token = creator
            .protect_root_bootstrap_with_native_work(
                &owner,
                &birth.volume,
                role,
                birth.operation,
                birth.config,
                birth.initial_generation,
                birth.selected_generation,
                None,
                &work,
            )?
            .await?
            .into_native_shared(&work)?;
        creator.bind_root_bootstrap_allocation_with_native_work(token, &work)?;
    }
    let [project, scratch] = creator.bound_root_bootstrap_allocations(&work)?;
    let admission_custody;
    let (admission, custody) = owner
        .original_root_admission_with_native_work(&work)
        .await?;
    admission_custody = custody;
    let context = RootBootstrapContentContext::from_verified_root_with_native_work(
        &owner,
        &admission,
        project.witness.binding(),
        &work,
    )
    .await?;
    let mut accesses = Vec::new();
    for token in [project, scratch] {
        let (receipt, custody) = creator
            .verify_root_bootstrap_with_native_work(context, token.request(), &work)?
            .await?;
        accesses.push(
            CreatedResourceAccess::consume_root_bootstrap_with_native_work(
                context,
                token.request(),
                receipt,
                custody,
                &work,
            )?,
        );
    }
    let scratch_access = accesses.pop().unwrap();
    let access = accesses
        .pop()
        .unwrap()
        .merge_root_bootstrap_with_native_work(scratch_access, &work)?;
    let scope_custody;
    let mut io = crate::store::history::native_work_io(&work).map_err(Error::from)?;
    let (scope, retained) = fixture.root_scope.with_created_access_with_work(
        &admission,
        Some(access),
        &mut Some(&mut io),
    )?;
    scope_custody = crate::store::history::HistoryIo::take_native_custody(&mut io, retained)?;
    assert_eq!(scope.grants(), fixture.root_scope.grants());
    assert_eq!(
        scope.current_created_private_volume(&mut Some(
            &mut crate::store::native_work_io(&work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None
            })?
        ))?,
        Some(&scratch.request.volume)
    );
    assert!(
        scope
            .created_content(Some(&project.request.volume))
            .unwrap()
            .writer
            .is_none()
    );
    assert!(
        scope
            .created_content(Some(&scratch.request.volume))
            .unwrap()
            .writer
            .is_some()
    );
    let reader = &scope
        .created_content(Some(&project.request.volume))
        .unwrap()
        .reader;
    let content_io = PreparationIo::from_native_work(&work)?;
    let (file, bytes, metadata) = reader
        .read_private_path(
            &project.request.volume,
            "",
            "root.txt",
            Some(&project.request.generation),
            1024,
            Some(&content_io),
        )?
        .await?;
    assert_eq!(bytes.as_ref(), b"original project import");
    assert_eq!(file.volume(), &project.request.volume);
    drop((file, bytes));
    drop(metadata);
    let private = scope
        .created_content(Some(&scratch.request.volume))
        .unwrap();
    let staged_custody;
    let (staged, custody) = private
        .writer
        .as_ref()
        .unwrap()
        .stage_once(
            OperationId::from_bytes([185; 16]),
            "root-output.txt",
            b"actual original root output",
            "text/plain",
            "root-output.txt",
            Some(&content_io),
        )?
        .await?;
    staged_custody = custody;
    let output = private
        .reader
        .read(&staged, 1024, Some(&content_io))?
        .await?;
    assert_eq!(output.as_ref(), b"actual original root output");
    assert_eq!(staged.volume(), &scratch.request.volume);
    assert_ne!(staged.volume(), &project.request.volume);
    drop(output);
    drop(staged);
    drop(staged_custody);
    let policy = crate::filesystem::root_source::FsRootProjectSourceProvider::new(
        fixture.filesystem.clone(),
        project.owner(),
        fixture.root,
        Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
    )?
    .with_bootstrap_source_with_native_work(Arc::clone(project), &work)?;
    let selection = ProjectSourceSnapshot {
        project: project.request.volume.clone(),
        generation: project.request.generation.clone(),
        native_snapshot: None,
    };
    policy
        .verify_with_native_work(
            RootProjectSourceContext {
                owner: project.owner(),
                caller: &admission,
                source: &admission,
                graph_root: fixture.root,
                operation_id: OperationId::from_bytes([180; 16]),
                maximum_work: 1_048_576,
                maximum_bytes: 1_048_576,
                cancellation: work.cancellation(),
                created_project_authenticated: false,
            },
            &selection,
            &work,
        )?
        .await?;
    drop(scope);
    drop(scope_custody);
    drop(admission);
    drop(admission_custody);
    Ok(())
}

#[tokio::test]
async fn root_bootstrap_wrong_creator_birth_selection_and_graph_never_install_access() -> Result<()>
{
    use crate::resource_preparation::{RootBootstrapContentContext, RootBootstrapContentRole};
    let work = observation_work()?;
    let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
    let owner = bind_genuine_original_root(&fixture, &work).await?;
    let birth = &fixture.original_root_births.as_ref().unwrap()[0];
    let bad_issuer = AuthorityIssuer::new(
        "created-owner",
        [81; 32],
        fixture.filesystem_creator.issuer.audience().clone(),
    );
    let bad_creator = FilesystemResourceCreationProvider::new(
        fixture.filesystem.clone(),
        &bad_issuer,
        Some(&work),
    )?;
    assert!(
        bad_creator
            .protect_root_bootstrap_with_native_work(
                &owner,
                &birth.volume,
                RootBootstrapContentRole::Project,
                birth.operation,
                birth.config,
                birth.initial_generation,
                birth.selected_generation,
                None,
                &work
            )?
            .await
            .is_err()
    );

    let creator = &fixture.filesystem_creator;
    assert!(
        creator
            .protect_root_bootstrap_with_native_work(
                &owner,
                &birth.volume,
                RootBootstrapContentRole::Project,
                acyclic_fs::OperationId::from_bytes([179; 16]),
                birth.config,
                birth.initial_generation,
                birth.selected_generation,
                None,
                &work
            )?
            .await
            .is_err()
    );
    let mut wrong_initial = birth.initial_generation.digest().into_bytes();
    wrong_initial[0] ^= 1;
    assert!(
        creator
            .protect_root_bootstrap_with_native_work(
                &owner,
                &birth.volume,
                RootBootstrapContentRole::Project,
                birth.operation,
                birth.config,
                acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(wrong_initial)),
                birth.selected_generation,
                None,
                &work
            )?
            .await
            .is_err()
    );
    let foreign_scratch = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "foreign-original-root-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([184; 16])),
        &mut None,
    )?;
    let denied = creator.protect_root_bootstrap_with_native_work(
        &owner,
        &foreign_scratch,
        RootBootstrapContentRole::Scratch,
        birth.operation,
        birth.config,
        birth.initial_generation,
        birth.selected_generation,
        None,
        &work,
    );
    assert!(match denied {
        Ok(future) => future.await.is_err(),
        Err(_) => true,
    });

    assert!(creator.bound_root_bootstrap_allocations(&work).is_err());
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
    assert!(
        bad_creator
            .bind_root_bootstrap_allocation_with_native_work(Arc::clone(&token), &work)
            .is_err()
    );

    creator.bind_root_bootstrap_allocation_with_native_work(Arc::clone(&token), &work)?;
    let admission_custody;
    let (admission, custody) = owner
        .original_root_admission_with_native_work(&work)
        .await?;
    admission_custody = custody;
    let context = RootBootstrapContentContext::from_verified_root_with_native_work(
        &owner,
        &admission,
        token.witness.binding(),
        &work,
    )
    .await?;
    for control in 0..5 {
        let (mut request, request_custody) = token.request().copy(&work)?;
        match control {
            0 => request.graph_root = crate::TaskId::from_bytes([181; 16]),
            1 => request.root_operation = OperationId::from_bytes([182; 16]),
            2 => {
                request.generation = GenerationRef::new(
                    fixture.filesystem_provider.clone(),
                    birth.initial_generation.digest().into_bytes().to_vec(),
                    None,
                )?
            }
            3 => {
                request.volume = VolumeRef::new(
                    fixture.filesystem_provider.clone(),
                    "foreign-root-project".into(),
                    VolumeClass::Project,
                    VolumeOwner::Project("created-project".into()),
                    &mut None,
                )?
            }
            _ => request.role = RootBootstrapContentRole::Scratch,
        }
        {
            let denied = creator.verify_root_bootstrap_with_native_work(context, &request, &work);
            assert!(match denied {
                Ok(future) => future.await.is_err(),
                Err(_) => true,
            });
        }

        drop(request);
        drop(request_custody);
    }
    drop(admission);
    drop(admission_custody);
    Ok(())
}

#[tokio::test]
async fn physical_root_birth_alone_and_superseded_protected_record_cannot_authenticate_source()
-> Result<()> {
    use crate::resource_preparation::RootBootstrapContentRole;
    use crate::source_authorization::{
        ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
    };
    let work = observation_work()?;
    let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
    let creator = &fixture.filesystem_creator;
    let birth = &fixture.original_root_births.as_ref().unwrap()[0];
    let owner = fixture
        .host
        .journal_owner(fixture.root, (&fixture.root_lease.reservation).into())
        .await?;
    assert!(
        creator
            .protect_root_bootstrap_with_native_work(
                &owner,
                &birth.volume,
                RootBootstrapContentRole::Project,
                birth.operation,
                birth.config,
                birth.initial_generation,
                birth.selected_generation,
                None,
                &work
            )?
            .await
            .is_err()
    );
    let admission_custody;
    let (admission, custody) = owner
        .original_root_admission_with_native_work(&work)
        .await?;
    admission_custody = custody;
    let bare = crate::filesystem::root_source::FsRootProjectSourceProvider::new(
        fixture.filesystem.clone(),
        creator.issuer.audience(),
        fixture.root,
        Some(&crate::filesystem::PreparationIo::from_native_work(&work)?),
    )?;
    let selection = ProjectSourceSnapshot {
        project: birth.volume.clone(),
        generation: GenerationRef::new(
            fixture.filesystem_provider.clone(),
            birth.selected_generation.digest().into_bytes().to_vec(),
            None,
        )?,
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
    {
        let denied = bare.verify_with_native_work(context(), &selection, &work);
        assert!(match denied {
            Ok(future) => future.await.is_err(),
            Err(_) => true,
        });
    }

    let bound_owner = bind_genuine_original_root(&fixture, &work).await?;
    let token = creator
        .protect_root_bootstrap_with_native_work(
            &bound_owner,
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
    let policy = bare.with_bootstrap_source_with_native_work(Arc::clone(&token), &work)?;
    policy
        .verify_with_native_work(context(), &selection, &work)?
        .await?;
    let initial_selection = ProjectSourceSnapshot {
        project: selection.project.clone(),
        generation: GenerationRef::new(
            fixture.filesystem_provider.clone(),
            birth.initial_generation.digest().into_bytes().to_vec(),
            None,
        )?,
        native_snapshot: None,
    };
    {
        let denied = policy.verify_with_native_work(context(), &initial_selection, &work);
        assert!(match denied {
            Ok(future) => future.await.is_err(),
            Err(_) => true,
        });
    }

    let journal_io = PreparationIo::for_unmeasured(work.cancellation())?;
    let journal = allocation_ref(&creator.host.provider, &birth.volume, Some(&journal_io))?;
    let generation = fixture.filesystem.resolve(&journal).await?.generation;
    fixture
        .filesystem
        .apply(
            &journal,
            Some(&generation),
            &[crate::filesystem::WorkspaceMutation::PutFile {
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
    drop(admission_custody);
    Ok(())
}

#[tokio::test]
async fn canonical_original_root_binder_rejects_foreign_prior_bind_and_wrong_actor() -> Result<()> {
    use crate::core::{Action, Command, SchemaRegistry};
    for prior_foreign_bind in [false, true] {
        let work = observation_work()?;
        let fixture = crate::durable_host::tests::created_resource_access::CreationFixture::new_with_original_root_births(&work).await?;
        let owner = fixture
            .host
            .journal_owner(fixture.root, (&fixture.root_lease.reservation).into())
            .await?;
        if prior_foreign_bind {
            let audience = Authority {
                kind: AggregateKind::Conversation,
                id: "genuine-original-root-bootstrap".into(),
            };
            let issuer = AuthorityIssuer::new("created-owner", [80; 32], audience.clone());
            let foreign_agent = AgentId::from_bytes([186; 16]);
            let scope = issuer
                .issue(
                    "foreign-prior-root-bind",
                    &Capabilities::new(["conversation:bind"]),
                    Some(foreign_agent),
                    None,
                    &mut None,
                )
                .expect("scope proof");
            let mut aggregate = crate::store::StreamAggregate::open(
                &fixture.stream,
                audience,
                issuer.verifier(&mut None).expect("verifier copy"),
                SchemaRegistry::new(),
                None,
                crate::store::default_projection_read_limits(),
                crate::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None,
            )
            .await?;
            aggregate
                .execute(
                    Command {
                        operation_id: OperationId::from_bytes([186; 16]),
                        idempotency_key: IdempotencyKey::new("genuine-foreign-prior-root-bind")?,
                        expected_revision: 0,
                        causal_parent: None,
                        action: Action::BindConversation {
                            agent: foreign_agent,
                        },
                        scope,
                    },
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await?;
        }
        let actor = if prior_foreign_bind {
            AgentId::from_bytes([80; 16])
        } else {
            AgentId::from_bytes([187; 16])
        };
        let authority_custody;
        let (authority, custody) = original_root_fixture_authority(actor, &work)?;
        authority_custody = custody;
        let scratch = &fixture.original_root_births.as_ref().unwrap()[1];
        assert!(
            crate::filesystem::bind_original_root_conversation_with_native_work(
                &fixture.stream,
                authority,
                authority_custody,
                &owner,
                &scratch.volume,
                scratch.operation,
                fixture.root_scope.limits(),
                &work,
            )
            .await
            .is_err()
        );
        assert!(
            fixture
                .filesystem_creator
                .bound_root_bootstrap_allocations(&work)
                .is_err()
        );
        assert!(
            owner
                .root_bootstrap_binding_witness_with_native_work(&work)
                .await
                .is_err()
        );
    }
    Ok(())
}
