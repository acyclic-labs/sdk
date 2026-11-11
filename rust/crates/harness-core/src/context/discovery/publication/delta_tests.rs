use super::*;
use crate::{
    conversation::{VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{FilesystemContentVerifier, FilesystemHost},
    resources::ProviderRef,
};
use acyclic_fs::{CancellationToken, EmbeddedCapabilities, Fs, WorkBudget};
use acyclic_stream::{MemoryStream, StreamProvider, StreamProviderExt};

// A real referenced publication with 128 discovered skills. An unchanged bad
// replacement must not poison an unrelated valid update or removal, and the
// two immutable endpoints must not double the discovery directory allowance.
#[tokio::test]
async fn a_referenced_delta_skips_unchanged_invalid_skills_without_a_second_discovery_walk()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let fs = Fs::new(
        acyclic_fs::distributed::StreamAuthorityStore::new(streams.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
        EmbeddedCapabilities::MEMORY,
    );
    let provider = ProviderRef::new("delta".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(fs.clone(), &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        "skills".into(),
        VolumeClass::Project,
        VolumeOwner::Project("skills".into()),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let cancellation = CancellationToken::new();
    let workspace = fs
        .open_workspace(
            volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let issuer = AuthorityRoot::new(
        "delta",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "delta".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "skills-only",
        &crate::Capabilities::new([volume.directory_read_capability("skills", &mut None)?]),
        Some(crate::AgentId::new()),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let reader =
        FilesystemContentVerifier::new(host.clone(), &verifier, &scope, Some(32_768), None)?;
    let root = ContextRoot {
        volume: volume.clone(),
        directory: "skills".into(),
    };
    let io = PreparationIo::for_unmeasured(&cancellation)?;
    let stream_path = acyclic_fs::session::changes_path(&fs, workspace.id().volume_id())?;
    let mut delta_from = 0;
    let mut original = None;
    for update in [false, true] {
        let mut directories = vec!["/skills".to_owned()];
        let mut files = Vec::new();
        if update {
            directories.push("/skills/added".into());
            files.push((
                "/skills/added/SKILL.md".into(),
                acyclic_stream::OwnedBytes::copy_from_slice(
                    b"---\nname: added\ndescription: Added\n---\nbody\n",
                ),
            ));
            delta_from = streams.tail(stream_path.clone()).await?;
            directories.push("/generated".into());
            for group in 0..16 {
                directories.push(format!(
                    "/generated/descriptive-publication-reference-group-{group:02}"
                ));
            }
            for index in 0..6144 {
                files.push((format!(
                    "/generated/descriptive-publication-reference-group-{:02}/a-source-file-with-a-descriptive-name-{index:05}.rs",
                    index % 16,
                ), acyclic_stream::OwnedBytes::copy_from_slice(b"fn main() {}")));
            }
            files.push((
                "/skills/skill001/SKILL.md".into(),
                acyclic_stream::OwnedBytes::copy_from_slice(
                    b"---\nname: skill001\ndescription: Updated\n---\nupdated body\n",
                ),
            ));
        } else {
            for index in 0..128 {
                let directory = format!("/skills/skill{index:03}");
                files.push((
                    format!("{directory}/SKILL.md"),
                    acyclic_stream::OwnedBytes::from(
                        format!("---\nname: skill{index:03}\ndescription: Original\n---\nbody\n")
                            .into_bytes(),
                    ),
                ));
                directories.push(directory);
            }
        }
        let directories: Vec<_> = directories.iter().map(String::as_str).collect();
        let files: Vec<_> = files
            .iter()
            .map(|(path, bytes)| (path.as_str(), bytes.clone()))
            .collect();
        let mut transaction = workspace
            .begin_transaction(
                acyclic_fs::IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        transaction
            .write_batch(&directories, &files, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        if update {
            transaction
                .remove(
                    "/skills/skill002/SKILL.md",
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| failure.error)?;
        }
        transaction
            .commit(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: Default::default(),
                        },
                    )
                },
            )
            .await
            .map_err(|failure| failure.error)?;
        if !update {
            original = Some(
                workspace
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            );
            let mut source = VolumeDiscovery {
                reader: reader.as_ref(),
                root: &root,
                io: Some(&io),
            };
            let mut work = DiscoveryWork::default();
            let skills = super::super::source::discover_skills_from(
                &mut source,
                "skills",
                "SKILL.md",
                &mut work,
                None,
                false,
            )
            .await?;
            assert_eq!(skills.len(), 128);
            assert_eq!(work.directories, 129);
            workspace
                .write_text("/skills/skill000/SKILL.md", "malformed replacement")
                .await?;
        }
    }
    let volume_id = workspace.id().volume_id();
    let resource = acyclic_fs::session::resource(volume_id, "")?;
    let credential = acyclic_auth::testing::root(|block| {
        use acyclic_auth::contract::*;
        block.certificates.as_mut().unwrap().keys[0].key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: format!("{resource}/skills"),
                        prefix: true,
                    },
                    actions: vec!["fs/list".into()],
                }),
            }],
        };
    });
    let authorization =
        acyclic_stream::session::testing::resource_authorization(&credential, &resource)?;
    let original = original.ok_or("missing original generation")?;
    let mut last_generation = None;
    for restored in [false, true] {
        if restored {
            delta_from = streams.tail(stream_path.clone()).await?;
            let current = workspace
                .head(WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| failure.error)?
                .value;
            let outcome = workspace
                .restore_generation(
                    &original,
                    current.id(),
                    acyclic_fs::IdempotencyKey::new(),
                    acyclic_fs::PublicationPermit::Unrestricted,
                )
                .await?;
            assert!(matches!(outcome, acyclic_fs::WorkspaceRestore::Restored(_)));
        }
        let mut records = streams
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: stream_path.clone(),
                from: delta_from,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = records
            .next()
            .await
            .ok_or("missing referenced publication")??;
        let entries = acyclic_stream::decode_changes(record.value.as_ref())?;
        assert_eq!(entries.entries.len(), 1);
        assert_eq!(entries.entries[0].operation, "delta");
        let reference = acyclic_fs::PublicationEntry::from_parts(
            &entries.entries[0].key,
            &entries.entries[0].operation,
            &entries.entries[0].metadata,
            workspace.volume_config(),
        )?
        .delta
        .ok_or("missing delta reference")?;
        let acyclic_fs::DeltaOrigin::Generation(before) =
            reference.origin_for(&Default::default())?
        else {
            return Err("publication lost its actual baseline".into());
        };
        if restored {
            assert_eq!(
                Some(before),
                last_generation,
                "restore must compare against its current head"
            );
        }
        let generation = workspace
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        last_generation = Some(generation.id());
        let generation = host.generation_ref_id(generation.id(), None)?;
        let before = host.generation_ref_id(before, None)?;
        let mut terminal: acyclic_stream::BoxProviderFuture<'static, StreamError> =
            Box::pin(std::future::pending());
        let roots = [&root];
        let compared = reader
            .diff_private_paths(&roots, (&before, &generation), Some(&io))?
            .await?;
        let changes = expand_delta(
            &mut terminal,
            reader.as_ref(),
            &root,
            DeltaExpansion {
                authorization: (volume_id, &authorization),
                reference,
                generation: &generation,
                filter: &Default::default(),
                headers: &record.headers,
                config: workspace.volume_config(),
                scope: "",
            },
            compared.first(),
            ContextDiscoveryLimits::default(),
            &io,
        )
        .await??;
        assert_eq!(changes.len(), if restored { 4 } else { 3 });
        assert!(changes.iter().any(|change| matches!(change,
            SkillPublicationChange::Skill(skill) if skill.source.path == "skills/skill001/SKILL.md"
        )));
        if restored {
            assert!(changes.iter().any(|change| matches!(change,
                SkillPublicationChange::Removed(path) if path.path == "skills/added/SKILL.md"
            )));
            assert!(changes.iter().any(|change| matches!(change,
                SkillPublicationChange::Skill(skill) if skill.source.path == "skills/skill002/SKILL.md"
            )));
        } else {
            assert!(changes.iter().any(|change| matches!(change,
                SkillPublicationChange::Removed(path) if path.path == "skills/skill002/SKILL.md"
            )));
        }
        let forbidden = ContextRoot {
            volume: volume.clone(),
            directory: "generated".into(),
        };
        assert!(
            reader
                .diff_private_paths(&[&forbidden], (&generation, &generation), Some(&io))?
                .await
                .is_err()
        );
    }
    Ok(())
}
