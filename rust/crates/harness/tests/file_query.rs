use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend, performance::BOUNDARY_BUDGET};
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, Result,
    client::FileQueryView,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{
        FilesystemContentVerifier, FilesystemFileQuery, FilesystemHost, WorkspaceMutation,
    },
    resources::ProviderRef,
    task_volume::{Change, FileQuerySource, Query},
};
use acyclic_stream::MemoryStream;
use futures::StreamExt as _;
use std::sync::Arc;

async fn next(changes: &mut acyclic_harness::task_volume::ChangeStream) -> Change {
    tokio::time::timeout(std::time::Duration::from_secs(5), changes.next())
        .await
        .expect("file follow did not deliver the committed revision")
        .expect("file follow ended")
        .unwrap()
}

struct Fixture {
    host: Arc<FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>>,
    source: FilesystemFileQuery<MemoryStream, MemoryObjectBackend>,
    volume: VolumeRef,
    issuer: AuthorityIssuer,
    agent: AgentId,
    workspace: acyclic_harness::resources::WorkspaceRef,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let provider =
            ProviderRef::new("query".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::new();
        let volume = VolumeRef::new(
            provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            "query",
            [9; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "query".into(),
            },
        );
        let scope = issuer.issue(
            "owner",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )?;
        let reader = FilesystemContentVerifier::new(
            host.clone(),
            &issuer.verifier(&mut None)?,
            &scope,
            65_536,
            None,
        )?;
        let workspace = host.create_volume(&volume).await?.workspace;
        let source = reader.file_query(&volume, BOUNDARY_BUDGET)?;
        Ok(Self {
            host,
            source,
            volume,
            issuer,
            agent,
            workspace,
        })
    }

    async fn write(&self, writes: &[(&str, Option<&str>)]) -> Result<()> {
        let mut mutations = vec![WorkspaceMutation::CreateDirectory {
            path: "/plans".into(),
        }];
        mutations.extend(writes.iter().map(|(path, text)| match text {
            Some(text) => WorkspaceMutation::PutFile {
                path: format!("/{path}"),
                bytes: text.as_bytes().to_vec().into(),
            },
            None => WorkspaceMutation::Remove {
                path: format!("/{path}"),
            },
        }));
        self.host
            .apply(
                &self.workspace,
                None,
                &mutations,
                &IdempotencyKey::new(uuid::Uuid::new_v4().to_string())?,
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn ordinary_file_query_folds_atomic_replacements_removals_and_duplicate_replay() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let observation = fixture
        .source
        .observe_files(Query::Plans, None, 2, 64)
        .await
        .unwrap();
    let mut changes = observation.changes;
    let mut view =
        FileQueryView::from_snapshot(Query::Plans, observation.snapshot.unwrap(), 2, 64)?;
    fixture
        .write(&[
            ("plans/a.md", Some("- [ ] Build")),
            ("plans/b.md", Some("- [~] Ship")),
        ])
        .await?;
    let change = next(&mut changes).await;
    assert_eq!(change.files.len(), 2);
    let pointer = change.files[0].bytes.as_ptr();
    let key = change.files[0].key.clone();
    assert!(view.apply(change)?);
    assert_eq!(view.files()[&key].bytes.as_ptr(), pointer);
    fixture
        .write(&[
            ("plans/a.md", None),
            ("plans/b.md", Some("- [x] Ship")),
            ("plans/c.md", Some("- [ ] Check")),
        ])
        .await?;
    let change = next(&mut changes).await;
    assert_eq!((change.files.len(), change.removed.len()), (2, 1));
    let duplicate = Change {
        file: None,
        files: change.files.clone(),
        removed: change.removed.clone(),
        cursor: change.cursor.clone(),
    };
    let bad = Change {
        file: Some(change.files[0].clone()),
        files: change.files.clone(),
        removed: change.removed.clone(),
        cursor: change.cursor.clone(),
    };
    let before = view.clone();
    assert!(view.apply(bad).is_err());
    assert_eq!(view, before);
    assert!(view.apply(change)?);
    assert_eq!(
        view.plan_lines(),
        ["plans/b.md", "[x] Ship", "plans/c.md", "[ ] Check"]
    );
    assert!(!view.apply(duplicate)?);
    fixture
        .write(&[("plans/b.md", None), ("plans/c.md", None)])
        .await?;
    assert!(view.apply(next(&mut changes).await)?);
    assert!(view.files().is_empty());
    Ok(())
}

#[tokio::test]
async fn ordinary_file_query_resumes_every_revision_and_resets_incompatible_cursors() -> Result<()>
{
    let fixture = Fixture::new().await?;
    fixture
        .write(&[("plans/release.md", Some("- [ ] Build"))])
        .await?;
    let observation = fixture
        .source
        .observe_files(Query::Plans, None, 2, 64)
        .await
        .unwrap();
    let mut view =
        FileQueryView::from_snapshot(Query::Plans, observation.snapshot.unwrap(), 2, 64)?;
    let cursor = view.cursor().clone();
    fixture
        .write(&[("plans/release.md", Some("- [~] Build"))])
        .await?;
    fixture
        .write(&[("plans/release.md", Some("- [x] Build"))])
        .await?;
    let resume = fixture
        .source
        .observe_files(Query::Plans, Some(cursor.clone()), 2, 64)
        .await
        .unwrap();
    assert!(!resume.reset);
    assert!(resume.snapshot.is_none());
    let mut changes = resume.changes;
    assert!(view.apply(next(&mut changes).await)?);
    assert_eq!(view.plan_lines(), ["plans/release.md", "[~] Build"]);
    assert!(view.apply(next(&mut changes).await)?);
    assert_eq!(view.plan_lines(), ["plans/release.md", "[x] Build"]);
    for mutation in [
        "epoch",
        "query",
        "schema",
        "commit",
        "position",
        "watermark",
        "volume",
    ] {
        let mut bad = cursor.clone();
        match mutation {
            "epoch" => bad.epoch = uuid::Uuid::nil(),
            "query" => bad.query[0] ^= 1,
            "schema" => bad.schema += 1,
            "commit" => bad.commit[0] ^= 1,
            "position" => bad.position = u64::MAX,
            "watermark" => bad.durable = bad.accepted + 1,
            "volume" => bad.volume[0] ^= 1,
            _ => unreachable!(),
        }
        let reset = fixture
            .source
            .observe_files(Query::Plans, Some(bad), 2, 64)
            .await
            .unwrap();
        assert!(reset.reset, "{mutation}");
        let reset = FileQueryView::from_snapshot(Query::Plans, reset.snapshot.unwrap(), 2, 64)?;
        assert_eq!(reset.plan_lines(), view.plan_lines(), "{mutation}");
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_file_queries_keep_authority_work_and_selection_bounds() -> Result<()> {
    let fixture = Fixture::new().await?;
    let denied = fixture.issuer.issue(
        "denied",
        &Capabilities::new([fixture
            .volume
            .directory_read_capability("other", &mut None)?]),
        Some(fixture.agent),
        None,
        &mut None,
    )?;
    let reader = FilesystemContentVerifier::new(
        fixture.host.clone(),
        &fixture.issuer.verifier(&mut None)?,
        &denied,
        65_536,
        None,
    )?;
    let source = reader.file_query(&fixture.volume, BOUNDARY_BUDGET)?;
    assert!(matches!(
        source.observe_files(Query::Plans, None, 2, 64).await,
        Err(acyclic_harness::task_volume::Error::FileRead(
            acyclic_harness::Error::Unauthorized(_)
        ))
    ));
    fixture
        .write(&[
            ("plans/a.md", Some("- [ ] Build")),
            ("plans/b.md", Some("- [x] Ship")),
        ])
        .await?;
    assert!(
        fixture
            .source
            .observe_files(Query::Plans, None, 2, 64)
            .await
            .is_ok()
    );
    assert!(
        fixture
            .source
            .observe_files(Query::Plans, None, 1, 64)
            .await
            .is_err()
    );
    assert!(
        fixture
            .source
            .observe_files(Query::Plans, None, 2, 4)
            .await
            .is_err()
    );
    assert!(
        fixture
            .source
            .observe_files(Query::Tasks, None, 2, 64)
            .await
            .is_err()
    );
    let reader = FilesystemContentVerifier::new(
        fixture.host.clone(),
        &fixture.issuer.verifier(&mut None)?,
        &fixture.issuer.issue(
            "tiny",
            &Capabilities::new([fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?]),
            Some(fixture.agent),
            None,
            &mut None,
        )?,
        65_536,
        None,
    )?;
    let tiny = reader.file_query(&fixture.volume, acyclic_fs::performance::work_budget(0, 0))?;
    assert!(tiny.observe_files(Query::Plans, None, 2, 64).await.is_err());
    Ok(())
}
