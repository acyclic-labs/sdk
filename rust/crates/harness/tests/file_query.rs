//! Ordinary filesystem queries and shared client projections at accepted cuts.
use acyclic_fs::{
    AsyncObjectStore, Fs, MemoryAuthorityBackend, MemoryObjectBackend, performance::BOUNDARY_BUDGET,
};
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
use futures::{FutureExt as _, StreamExt as _};
use std::sync::Arc;
#[path = "file_query/goal_sources.rs"]
mod goal_sources;
#[path = "file_query/goals.rs"]
mod goals;
#[path = "file_query/objects.rs"]
mod objects;
#[path = "file_query/replacements.rs"]
mod replacements;
use objects::CountingObjects;

async fn next(changes: &mut acyclic_harness::task_volume::ChangeStream) -> Change {
    tokio::time::timeout(std::time::Duration::from_secs(5), changes.next())
        .await
        .expect("file follow did not deliver the committed revision")
        .expect("file follow ended")
        .unwrap()
}

#[tokio::test]
async fn ordinary_plan_names_and_non_markdown_files_follow_without_consuming_matches()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new().await?;
    fixture
        .write(&[
            ("plans/0.txt", Some("ignored")),
            ("plans/1.txt", Some("ignored")),
            ("plans/release notes.md", Some("- [ ] First\n\n  Second")),
            ("plans/調査.md", Some("- [~] 調査")),
            ("plans/z.txt", Some("ignored")),
        ])
        .await?;
    let observation = fixture
        .source
        .observe_files(Query::Plans, None, 2, 128)
        .await?;
    let mut changes = observation.changes;
    let mut view =
        FileQueryView::from_snapshot(Query::Plans, observation.snapshot.unwrap(), 2, 128)?;
    assert_eq!(
        view.files()
            .keys()
            .map(|key| key.path())
            .collect::<Vec<_>>(),
        ["plans/release notes.md", "plans/調査.md"]
    );
    fixture
        .write(&[
            ("plans/release notes.md", Some("- [x] Done")),
            ("plans/2.txt", Some("ignored")),
        ])
        .await?;
    view.apply(next(&mut changes).await)?;
    assert_eq!(
        view.files()[&acyclic_harness::task_volume::FileKey::parse("plans/release notes.md")?]
            .segments[0]
            .as_ref(),
        b"- [x] Done"
    );
    fixture
        .write(&[("plans/調査.md", None), ("plans/.md", Some("- [ ] Hidden"))])
        .await?;
    view.apply(next(&mut changes).await)?;
    assert_eq!(
        view.files()
            .keys()
            .map(|key| key.path())
            .collect::<Vec<_>>(),
        ["plans/release notes.md"]
    );
    let snapshot = fixture
        .source
        .observe_files(Query::Plans, None, 1, 128)
        .await?
        .snapshot
        .ok_or("missing snapshot")?;
    assert_eq!(snapshot.files.len(), 1);
    assert_eq!(snapshot.files[0].key.path(), "plans/release notes.md");
    Ok(())
}

#[test]
fn empty_stem_publications_preserve_live_and_snapshot_plan_views()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use proptest::{prelude::*, test_runner::TestRunner};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // A non-plan publication advances the cursor without changing selected
    // bytes or consuming the one-plan allowance, for arbitrary names/content.
    TestRunner::default().run(&("[a-zλ雪_-]{1,12}", ".{0,32}"), |(stem, text)| {
        let result: std::result::Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
            let fixture = Fixture::new().await?;
            let path = format!("plans/{stem}.md");
            fixture.write(&[(&path, Some(&text))]).await?;
            let observation = fixture
                .source
                .observe_files(Query::Plans, None, 1, 256)
                .await?;
            let initial = observation.snapshot.ok_or("initial snapshot missing")?;
            let expected = initial.files.clone();
            let mut changes = observation.changes;
            let mut view = FileQueryView::from_snapshot(Query::Plans, initial, 1, 256)?;
            fixture.write(&[("plans/.md", Some("ignored"))]).await?;
            let change = tokio::task::unconstrained(changes.next())
                .now_or_never()
                .ok_or("published revision is pending")?
                .ok_or("follow ended")??;
            assert!(!view.apply(change)?);
            let latest = fixture
                .source
                .observe_files(Query::Plans, None, 1, 256)
                .await?
                .snapshot
                .ok_or("latest snapshot missing")?;
            assert_eq!(view.cursor(), &latest.cursor);
            assert_eq!(latest.files, expected);
            let actual = view
                .files()
                .values()
                .map(|file| {
                    assert_eq!(file.segments.len(), 1);
                    acyclic_harness::task_volume::FileValue {
                        key: file.key.clone(),
                        channel: file.channel,
                        publication: file.publication,
                        bytes: file.segments[0].clone(),
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            Ok(())
        });
        result.map_err(|error| TestCaseError::fail(error.to_string()))
    })?;
    Ok(())
}

#[tokio::test]
async fn one_matching_plan_with_unrelated_entries_stays_live()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    for path in ["plans/a.md", "plans/计划.md", "plans/release notes.md"] {
        let fixture = Fixture::new().await?;
        fixture
            .write(&[
                ("plans/0.txt", Some("ignored")),
                (path, Some("- [ ] Build")),
                ("plans/notes.txt", Some("ignored")),
            ])
            .await?;
        let observation = fixture
            .source
            .observe_files(Query::Plans, None, 1, 64)
            .await?;
        let mut changes = observation.changes;
        let mut view = FileQueryView::from_snapshot(
            Query::Plans,
            observation.snapshot.ok_or("snapshot missing")?,
            1,
            64,
        )?;
        assert_eq!(view.plan_rows().collect::<Vec<_>>(), [path, "[ ] Build"]);
        fixture
            .write(&[
                (path, Some("- [x] Ship")),
                ("plans/another.txt", Some("ignored")),
            ])
            .await?;
        assert!(view.apply(next(&mut changes).await)?);
        assert_eq!(view.plan_rows().collect::<Vec<_>>(), [path, "[x] Ship"]);
    }
    Ok(())
}

struct Fixture<O = MemoryObjectBackend> {
    host: Arc<FilesystemHost<MemoryAuthorityBackend, O>>,
    source: FilesystemFileQuery<MemoryStream, O>,
    filesystem: Fs<MemoryAuthorityBackend, O>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
    volume: VolumeRef,
    issuer: AuthorityIssuer,
    agent: AgentId,
    workspace: acyclic_harness::resources::WorkspaceRef,
}

impl Fixture {
    async fn new() -> Result<Self> {
        Self::with_filesystem(Fs::memory(), Arc::default()).await
    }
}

impl Fixture<CountingObjects> {
    async fn counted() -> Result<Self> {
        let objects = CountingObjects::default();
        let reads = objects.reads.clone();
        let filesystem = Fs::new(
            acyclic_fs::StreamAuthorityStore::new(Arc::new(MemoryStream::default())),
            objects,
            acyclic_fs::EmbeddedCapabilities::MEMORY,
        );
        Self::with_filesystem(filesystem, reads).await
    }
}

impl<O: AsyncObjectStore + 'static> Fixture<O> {
    async fn with_filesystem(
        filesystem: Fs<MemoryAuthorityBackend, O>,
        reads: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Result<Self> {
        Self::with_class(filesystem, reads, VolumeClass::AgentPrivate).await
    }

    async fn with_class(
        filesystem: Fs<MemoryAuthorityBackend, O>,
        reads: Arc<std::sync::atomic::AtomicUsize>,
        class: VolumeClass,
    ) -> Result<Self> {
        let provider =
            ProviderRef::new("query".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(filesystem.clone(), &provider, None)?;
        let agent = AgentId::new();
        let volume = VolumeRef::new(
            provider,
            "private".into(),
            class,
            if class == VolumeClass::Project {
                VolumeOwner::Project("query".into())
            } else {
                VolumeOwner::Agent(agent)
            },
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
            filesystem,
            reads,
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

    async fn ignored(
        &self,
        entries: &[(u8, String, bool)],
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let name = self.volume.storage_name(&mut None)?;
        let workspace = self
            .filesystem
            .open_workspace(name, BOUNDARY_BUDGET, &cancellation)
            .await?
            .value;
        let mut transaction = workspace
            .begin_transaction(
                acyclic_fs::IdempotencyKey::new(),
                BOUNDARY_BUDGET,
                &cancellation,
            )
            .await?
            .value;
        for (index, (kind, name, markdown)) in entries.iter().enumerate() {
            let path = format!(
                "/plans/{index}-{name}.{}",
                if *markdown { "md" } else { "txt" }
            );
            match kind {
                0 => {
                    transaction
                        .create_directory(&path, BOUNDARY_BUDGET, &cancellation)
                        .await?;
                    transaction
                        .write_text(&format!("{path}/ignored.txt"), "ignored")
                        .await?;
                }
                1 => {
                    transaction
                        .create_symbolic_link(
                            &path,
                            bytes::Bytes::from_static(b"/missing.md"),
                            BOUNDARY_BUDGET,
                            &cancellation,
                        )
                        .await?;
                }
                _ => {
                    transaction
                        .write_text(&format!("{path}.txt"), "ignored")
                        .await?;
                }
            }
        }
        let commit = transaction
            .commit(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                BOUNDARY_BUDGET,
                &cancellation,
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await?
            .value;
        assert!(matches!(
            commit,
            acyclic_fs::TransactionCommit::Committed(_)
        ));
        Ok(())
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn ignored_plan_entries_never_fail_snapshot_or_follow(
        entries in proptest::collection::vec((0u8..3, "[a-z]{1,12}", proptest::bool::ANY), 1..9)
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let fixture = Fixture::new().await?;
            fixture.write(&[("plans/release.md", Some("- [ ] Build"))]).await?;
            let observation = fixture.source.observe_files(Query::Plans, None, 1, 64).await?;
            let mut changes = observation.changes;
            fixture.ignored(&entries).await?;
            let change = next(&mut changes).await;
            assert!(change.files.is_empty() && change.removed.is_empty());
            let snapshot = fixture.source.observe_files(Query::Plans, None, 1, 64).await?.snapshot.unwrap();
            assert_eq!(snapshot.files.len(), 1);
            fixture.write(&[("plans/release.md", Some("- [x] Build"))]).await?;
            assert_eq!(next(&mut changes).await.files.len(), 1);
            Ok::<_, Box<dyn std::error::Error>>(())
        }).unwrap();
    }
}

#[tokio::test]
async fn editing_one_of_a_hundred_files_reads_only_its_content()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    let fixture = Fixture::<CountingObjects>::counted().await?;
    let paths: Vec<_> = (0..100).map(|i| format!("plans/{i:03}.md")).collect();
    // Use chunk-backed content so the backend counts actual file-body reads.
    let text = "- [ ] Build\n".repeat(8);
    assert!(text.len() > acyclic_fs::kernel::MAXIMUM_INLINE_FILE_BYTES);
    let maximum_bytes = paths.len() * text.len();
    let writes: Vec<_> = paths
        .iter()
        .map(|path| (path.as_str(), Some(text.as_str())))
        .collect();
    fixture.write(&writes).await?;
    fixture.reads.store(0, Ordering::SeqCst);
    let observation = fixture
        .source
        .observe_files(Query::Plans, None, 100, maximum_bytes)
        .await?;
    assert_eq!(fixture.reads.load(Ordering::SeqCst), 100);
    let mut changes = observation.changes;
    let mut view = FileQueryView::from_snapshot(
        Query::Plans,
        observation.snapshot.unwrap(),
        100,
        maximum_bytes,
    )?;
    let before = view.clone();
    for changed in [1, 3] {
        let updated = text.replace("[ ]", if changed == 1 { "[~]" } else { "[x]" });
        let writes: Vec<_> = paths[..changed]
            .iter()
            .map(|path| (path.as_str(), Some(updated.as_str())))
            .collect();
        fixture.write(&writes).await?;
        fixture.reads.store(0, Ordering::SeqCst);
        let change = next(&mut changes).await;
        assert_eq!(fixture.reads.load(Ordering::SeqCst), changed);
        assert_eq!(change.files.len(), changed);
        view.apply(change)?;
        for key in view.files().keys().skip(changed) {
            assert_eq!(
                view.files()[key].segments[0].as_ptr(),
                before.files()[key].segments[0].as_ptr()
            );
        }
    }
    fixture
        .write(&[("plans/ignored.txt", Some("ignored"))])
        .await?;
    fixture.reads.store(0, Ordering::SeqCst);
    assert!(next(&mut changes).await.files.is_empty());
    assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn single_file_query_retains_its_value_across_unrelated_revisions()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_harness::task_volume::FileKey;
    use std::sync::atomic::Ordering;
    let fixture = Fixture::<CountingObjects>::counted().await?;
    let first = "First ".repeat(16);
    let second = "Second".repeat(16);
    assert!(first.len() > acyclic_fs::kernel::MAXIMUM_INLINE_FILE_BYTES);
    fixture.write(&[("GOAL.md", Some(&first))]).await?;
    let observation = fixture
        .source
        .observe_files(Query::File(FileKey::Goal), None, 1, first.len())
        .await?;
    let mut changes = observation.changes;
    fixture
        .write(&[("plans/other.md", Some("- [ ] Other"))])
        .await?;
    fixture.reads.store(0, Ordering::SeqCst);
    assert!(next(&mut changes).await.files.is_empty());
    assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
    fixture.write(&[("GOAL.md", Some(&second))]).await?;
    fixture.reads.store(0, Ordering::SeqCst);
    assert_eq!(
        next(&mut changes).await.files[0].bytes.as_ref(),
        second.as_bytes()
    );
    assert_eq!(fixture.reads.load(Ordering::SeqCst), 1);
    Ok(())
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
    assert_eq!(view.files()[&key].segments[0].as_ptr(), pointer);
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

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Ordinary project queries preserve their exact file bytes and require the
    // signed read grant just like agent-private queries; class is no authority.
    #[test]
    fn project_and_agent_queries_preserve_signed_authority(
        project in proptest::bool::ANY,
        authorized in proptest::bool::ANY,
        text in ".{0,32}",
    ) {
        futures::executor::block_on(async {
            let fixture = Fixture::with_class(Fs::memory(), Arc::default(),
                if project { VolumeClass::Project } else { VolumeClass::AgentPrivate }).await?;
            fixture.write(&[("plans/a.md", Some(&text))]).await?;
            let capabilities = if authorized {
                Capabilities::new([fixture.volume.capability(VolumeOperation::Read, &mut None)?])
            } else { Capabilities::default() };
            let scope = fixture.issuer.issue("reader", &capabilities, Some(fixture.agent), None, &mut None)?;
            let reader = FilesystemContentVerifier::new(fixture.host.clone(),
                &fixture.issuer.verifier(&mut None)?, &scope, 65_536, None)?;
            let result = reader.file_query(&fixture.volume, BOUNDARY_BUDGET)?
                .observe_files(Query::Plans, None, 1, 4096).await;
            if authorized {
                let snapshot = result?.snapshot.unwrap();
                assert_eq!(snapshot.files.len(), 1);
                assert_eq!(snapshot.files[0].bytes.as_ref(), text.as_bytes());
            } else {
                assert!(matches!(result, Err(acyclic_harness::task_volume::Error::FileRead(
                    acyclic_harness::Error::Unauthorized(_)))));
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }).unwrap();
    }
}
