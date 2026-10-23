//! Incremental pinned-file query projection and bounded diff properties.

#[cfg(not(target_arch = "wasm32"))]
#[path = "../client_diff_model.rs"]
mod model;
#[cfg(not(target_arch = "wasm32"))]
#[path = "../client_diff_properties.rs"]
mod properties;

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, Result,
    client::{DiffLimits, DiffPath, DiffPublication, DiffView, FileDiff},
    context::{ContextRoot, PinnedContextPath},
    conversation::{
        ContentGrant, ContentResidencyVerifier, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemContentVerifier, FilesystemHost},
    resources::ProviderRef,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Fixture {
    host: Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    root: ContextRoot,
    writer: ContentGrant,
    reader: Arc<dyn ContentResidencyVerifier>,
}

#[test]
fn bounded_diffs_apply_and_match_minimal_edit_counts_for_short_sequences()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut texts = vec![String::new()];
    for length in 1..=4 {
        for bits in 0..(1 << length) {
            texts.push(
                (0..length)
                    .map(|bit| if bits & (1 << bit) == 0 { "a\n" } else { "b\n" })
                    .collect(),
            );
        }
    }
    for before in &texts {
        for after in &texts {
            let oracle = diffy::create_patch(before, after);
            let (mut added, mut removed) = (0, 0);
            for line in oracle.hunks().iter().flat_map(|hunk| hunk.lines()) {
                match line {
                    diffy::Line::Insert(_) => added += 1,
                    diffy::Line::Delete(_) => removed += 1,
                    _ => {}
                }
            }
            let actual = FileDiff::between(
                Some(before.as_bytes()),
                Some(after.as_bytes()),
                8192,
                32_768,
            )?;
            if let Some(actual) = actual {
                assert_eq!((actual.added, actual.removed), (added, removed));
                let patch = diffy::Patch::from_str(&actual.patch)?;
                assert_eq!(diffy::apply(before, &patch)?, *after);
            } else {
                assert_eq!(before, after);
            }
        }
    }
    Ok(())
}

#[test]
fn equal_size_replacements_stop_at_the_computation_budget() {
    let before = "a\n".repeat(2000);
    let after = "b\n".repeat(2000);
    let result = FileDiff::between(Some(before.as_bytes()), Some(after.as_bytes()), 64, 16_384);
    assert!(
        matches!(result, Err(acyclic_harness::Error::Invalid(reason))
        if reason == "diff computation work bound exceeded")
    );
}

#[test]
fn additions_deletions_and_asymmetric_inputs_fit_a_linear_work_budget()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let lines = "a\n".repeat(2000);
    let anchored = format!("{lines}anchor\n");
    let existing = "anchor\n".repeat(2000);
    let appended = format!("{existing}{lines}");
    for (before, after, added, removed) in [
        (None, Some(lines.as_str()), 2000, 0),
        (Some(lines.as_str()), None, 0, 2000),
        (Some("anchor\n"), Some(anchored.as_str()), 2000, 0),
        (Some(anchored.as_str()), Some("anchor\n"), 0, 2000),
        (Some("different\n"), Some(lines.as_str()), 2000, 1),
        (Some(lines.as_str()), Some("different\n"), 1, 2000),
        (Some(existing.as_str()), Some(appended.as_str()), 2000, 0),
        (Some(appended.as_str()), Some(existing.as_str()), 0, 2000),
    ] {
        let actual = FileDiff::between(
            before.map(str::as_bytes),
            after.map(str::as_bytes),
            8192,
            100_000,
        )?
        .ok_or("expected changed file")?;
        assert_eq!((actual.added, actual.removed), (added, removed));
        let patch = diffy::Patch::from_str(&actual.patch)?;
        assert_eq!(
            diffy::apply(before.unwrap_or_default(), &patch)?,
            after.unwrap_or_default()
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn invalid_publications_validate_before_hashing_or_reading() -> Result<()> {
    let fixture = Fixture::new().await?;
    let base = fixture.source("absent").await?;
    futures::executor::block_on(async {});
    let mut previous_bytes = None;
    for bytes in [8192, 1_048_576] {
        let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
        let publication = DiffPublication {
            previous: base.generation.clone(),
            generation: base.generation.clone(),
            paths: vec![DiffPath {
                path: format!("\0{}", "x".repeat(bytes)),
                base: None,
                current: None,
            }],
        };
        let reader = Reader {
            inner: fixture.reader.as_ref(),
            reads: AtomicUsize::new(0),
            expected_limits: None,
            pause_first: None,
        };
        let mut result = Ok(Vec::new());
        let allocation = acyclic_test_support::measure(|| {
            result = futures::executor::block_on(view.apply(&publication, &reader, limits()));
        });
        assert!(matches!(result, Err(acyclic_harness::Error::Invalid(_))));
        if let Some(previous) = previous_bytes {
            assert_eq!(allocation.bytes_total, previous);
        }
        previous_bytes = Some(allocation.bytes_total);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        assert_eq!(view.generation(), &base.generation);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn publication_replay_hashing_does_not_allocate_an_encoded_copy() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("anchor", "before", "base").await?;
    let base = fixture.source("anchor").await?;
    fixture.put("anchor", "after", "edit").await?;
    fixture.put("anchor", "before", "restore").await?;
    let current = fixture.source("anchor").await?;
    futures::executor::block_on(async {});
    let mut previous_bytes = None;
    for directories in [4, 1024] {
        let prefix = "z/".repeat(directories);
        let mut paths = vec![DiffPath {
            path: "anchor".into(),
            base: Some(base.clone()),
            current: Some(current.clone()),
        }];
        paths.extend((0..256).map(|index| DiffPath {
            path: format!("{prefix}{index}.txt"),
            base: None,
            current: None,
        }));
        let publication = DiffPublication {
            previous: base.generation.clone(),
            generation: current.generation.clone(),
            paths,
        };
        let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
        let reader = Reader {
            inner: fixture.reader.as_ref(),
            reads: AtomicUsize::new(0),
            expected_limits: None,
            pause_first: None,
        };
        let limits = DiffLimits {
            changes: 512,
            ..limits()
        };
        view.apply(&publication, &reader, limits).await?;
        let mut result = Ok(Vec::new());
        let allocation = acyclic_test_support::measure(|| {
            result = futures::executor::block_on(view.apply(&publication, &reader, limits));
        });
        assert!(result?.is_empty());
        if let Some(previous) = previous_bytes {
            assert_eq!(allocation.bytes_total, previous);
        }
        previous_bytes = Some(allocation.bytes_total);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
    }
    Ok(())
}

impl Fixture {
    async fn new() -> Result<Self> {
        let provider = ProviderRef::new("diff".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            provider,
            "sources".into(),
            VolumeClass::SessionShared,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            "diff",
            [9; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "test".into(),
            },
        );
        let scope = issuer
            .issue(
                "owner",
                &Capabilities::new([
                    volume.capability(VolumeOperation::Read, &mut None)?,
                    volume.capability(VolumeOperation::Write, &mut None)?,
                    volume.directory_read_capability("", &mut None)?,
                ]),
                Some(agent),
                None,
                &mut None,
            )
            .expect("scope");
        host.create_volume(&volume).await?;
        let verifier = issuer.verifier(&mut None).expect("verifier");
        let writer = ContentGrant::verify(
            &verifier,
            &scope,
            &volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 32_768, None)?;
        Ok(Self {
            host,
            root: ContextRoot {
                volume,
                directory: String::new(),
            },
            writer,
            reader,
        })
    }
    async fn put(&self, path: &str, text: &str, key: &str) -> Result<()> {
        self.host
            .put_content(
                &self.root.volume,
                &self.writer,
                path,
                text.as_bytes(),
                "text/plain",
                "source",
                1_000_000,
                &IdempotencyKey::new(key)?,
                None,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await?;
        Ok(())
    }
    async fn source(&self, path: &str) -> Result<PinnedContextPath> {
        let page = self
            .reader
            .list_private_directory(&self.root.volume, "", "", None, None, 128, None, None)?
            .await?;
        Ok(PinnedContextPath {
            root: self.root.clone(),
            generation: page.generation,
            path: path.into(),
        })
    }
}

struct Reader<'a> {
    inner: &'a dyn ContentResidencyVerifier,
    reads: AtomicUsize,
    expected_limits: Option<&'a [u64]>,
    pause_first: Option<&'a (dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync)>,
}
impl ContentResidencyVerifier for Reader<'_> {
    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a acyclic_harness::resources::GenerationRef>,
        maximum_bytes: u64,
        access: acyclic_harness::conversation::ContentAccess<'a, 'io>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            (
                acyclic_harness::conversation::FileRef,
                acyclic_stream::OwnedBytes,
                Option<acyclic_harness::conversation::ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        let acyclic_harness::conversation::ContentAccess { ceiling, io } = access;

        let index = self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(limits) = self.expected_limits {
            assert_eq!(limits.get(index), Some(&maximum_bytes));
        }
        let read = self.inner.read_private_path(
            volume,
            prefix,
            path,
            generation,
            maximum_bytes,
            acyclic_harness::conversation::ContentAccess { ceiling, io },
        )?;
        if index == 0 {
            if let Some(pause) = self.pause_first {
                return acyclic_harness::conversation::BoundedContentFuture::new(
                    io,
                    || async move {
                        pause().await;
                        read.await
                    },
                );
            }
        }
        Ok(read)
    }
}

fn limits() -> DiffLimits {
    DiffLimits {
        files: 32,
        changes: 16,
        file_bytes: 1024,
        render_bytes: 8192,
        work: 1_048_576,
    }
}

#[tokio::test]
async fn a_large_addition_does_not_terminate_the_follow() -> Result<()> {
    let fixture = Fixture::new().await?;
    let base = fixture.source("added.txt").await?;
    fixture.put("added.txt", &"a\n".repeat(2000), "add").await?;
    let current = fixture.source("added.txt").await?;
    let publication = DiffPublication {
        previous: base.generation.clone(),
        generation: current.generation.clone(),
        paths: vec![DiffPath {
            path: "added.txt".into(),
            base: None,
            current: Some(current),
        }],
    };
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation)?;
    let source = futures::stream::iter([Ok(publication.clone()), Ok(publication.clone())]);
    let mut follow = view.follow(
        Box::pin(source),
        fixture.reader.as_ref(),
        DiffLimits {
            file_bytes: 8192,
            ..limits()
        },
    );
    assert_eq!(
        follow.next().await.transpose()?,
        Some(vec!["added.txt".into()])
    );
    assert_eq!(follow.view().line_counts(), (2000, 0));
    assert_eq!(follow.view().generation(), &publication.generation);
    assert_eq!(follow.next().await.transpose()?, Some(vec![]));
    assert!(follow.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn exhausted_work_stops_before_reading_the_next_file() -> Result<()> {
    let fixture = Fixture::new().await?;
    let base = fixture.source("absent").await?;
    fixture.put("binary", "\0", "binary").await?;
    fixture.put("large", &"x".repeat(16_384), "large").await?;
    let current = fixture.source("large").await?;
    let publication = DiffPublication {
        previous: base.generation.clone(),
        generation: current.generation.clone(),
        paths: ["binary", "large"]
            .into_iter()
            .map(|path| DiffPath {
                path: path.into(),
                base: None,
                current: Some(PinnedContextPath {
                    path: path.into(),
                    ..current.clone()
                }),
            })
            .collect(),
    };
    let reader = Reader {
        inner: fixture.reader.as_ref(),
        reads: AtomicUsize::new(0),
        expected_limits: None,
        pause_first: None,
    };
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation)?;
    let reader = Reader {
        expected_limits: Some(&[1]),
        ..reader
    };
    let before = view.clone();
    let result = view
        .apply(
            &publication,
            &reader,
            DiffLimits {
                file_bytes: 32_768,
                work: 1,
                ..limits()
            },
        )
        .await;
    assert!(
        matches!(result, Err(acyclic_harness::Error::Invalid(reason))
        if reason == "diff computation work bound exceeded")
    );
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);

    assert_eq!(view, before);
    Ok(())
}

#[tokio::test]
async fn endpoint_reads_use_only_the_remaining_input_allowance() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("a.txt", "a", "base").await?;
    let base = fixture.source("a.txt").await?;
    fixture.put("a.txt", "bc", "edit").await?;
    let current = fixture.source("a.txt").await?;
    let publication = DiffPublication {
        previous: base.generation.clone(),
        generation: current.generation.clone(),
        paths: vec![DiffPath {
            path: "a.txt".into(),
            base: Some(base.clone()),
            current: Some(current),
        }],
    };
    let reader = Reader {
        inner: fixture.reader.as_ref(),
        reads: AtomicUsize::new(0),
        expected_limits: None,
        pause_first: None,
    };
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation)?;
    let reader = Reader {
        expected_limits: Some(&[2, 1]),
        ..reader
    };
    let before = view.clone();
    assert!(
        view.apply(
            &publication,
            &reader,
            DiffLimits {
                work: 2,
                ..limits()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(reader.reads.load(Ordering::SeqCst), 2);

    assert_eq!(view, before);
    Ok(())
}

#[tokio::test]
async fn unchanged_baseline_snapshot_initializes_once_without_reads() -> Result<()> {
    let fixture = Fixture::new().await?;
    let base = fixture.source("not-present.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    let snapshot = DiffPublication {
        previous: base.generation.clone(),
        generation: base.generation.clone(),
        paths: vec![],
    };
    let mut conflict = snapshot.clone();
    conflict.paths.push(DiffPath {
        path: "a.txt".into(),
        base: None,
        current: None,
    });
    let reader = Reader {
        inner: fixture.reader.as_ref(),
        reads: AtomicUsize::new(0),
        expected_limits: None,
        pause_first: None,
    };
    {
        let source =
            futures::stream::iter([Ok(snapshot.clone()), Ok(snapshot.clone()), Ok(conflict)]);
        let mut follow = view.follow(Box::pin(source), &reader, limits());
        assert!(
            follow
                .next()
                .await
                .transpose()?
                .is_some_and(|paths| paths.is_empty())
        );
        assert!(follow.view().files().is_empty());
        assert!(
            follow
                .next()
                .await
                .transpose()?
                .is_some_and(|paths| paths.is_empty())
        );
        assert!(matches!(
            follow.next().await,
            Some(Err(acyclic_harness::Error::Conflict(_)))
        ));
        assert!(follow.next().await.is_none());
        assert_eq!(follow.view().generation(), &base.generation);
    }
    assert!(view.apply(&snapshot, &reader, limits()).await?.is_empty());
    assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn render_bound_stops_before_reading_later_files_and_preserves_the_view() -> Result<()> {
    let fixture = Fixture::new().await?;
    let base = fixture.source("not-present.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    let text = "x".repeat(16_384);
    for index in 0..8 {
        fixture
            .put(&format!("big-{index}.txt"), &text, &format!("put-{index}"))
            .await?;
    }
    let current = fixture.source("big-0.txt").await?;
    let publication = DiffPublication {
        previous: base.generation,
        generation: current.generation.clone(),
        paths: (0..8)
            .map(|index| {
                let path = format!("big-{index}.txt");
                DiffPath {
                    path: path.clone(),
                    base: None,
                    current: Some(PinnedContextPath {
                        path,
                        ..current.clone()
                    }),
                }
            })
            .collect(),
    };
    let reader = Reader {
        inner: fixture.reader.as_ref(),
        reads: AtomicUsize::new(0),
        expected_limits: None,
        pause_first: None,
    };
    let before = serde_json::to_string(&view)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    assert!(
        view.apply(
            &publication,
            &reader,
            DiffLimits {
                file_bytes: 32_768,
                render_bytes: 64,
                ..limits()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        serde_json::to_string(&view)
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
        before
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_a_read_keeps_the_received_publication_for_the_next_read() -> Result<()> {
    use futures::FutureExt as _;
    let fixture = Fixture::new().await?;
    fixture.put("a.txt", "before\n", "base").await?;
    let base = fixture.source("a.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    fixture.put("a.txt", "after\n", "edit").await?;
    let current = fixture.source("a.txt").await?;
    let publication = DiffPublication {
        previous: base.generation.clone(),
        generation: current.generation.clone(),
        paths: vec![DiffPath {
            path: "a.txt".into(),
            base: Some(base),
            current: Some(current),
        }],
    };
    let reader = Reader {
        inner: fixture.reader.as_ref(),
        reads: AtomicUsize::new(0),
        expected_limits: None,
        pause_first: Some(&|| Box::pin(futures::future::pending())),
    };
    let mut follow = view.follow(
        Box::pin(futures::stream::iter([Ok(publication.clone())])),
        &reader,
        limits(),
    );
    assert!(follow.next().now_or_never().is_none());
    assert!(follow.view().files().is_empty());
    assert_eq!(follow.next().await.transpose()?, Some(vec!["a.txt".into()]));
    assert_eq!(follow.view().generation(), &publication.generation);
    assert_eq!(follow.view().line_counts(), (1, 1));
    assert_eq!(reader.reads.load(Ordering::SeqCst), 3);
    assert!(follow.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn deletion_only_publications_keep_the_published_generation() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("a.txt", "removed\n", "base").await?;
    let base = fixture.source("a.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    let workspace = acyclic_harness::filesystem::workspace_ref(
        fixture.root.volume.provider().clone(),
        &fixture.root.volume.storage_name(&mut None)?,
    )?;
    let generation = fixture
        .host
        .apply(
            &workspace,
            Some(&base.generation),
            &[acyclic_harness::filesystem::WorkspaceMutation::Remove {
                path: "/a.txt".into(),
            }],
            &IdempotencyKey::new("delete")?,
        )
        .await?;
    let publication = DiffPublication {
        previous: base.generation.clone(),
        generation: generation.clone(),
        paths: vec![DiffPath {
            path: "a.txt".into(),
            base: Some(base),
            current: None,
        }],
    };
    let reader = Reader {
        inner: fixture.reader.as_ref(),
        reads: AtomicUsize::new(0),
        expected_limits: None,
        pause_first: None,
    };
    assert_eq!(
        view.apply(&publication, &reader, limits()).await?,
        ["a.txt"]
    );
    assert_eq!(view.generation(), &generation);
    assert_eq!(view.line_counts(), (0, 1));
    assert_eq!(
        view.files()["a.txt"].kind,
        acyclic_harness::client::DiffKind::Removed
    );
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn query_follow_waits_for_push_and_stops_on_a_cursor_gap() -> Result<()> {
    use futures::FutureExt as _;
    let fixture = Fixture::new().await?;
    fixture.put("a.txt", "before\n", "base").await?;
    let base = fixture.source("a.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    fixture.put("a.txt", "after\n", "edit").await?;
    let current = fixture.source("a.txt").await?;
    let publication = DiffPublication {
        previous: base.generation.clone(),
        generation: current.generation.clone(),
        paths: vec![DiffPath {
            path: "a.txt".into(),
            base: Some(base),
            current: Some(current),
        }],
    };
    let (mut sender, receiver) = futures::channel::mpsc::channel(1);
    {
        let mut follow = view.follow(Box::pin(receiver), fixture.reader.as_ref(), limits());
        assert!(follow.next().now_or_never().is_none());
        assert!(follow.view().files().is_empty());
        sender
            .try_send(Ok(publication.clone()))
            .expect("bounded push");
        assert_eq!(follow.next().await.expect("publication")?, ["a.txt"]);
        assert_eq!(follow.view().line_counts(), (1, 1));
        assert_eq!(
            follow.view().files()["a.txt"].patch,
            "--- original\n+++ modified\n@@ -1 +1 @@\n-before\n+after\n"
        );
        assert!(follow.next().now_or_never().is_none());
        assert_eq!(follow.view().generation(), &publication.generation);
        let mut gap = publication.clone();
        gap.paths.clear();
        sender.try_send(Ok(gap)).expect("bounded push");
        assert!(follow.next().await.expect("cursor error").is_err());
        assert!(follow.next().await.is_none());
    }
    assert_eq!(view.generation(), &publication.generation);
    assert_eq!(view.line_counts(), (1, 1));
    Ok(())
}

#[tokio::test]
async fn publications_update_only_named_files_and_keep_the_fork_base() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("a.txt", "base\n", "a-base").await?;
    fixture.put("untouched.txt", "keep\n", "untouched").await?;
    let base = fixture.source("a.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    let reader = Reader {
        inner: fixture.reader.as_ref(),
        reads: AtomicUsize::new(0),
        expected_limits: None,
        pause_first: None,
    };
    for (index, current, expected) in [
        (1, "first\n", 1),
        (2, "second\nmore\n", 1),
        (3, "base\n", 0),
    ] {
        fixture
            .put("a.txt", current, &format!("edit-{index}"))
            .await?;
        let source = fixture.source("a.txt").await?;
        let publication = DiffPublication {
            previous: view.generation().clone(),
            generation: source.generation.clone(),
            paths: vec![DiffPath {
                path: "a.txt".into(),
                base: Some(base.clone()),
                current: Some(source),
            }],
        };
        assert_eq!(
            view.apply(&publication, &reader, limits()).await?,
            ["a.txt"]
        );
        assert_eq!(view.files().len(), expected);
        if let Some(diff) = view.files().get("a.txt") {
            assert!(diff.patch.contains("-base\n"));
            assert!(!diff.patch.contains("-first\n"));
        }
        assert_eq!(reader.reads.load(Ordering::SeqCst), index * 2);
        let mut resumed = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
        let snapshot = DiffPublication {
            previous: base.generation.clone(),
            ..publication.clone()
        };
        resumed
            .apply(&snapshot, fixture.reader.as_ref(), limits())
            .await?;
        assert_eq!(resumed.files(), view.files());
        assert_eq!(resumed.line_counts(), view.line_counts());
        assert_eq!(resumed.generation(), view.generation());
        assert!(
            view.apply(&publication, &reader, limits())
                .await?
                .is_empty()
        );
        assert_eq!(reader.reads.load(Ordering::SeqCst), index * 2);
    }
    assert_eq!(view.line_counts(), (0, 0));
    Ok(())
}

#[tokio::test]
async fn a_full_view_can_replace_a_file_in_one_atomic_publication() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("old.txt", "baseline\n", "base").await?;
    let base = fixture.source("old.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    fixture.put("old.txt", "old\n", "old").await?;
    let old = fixture.source("old.txt").await?;
    view.apply(
        &DiffPublication {
            previous: base.generation.clone(),
            generation: old.generation.clone(),
            paths: vec![DiffPath {
                path: "old.txt".into(),
                base: Some(base.clone()),
                current: Some(old),
            }],
        },
        fixture.reader.as_ref(),
        DiffLimits {
            files: 1,
            ..limits()
        },
    )
    .await?;
    fixture.put("new.txt", "new\n", "new").await?;
    fixture.put("old.txt", "baseline\n", "restore").await?;
    let new = fixture.source("new.txt").await?;
    let restored = PinnedContextPath {
        path: "old.txt".into(),
        ..new.clone()
    };
    let generation = new.generation.clone();
    let new_patch_bytes = FileDiff::between(None, Some(b"new\n"), 8192, 32_768)?
        .ok_or_else(|| acyclic_harness::Error::Invalid("new file missing".into()))?
        .patch
        .len();
    assert!(view.files()["old.txt"].patch.len() > new_patch_bytes);
    view.apply(
        &DiffPublication {
            previous: view.generation().clone(),
            generation,
            paths: vec![
                DiffPath {
                    path: "new.txt".into(),
                    base: None,
                    current: Some(new),
                },
                DiffPath {
                    path: "old.txt".into(),
                    base: Some(base),
                    current: Some(restored),
                },
            ],
        },
        fixture.reader.as_ref(),
        DiffLimits {
            files: 1,
            render_bytes: new_patch_bytes,
            ..limits()
        },
    )
    .await?;
    assert_eq!(view.files().keys().collect::<Vec<_>>(), ["new.txt"]);
    assert_eq!(view.line_counts(), (1, 0));
    Ok(())
}

#[tokio::test]
async fn rejected_publications_leave_the_view_and_cursor_unchanged() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.put("a.txt", "base\n", "base").await?;
    let base = fixture.source("a.txt").await?;
    let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())?;
    fixture.put("a.txt", "changed\n", "edit").await?;
    let source = fixture.source("a.txt").await?;
    let publication = DiffPublication {
        previous: base.generation.clone(),
        generation: source.generation.clone(),
        paths: vec![DiffPath {
            path: "a.txt".into(),
            base: Some(base.clone()),
            current: Some(source.clone()),
        }],
    };
    let before = serde_json::to_string(&view).expect("JSON");
    for limits in [
        DiffLimits {
            file_bytes: 1,
            ..limits()
        },
        DiffLimits {
            render_bytes: 1,
            ..limits()
        },
        DiffLimits {
            changes: 0,
            ..limits()
        },
    ] {
        assert!(
            view.apply(&publication, fixture.reader.as_ref(), limits)
                .await
                .is_err()
        );
        assert_eq!(serde_json::to_string(&view).expect("JSON"), before);
    }
    let mut duplicate = publication.clone();
    duplicate.paths.push(duplicate.paths[0].clone());
    assert!(
        view.apply(&duplicate, fixture.reader.as_ref(), limits())
            .await
            .is_err()
    );
    let mut wrong_source = publication.clone();
    wrong_source.paths[0].base = Some(source);
    assert!(
        view.apply(&wrong_source, fixture.reader.as_ref(), limits())
            .await
            .is_err()
    );
    view.apply(&publication, fixture.reader.as_ref(), limits())
        .await?;
    let mut conflicting = publication.clone();
    conflicting.paths.clear();
    assert!(
        view.apply(&conflicting, fixture.reader.as_ref(), limits())
            .await
            .is_err()
    );
    Ok(())
}

#[path = "../client_diff_data.rs"]
mod data;
#[path = "../support/file_diff_case.rs"]
mod diff_case;
#[path = "../support/file_diff_oracle.rs"]
mod diff_oracle;
#[path = "../support/scenario_data.rs"]
mod scenario_data;
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
