//! Native capture must reproduce host alias groups without changing uncaptured names.
#![cfg(unix)]

use acyclic_fs::kernel::NamespacePath;
use acyclic_fs::model::{CheckoutMode, GenerationSelector, VolumeLimits};
use acyclic_fs::{
    CancellationToken, CaptureError, CaptureOptions, CapturePolicy, FileId, Fs,
    MemoryAuthorityBackend, MemoryObjectBackend, OperationId, WatchBatch, WatchChange, WorkBudget,
    Workspace, capture_baseline, capture_paths, capture_root_identity, capture_watch_batch,
    seal_checkout,
};
use std::error::Error;
use std::os::unix::fs::MetadataExt as _;

type TestWorkspace = Workspace<MemoryAuthorityBackend, MemoryObjectBackend>;
type TestResult = Result<(), Box<dyn Error>>;

struct Fixture {
    source: tempfile::TempDir,
    workspace: TestWorkspace,
    options: CaptureOptions,
    cancellation: CancellationToken,
}

impl Fixture {
    async fn new() -> Result<Self, Box<dyn Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let source = tempfile::tempdir()?;
        let cancellation = CancellationToken::new();
        let workspace = Fs::memory()
            .create_workspace("capture-links", WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let options = CaptureOptions {
            source_root: source.path().to_owned(),
            expected_root_identity: capture_root_identity(source.path())?,
            maximum_paths: 16,
            maximum_extent_spans: 8,
        };
        Ok(Self {
            source,
            workspace,
            options,
            cancellation,
        })
    }

    fn replace_group(&self, names: &[&str], bytes: &[u8]) -> TestResult {
        let (first, remaining) = names
            .split_first()
            .ok_or("a replacement hard-link group must not be empty")?;
        let temporary = self.source.path().join("save.tmp");
        std::fs::write(&temporary, bytes)?;
        let first = self.source.path().join(first);
        std::fs::rename(temporary, &first)?;
        for name in remaining {
            let destination = self.source.path().join(name);
            if destination.exists() {
                std::fs::remove_file(&destination)?;
            }
            std::fs::hard_link(&first, destination)?;
        }
        Ok(())
    }

    async fn capture(&self) -> TestResult {
        let mut checkout = self
            .workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &self.cancellation,
            )
            .await?
            .value;
        capture_baseline(
            &mut checkout,
            &self.options,
            &CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &self.cancellation,
        )
        .await?;
        seal_checkout(
            &mut checkout,
            OperationId::new(),
            WorkBudget::UNBOUNDED,
            &self.cancellation,
        )
        .await?;
        Ok(())
    }

    async fn assert_file(&self, name: &str, bytes: &[u8]) -> Result<FileId, Box<dyn Error>> {
        let path = format!("/{name}");
        let captured = self
            .workspace
            .read(&path, 64, WorkBudget::UNBOUNDED, &self.cancellation)
            .await?
            .value;
        assert_eq!(captured.as_ref(), bytes, "captured {name}");
        Ok(self
            .workspace
            .stat(&path, WorkBudget::UNBOUNDED, &self.cancellation)
            .await?
            .value
            .file_id)
    }
}

fn path(name: &str) -> Result<NamespacePath, Box<dyn Error>> {
    let limits = VolumeLimits::default();
    Ok(NamespacePath::from_portable(
        &acyclic_fs::path::PortablePath::parse(name, limits)?,
        limits,
    )?)
}

#[tokio::test]
async fn atomic_save_splits_either_name_of_a_hard_link() -> TestResult {
    for (changed, unchanged) in [("a", "z"), ("z", "a")] {
        let fixture = Fixture::new().await?;
        fixture.replace_group(&["a", "z"], b"before")?;
        fixture.capture().await?;
        fixture.replace_group(&[changed], b"after!")?;
        for _ in 0..2 {
            fixture.capture().await?;
            let changed_id = fixture.assert_file(changed, b"after!").await?;
            let unchanged_id = fixture.assert_file(unchanged, b"before").await?;
            assert_ne!(changed_id, unchanged_id);
        }
    }
    Ok(())
}

#[tokio::test]
async fn capture_rebuilds_larger_host_alias_partitions() -> TestResult {
    for groups in [
        vec![vec!["a"], vec!["b", "c", "d"]],
        vec![vec!["a", "b"], vec!["c", "d"]],
        vec![vec!["a", "c"], vec!["b", "d"]],
        vec![vec!["a"], vec!["b"], vec!["c"], vec!["d"]],
    ] {
        let fixture = Fixture::new().await?;
        fixture.replace_group(&["a", "b", "c", "d"], b"before")?;
        fixture.capture().await?;
        for (index, group) in groups.iter().enumerate() {
            fixture.replace_group(group, &[b'A' + u8::try_from(index)?; 6])?;
        }
        fixture.capture().await?;
        let mut identities = Vec::new();
        for (index, group) in groups.iter().enumerate() {
            let expected = [b'A' + u8::try_from(index)?; 6];
            let first_id = fixture.assert_file(group[0], &expected).await?;
            assert!(!identities.contains(&first_id));
            identities.push(first_id);
            for name in group {
                assert_eq!(fixture.assert_file(name, &expected).await?, first_id);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn links_outside_the_capture_root_do_not_join_distinct_host_files() -> TestResult {
    let fixture = Fixture::new().await?;
    fixture.replace_group(&["a", "z"], b"before")?;
    fixture.capture().await?;
    let outside = tempfile::tempdir()?;
    std::fs::hard_link(fixture.source.path().join("z"), outside.path().join("old"))?;
    fixture.replace_group(&["a"], b"after!")?;
    std::fs::hard_link(fixture.source.path().join("a"), outside.path().join("new"))?;
    assert_eq!(
        std::fs::metadata(fixture.source.path().join("a"))?.nlink(),
        2
    );
    assert_eq!(
        std::fs::metadata(fixture.source.path().join("z"))?.nlink(),
        2
    );
    fixture.capture().await?;
    assert_ne!(
        fixture.assert_file("a", b"after!").await?,
        fixture.assert_file("z", b"before").await?
    );
    assert_eq!(std::fs::read(outside.path().join("old"))?, b"before");
    assert_eq!(std::fs::read(outside.path().join("new"))?, b"after!");
    Ok(())
}

#[tokio::test]
async fn complete_unchanged_alias_groups_keep_their_file_identity() -> TestResult {
    let fixture = Fixture::new().await?;
    fixture.replace_group(&["a", "z"], b"before")?;
    fixture.capture().await?;
    let original = fixture.assert_file("a", b"before").await?;
    fixture.capture().await?;
    assert_eq!(fixture.assert_file("a", b"before").await?, original);
    assert_eq!(fixture.assert_file("z", b"before").await?, original);
    Ok(())
}

#[tokio::test]
async fn removing_one_alias_preserves_the_surviving_file_identity() -> TestResult {
    for (removed, retained) in [("a", "z"), ("z", "a")] {
        let fixture = Fixture::new().await?;
        fixture.replace_group(&["a", "z"], b"before")?;
        fixture.capture().await?;
        let original = fixture.assert_file(retained, b"before").await?;
        std::fs::remove_file(fixture.source.path().join(removed))?;
        fixture.capture().await?;
        assert_eq!(fixture.assert_file(retained, b"before").await?, original);
    }
    Ok(())
}

#[tokio::test]
async fn replacing_the_last_host_alias_respects_the_capture_scope_and_prior_generation()
-> TestResult {
    for (removed, retained) in [("a", "z"), ("z", "a")] {
        for complete in [true, false] {
            let fixture = Fixture::new().await?;
            fixture.replace_group(&["a", "z"], b"before")?;
            fixture.capture().await?;
            assert_eq!(
                fixture.assert_file("a", b"before").await?,
                fixture.assert_file("z", b"before").await?
            );
            let prior = fixture
                .workspace
                .head(WorkBudget::UNBOUNDED, &fixture.cancellation)
                .await?
                .value;
            std::fs::remove_file(fixture.source.path().join(removed))?;
            fixture.replace_group(&[retained], b"after!")?;
            if complete {
                fixture.capture().await?;
            } else {
                let mut checkout = fixture
                    .workspace
                    .checkout(
                        GenerationSelector::Head,
                        CheckoutMode::tracking_transaction(),
                        WorkBudget::UNBOUNDED,
                        &fixture.cancellation,
                    )
                    .await?
                    .value;
                capture_paths(
                    &mut checkout,
                    &[path(&format!("/{retained}"))?],
                    &fixture.options,
                    WorkBudget::UNBOUNDED,
                    &fixture.cancellation,
                )
                .await?;
                seal_checkout(
                    &mut checkout,
                    OperationId::new(),
                    WorkBudget::UNBOUNDED,
                    &fixture.cancellation,
                )
                .await?;
            }
            let retained_id = fixture.assert_file(retained, b"after!").await?;
            if complete {
                let missing = fixture
                    .workspace
                    .read(
                        &format!("/{removed}"),
                        64,
                        WorkBudget::UNBOUNDED,
                        &fixture.cancellation,
                    )
                    .await;
                assert!(matches!(
                    missing,
                    Err(ref failure) if matches!(failure.error, acyclic_fs::WorkspaceError::NotFound)
                ));
            } else {
                assert_ne!(fixture.assert_file(removed, b"before").await?, retained_id);
            }
            for name in ["a", "z"] {
                let original = prior
                    .read(
                        &format!("/{name}"),
                        64,
                        WorkBudget::UNBOUNDED,
                        &fixture.cancellation,
                    )
                    .await?
                    .value;
                assert_eq!(original.as_ref(), b"before", "pinned {name}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn capturing_one_split_path_preserves_the_uncaptured_alias() -> TestResult {
    let fixture = Fixture::new().await?;
    fixture.replace_group(&["a", "z"], b"before")?;
    fixture.capture().await?;
    fixture.replace_group(&["a"], b"after!")?;
    let mut checkout = fixture
        .workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &fixture.cancellation,
        )
        .await?
        .value;
    capture_paths(
        &mut checkout,
        &[path("/a")?],
        &fixture.options,
        WorkBudget::UNBOUNDED,
        &fixture.cancellation,
    )
    .await?;
    seal_checkout(
        &mut checkout,
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &fixture.cancellation,
    )
    .await?;
    assert_ne!(
        fixture.assert_file("a", b"after!").await?,
        fixture.assert_file("z", b"before").await?
    );
    Ok(())
}

#[tokio::test]
async fn a_watch_hint_for_a_former_alias_requires_a_clean_rescan() -> TestResult {
    let fixture = Fixture::new().await?;
    fixture.replace_group(&["a", "z"], b"before")?;
    fixture.capture().await?;
    fixture.replace_group(&["a"], b"after!")?;
    let mut checkout = fixture
        .workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &fixture.cancellation,
        )
        .await?
        .value;
    let result = capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            original: Default::default(),
            epoch: 7,
            first_sequence: 1,
            next_sequence: 2,
            changes: vec![WatchChange::Modified(path("/a")?)],
        },
        &fixture.options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &fixture.cancellation,
    )
    .await;
    assert!(matches!(
        result,
        Err(ref failure) if matches!(failure.error, CaptureError::RescanRequired { epoch: 7, .. })
    ));
    capture_baseline(
        &mut checkout,
        &fixture.options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &fixture.cancellation,
    )
    .await?;
    seal_checkout(
        &mut checkout,
        OperationId::new(),
        WorkBudget::UNBOUNDED,
        &fixture.cancellation,
    )
    .await?;
    assert_ne!(
        fixture.assert_file("a", b"after!").await?,
        fixture.assert_file("z", b"before").await?
    );
    Ok(())
}
