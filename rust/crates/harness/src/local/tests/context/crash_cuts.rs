use super::*;
use crate::conversation::{BoundedContentFuture, ContentHold, ContentIo};
use std::sync::atomic::{AtomicUsize, Ordering};

pub(in crate::local::tests) struct Interrupted<'a> {
    publisher: &'a dyn ContentPublisher,
    cut: usize,
    after: bool,
    calls: AtomicUsize,
}

impl<'a> Interrupted<'a> {
    pub(in crate::local::tests) fn new(
        publisher: &'a dyn ContentPublisher,
        cut: usize,
        after: bool,
    ) -> Self {
        Self {
            publisher,
            cut,
            after,
            calls: AtomicUsize::new(0),
        }
    }
}

impl ContentPublisher for Interrupted<'_> {
    fn volume(&self) -> &VolumeRef {
        self.publisher.volume()
    }

    fn observe_staged_once<'a>(
        &'a self,
        operation: OperationId,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        self.publisher.observe_staged_once(operation, maximum, io)
    }

    fn stage_once<'a>(
        &'a self,
        operation: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media: &'a str,
        display: &'a str,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let selected = self.calls.fetch_add(1, Ordering::Relaxed) == self.cut;
            let interrupted = || Error::Storage("injected publication interruption".into());
            if selected && !self.after {
                return Err(interrupted());
            }
            let file = self
                .publisher
                .stage_once(operation, path, bytes, media, display, io)?
                .await?;
            if selected {
                // The real provider has committed the bytes and retry receipt;
                // discard its acknowledgement before the caller sees a ref.
                return Err(interrupted());
            }
            Ok(file)
        })
    }
}

async fn source(
    harness: &LocalHarness,
    name: &str,
    text: Option<&str>,
    checkout: bool,
) -> Result<()> {
    #[cfg(feature = "native-execution")]
    if checkout {
        let path = harness.workspace().unwrap().path.join(name);
        match text {
            Some(text) => std::fs::write(path, text),
            None => std::fs::remove_file(path),
        }
        .map_err(super::super::super::storage)?;
        return Ok(());
    }
    let _ = checkout;
    if let Some(text) = text {
        instruction(harness, name, text).await?;
    } else {
        harness
            .files
            .apply(
                &crate::filesystem::workspace_ref(
                    harness.project.provider().clone(),
                    &harness.project.storage_name(&mut None)?,
                )?,
                None,
                &[crate::filesystem::WorkspaceMutation::Remove {
                    path: format!("/{name}"),
                }],
                &crate::IdempotencyKey::new(format!("remove-{}", OperationId::new()))?,
            )
            .await?;
    }
    Ok(())
}

async fn sweep_case(checkout: bool, cut: usize, after: bool, publications: usize) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let provider = text();
    let project = if checkout {
        None
    } else {
        Some(volume(directory.path()).await?)
    };
    // Every reopen of the checkout shares one state directory outside it.
    let state = tempfile::tempdir().map_err(super::super::super::storage)?;
    let config = || -> Result<LocalHarnessConfig> {
        match &project {
            Some(project) => volume_config(directory.path(), project.clone(), provider.clone()),
            None => Ok(crate::test_support::local_harness_config(
                directory.path(),
                (model()?, provider.clone()),
            )?
            .state_directory(state.path())),
        }
    };
    let harness = LocalHarness::open(config()?).await?;
    source(&harness, "AGENTS.md", Some("original agents"), checkout).await?;
    source(&harness, "CLAUDE.md", Some("original claude"), checkout).await?;
    let operation = OperationId::new();
    let publisher = harness.input_publisher()?;
    let interrupted = Interrupted {
        publisher: publisher.as_ref(),
        cut,
        after,
        calls: AtomicUsize::new(0),
    };
    assert!(
        matches!(harness.stage_turn_with(operation, "prompt", &interrupted).await, Err(Error::Storage(message)) if message == "injected publication interruption")
    );
    assert_eq!(interrupted.calls.load(Ordering::Relaxed), cut + 1);
    assert!(provider.requests().is_empty());
    // The manifest is followed by input and mailbox publication on both paths.
    let manifest_cut = publications - 3;
    let published = cut > manifest_cut || (cut == manifest_cut && after);
    let original = harness.retained_context_manifest(operation).await.ok();
    assert_eq!(original.is_some(), published);
    source(&harness, "AGENTS.md", Some("replacement agents"), checkout).await?;
    source(&harness, "CLAUDE.md", None, checkout).await?;
    drop(publisher);
    drop(harness);
    let reopened = LocalHarness::open(config()?).await?;
    let task = admitted(reopened.submit(operation, "prompt").await?)?;
    let (reference, manifest) = manifest(&reopened, operation).await?;
    if let Some(original) = original {
        assert_eq!(reference, original);
    }
    let reader = reopened.context_reader()?;
    let mut contents = Vec::new();
    for file in &manifest.discovery.instructions {
        contents.push(
            reader
                .read(file, reopened.limits.file_bytes, None)?
                .await?
                .as_ref()
                .to_vec(),
        );
    }
    let expected: &[&[u8]] = if published {
        &[b"original agents", b"original claude"]
    } else {
        &[b"replacement agents"]
    };
    assert_eq!(
        contents, expected,
        "checkout={checkout}, cut={cut}, after={after}"
    );
    assert_eq!(reopened.resume(task).await?.text, "done");
    assert_eq!(provider.requests().len(), 1);
    // A second resume replays the completed turn without a provider resend.
    assert_eq!(reopened.resume(task).await?.text, "done");
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn volume_recovery_covers_every_publication_and_lost_acknowledgement() -> Result<()> {
    futures::future::try_join_all((0..3).flat_map(|cut| {
        [false, true].map(move |after| WORKERS.spawn(sweep_case(false, cut, after, 3)))
    }))
    .await
    .map_err(super::super::super::storage)?
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    Ok(())
}

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn checkout_recovery_covers_every_publication_and_lost_acknowledgement() -> Result<()> {
    futures::future::try_join_all((0..5).flat_map(|cut| {
        [false, true].map(move |after| WORKERS.spawn(sweep_case(true, cut, after, 5)))
    }))
    .await
    .map_err(super::super::super::storage)?
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    Ok(())
}
