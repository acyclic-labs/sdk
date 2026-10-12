use super::*;

impl ForkCaptureProvider for ObjectContentStore {
    fn provider(&self) -> &ProviderRef {
        self.volume.provider()
    }

    fn capture<'a>(
        &'a self,
        _request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Capture>> {
        Box::pin(async move {
            let ResourceRevision::Artifact(artifact) = &selection.revision else {
                return Ok(Capture::Unsupported(
                    "Objects captures only artifacts".into(),
                ));
            };
            self.verify_artifact(artifact).await?;
            Ok(Capture::Captured(CapturedResource {
                source: selection.revision.clone(),
                revision: selection.revision.clone(),
            }))
        })
    }

    fn reconcile<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Option<Capture>>> {
        // Content verification is read-only and safe to observe again.
        Box::pin(async move { self.capture(request, selection).await.map(Some) })
    }
}

impl ForkSeedVerifier for ObjectContentStore {
    fn provider(&self) -> &ProviderRef {
        self.volume.provider()
    }

    fn verify<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        require_provider_io(work)?;
        crate::fork::admit_fork_future(work, |_| async move {
            seed.validate(&mut None)?;
            if seed.child_private_volume.provider() == self.volume.provider() {
                return Err(Error::Unsupported(
                    "Objects cannot verify a child-private workspace".into(),
                ));
            }
            for resource in &seed.resources {
                let revision = &resource.revision;
                if revision.provider() != self.volume.provider() {
                    continue;
                }
                let ResourceRevision::Artifact(artifact) = revision else {
                    return Err(Error::Unsupported(
                        "Objects cannot verify this fork resource".into(),
                    ));
                };
                self.verify_artifact(artifact).await?;
            }
            Ok(())
        })
    }

    fn read_manifest<'a, 'w: 'a>(
        &'a self,
        _seed: &'a ForkSeed,
        manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>>
    {
        require_provider_io(work)?;
        crate::fork::admit_fork_future(work, |_| async move {
            Ok((self.read_exact(manifest, self.maximum_bytes).await?, 0))
        })
    }

    fn verify_file<'a, 'w: 'a>(
        &'a self,
        _seed: &'a ForkSeed,
        file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        require_provider_io(work)?;
        crate::fork::admit_fork_future(work, |_| async move {
            self.read_exact(file, self.maximum_bytes).await.map(|_| ())
        })
    }
}

fn require_provider_io(work: &Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
    if let Some(io) = work.as_deref() {
        io.check_cancellation()?;
        return Err(crate::fork::boundary_error(
            work,
            crate::wire::ErrorCode::Unsupported,
            "Objects fork verification has no original-work provider",
        ));
    }
    Ok(())
}
