use super::*;

/// Admission proof for exact Machines checkpoint revisions selected by a parent fork.
impl ForkSeedVerifier for MachinesHost {
    fn provider(&self) -> &ProviderRef {
        &self.provider_ref
    }

    fn verify<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Unsupported,
                "Machines checkpoint inspection has no bounded provider boundary",
            ));
        }
        crate::fork::admit_fork_future(work, |work| async move {
            seed.validate(work)?;
            for capture in &seed.resources {
                let revision = &capture.revision;
                if revision.provider() != &self.provider_ref {
                    continue;
                }
                let ResourceRevision::Process(checkpoint) = revision else {
                    return Err(Error::Unsupported(
                        "Machines fork verifier cannot prove this resource".into(),
                    ));
                };
                let observed = self.inspect_checkpoint(checkpoint).await?;
                if !observed.forkable {
                    return Err(Error::Conflict(
                        "selected Machines checkpoint is not forkable".into(),
                    ));
                }
            }
            Ok(())
        })
    }
}

/// Read-only capture of a forkable process checkpoint.
impl ForkCaptureProvider for MachinesHost {
    fn provider(&self) -> &ProviderRef {
        &self.provider_ref
    }

    fn capture<'a>(
        &'a self,
        _request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Capture>> {
        Box::pin(async move {
            let ResourceRevision::Process(checkpoint) = &selection.revision else {
                return Ok(Capture::Unsupported(
                    "Machines captures only process checkpoints".into(),
                ));
            };
            let observed = self.inspect_checkpoint(checkpoint).await?;
            if !observed.forkable {
                return Ok(Capture::Unsupported(
                    "selected Machines checkpoint is not forkable".into(),
                ));
            }
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
        // Inspection alone cannot have created a provider effect.
        Box::pin(async move { self.capture(request, selection).await.map(Some) })
    }
}
