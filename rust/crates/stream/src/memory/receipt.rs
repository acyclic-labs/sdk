impl MemoryStream {
    fn receipt(
        &self,
        envelope: &CommittedEnvelope,
        revision: crate::Revision,
    ) -> Result<(crate::CommitReceipt, Allocation), StreamError> {
        let mut append_count = 0;
        for mutation in &envelope.mutations {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(inspect(&work.meter, || ()))?;
            }
            append_count += usize::from(matches!(mutation, CommittedMutation::Append(_)));
        }
        let (appends, mut allocation) = self.vector(append_count)?;
        let (forks, claim) = self.vector(envelope.mutations.len() - append_count)?;
        self.absorb(&mut allocation, claim)?;
        let receipt =
            crate::CommitReceipt::project(envelope, revision, appends, forks, |bytes, copied| {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = &self.work {
                    if copied {
                        work.record(work.meter.record_examination(0, bytes as u64))?;
                    } else {
                        work.record(inspect(&work.meter, || ()))?;
                        work.record(work.meter.admit_examination(0, bytes as u64))?;
                    }
                }
                #[cfg(target_arch = "wasm32")]
                let _ = (bytes, copied);
                Ok::<(), StreamError>(())
            })?;
        Ok((receipt, allocation))
    }

    /// The receipt a retry of `request` replays from its retained `envelope`:
    /// the original receipt, skipped soft appends included. Every replay
    /// goes through here.
    fn replayed(
        &self,
        request: &crate::CommitRequest,
        envelope: &CommittedEnvelope,
        revision: crate::Revision,
    ) -> Result<(crate::CommitReceipt, Allocation), StreamError> {
        let (mut receipt, mut claim) = self.receipt(envelope, revision)?;
        let (skipped, skipped_claim) = self.vector(crate::preparation::soft(request))?;
        self.absorb(&mut claim, skipped_claim)?;
        receipt.skipped = crate::preparation::skipped(request, envelope, skipped)?;
        Ok((receipt, claim))
    }
}
