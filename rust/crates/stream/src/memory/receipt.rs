impl MemoryStream {
    fn receipt(
        &self,
        envelope: &CommittedEnvelope,
        time: u64,
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
        let receipt = crate::CommitReceipt::project(envelope, time, appends, forks, || {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(inspect(&work.meter, || ()))?;
            }
            Ok(())
        })?;
        Ok((receipt, allocation))
    }
}
