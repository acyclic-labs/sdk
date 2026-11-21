//! A private pre-stage working checkpoint, carried by every later projection.
use super::*;

mod history;
pub(super) mod retirement;

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkingCut {
    version: u32,
    operation: OperationId,
    input_digest: [u8; 32],
    step: u32,
    prior_messages: u64,
    source: FileRef,
    retained: FileRef,
    compaction: FileRef,
    proof: Option<([u8; 32], [u8; 32])>,
}

impl WorkingCut {
    pub(super) fn payload_digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        preparation_digest(
            &(
                "working-context-cut/v2",
                self.version,
                self.operation,
                self.input_digest,
                self.step,
                self.prior_messages,
                &self.source,
                &self.retained,
                &self.compaction,
            ),
            io,
        )
    }
}

impl PreparationRecord for WorkingCut {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            self.source.logical_owned_bytes()?,
            self.retained.logical_owned_bytes()?,
            self.compaction.logical_owned_bytes()?,
        ])
        .map(Some)
    }
}

pub(super) struct RetainedCut<'a> {
    journal: Option<ExecutionJournalCacheId>,
    reference: FileRef,
    cut: WorkingCut,
    // Admitted once under the stock limits when loaded; steps extend copies.
    context: crate::context::AdmittedContext,
    // All resident fields are destroyed before their original custody.
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

#[derive(Default)]
pub(super) struct TailCache<'a> {
    retained: Option<RetainedCut<'a>>,
    prior_offset: u64,
}

pub(super) struct WorkingCompaction<'a> {
    pub(super) input: &'a TurnInput,
    pub(super) step: u32,
    pub(super) prior: usize,
    pub(super) source: &'a crate::context::Context,
    pub(super) retained: &'a crate::context::Context,
    pub(super) proof: &'a crate::context::CompactionReference,
}

impl<'a> StockExecution<'a> {
    /// Copy only messages after the durable cut. Stages receive the retained
    /// pre-stage base and the new tail, never yesterday's stage output.
    pub(super) async fn response_base(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior: &[ModelMessage],
    ) -> Result<(
        ContextInput,
        crate::context::AdmittedContext,
        Option<FileRef>,
    )> {
        self.response_history_base(
            journal,
            input,
            step,
            &mut request::ModelStepHistory::Selected(prior),
        )
        .await
    }

    /// [`Self::response_base`] over a step's history. A stock turn's history
    /// carries its uncompacted base, so that step bounds only new messages.
    pub(super) async fn response_history_base(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        history: &mut request::ModelStepHistory<'_>,
    ) -> Result<(
        ContextInput,
        crate::context::AdmittedContext,
        Option<FileRef>,
    )> {
        let compaction = !matches!(
            self.compaction_policy()?,
            crate::context::CompactionPolicy::Disabled
        );
        let reference = if compaction && let Some(previous) = step.checked_sub(1) {
            let (_, records) = self
                .model_records(
                    journal,
                    input.operation_id,
                    previous,
                    ModelPurpose::Response,
                )
                .await?;
            match records.iter().rev().find_map(|record| match &record.event {
                ExecutionEvent::ContextPrepared { projection, .. }
                | ExecutionEvent::ContextCompacted { projection, .. } => Some(projection),
                _ => None,
            }) {
                Some(projection) => {
                    transcript::working_reference(journal, projection, self.io).await?
                }
                None => None,
            }
        } else {
            None
        };
        let Some(reference) = reference else {
            if compaction
                && let Some((covered, base)) = self.preceding_context(journal, input).await?
            {
                let context_input = ContextInput::copy_turn_suffix(
                    input,
                    step,
                    history.messages(),
                    covered,
                    self.io,
                )?;
                let delta =
                    ContextPipeline::base_context(&context_input, self.stock.limits, self.io)?;
                let base = ContextPipeline::continue_base(base, delta, self.stock.limits, self.io)?;
                return Ok((context_input, base, None));
            }
            if self.working_tail.lock().await.prior_offset != 0 {
                return Err(self.invalid_working_cut());
            }
            // A checkpoint base is owner-resolved and composed again each step.
            if let request::ModelStepHistory::Turn(history, _) = history
                && input
                    .selected_context
                    .as_ref()
                    .is_none_or(|selected| selected.selection.checkpoint.is_none())
            {
                let (context_input, base) =
                    history.base(input, step, self.stock.limits, self.io)?;
                return Ok((context_input, base, None));
            }
            let mut context_input =
                ContextInput::copy_for_turn(input, step, history.messages(), self.io)?;
            let base = self
                .canonical_base_for_input(journal, &mut context_input)
                .await?;
            return Ok((context_input, base, None));
        };
        let prior = history.messages();
        let mut cache = self.working_tail.lock().await;
        self.refresh_working_cut(&mut cache, journal, &reference)
            .await?;
        let retained = cache
            .retained
            .as_ref()
            .ok_or_else(|| self.invalid_working_cut())?;
        let total = self.prior_count(cache.prior_offset, prior.len())?;
        let consumed = self.validate_working_cut(retained, input, step, total)?;
        let consumed = consumed
            .checked_sub(cache.prior_offset)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| self.invalid_working_cut())?;
        let suffix = prior
            .get(consumed..)
            .ok_or_else(|| self.invalid_working_cut())?;
        let context_input = ContextInput::copy_for_turn(input, step, suffix, self.io)?;
        let suffix = crate::model::clone_messages(&context_input.prior_messages, self.io)?;
        let base = retained
            .context
            .copy(self.io)?
            .under(self.stock.limits, self.io)?
            .extend(stock_into_iter(suffix, self.io)?, self.io)?;
        Ok((context_input, base, Some(reference)))
    }

    fn matching_prefix(
        &self,
        input: &TurnInput,
        prefix: &WorkingContextPrefix,
    ) -> Result<Option<usize>> {
        let Some(selected) = input.selected_context.as_ref() else {
            return Ok(None);
        };
        if selected.selection.checkpoint.is_some() {
            return Ok(None);
        }
        let Some(covered) = usize::try_from(prefix.messages)
            .ok()
            .filter(|covered| *covered > 0 && *covered < selected.messages.len())
        else {
            return Ok(None);
        };
        let ids = selected
            .selection
            .message_ids
            .get(..covered)
            .ok_or_else(|| self.invalid_working_cut())?;
        Ok((preparation_digest(&ids, self.io)? == prefix.selection).then_some(covered))
    }

    async fn preceding_context(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<Option<(usize, crate::context::AdmittedContext)>> {
        let Some(prefix) = journal.preceding_working_context() else {
            return Ok(None);
        };
        let Some(covered) = self.matching_prefix(input, prefix)? else {
            return Ok(None);
        };
        if journal.working_checkpoint_authority().is_none() {
            return Err(self.invalid_working_cut());
        }
        let mut cache = self.working_tail.lock().await;
        self.refresh_working_cut(&mut cache, journal, &prefix.checkpoint)
            .await?;
        let retained = cache
            .retained
            .as_ref()
            .ok_or_else(|| self.invalid_working_cut())?;
        if retained.cut.operation != prefix.operation
            || retained.cut.step != 0
            || cache.prior_offset != 0
        {
            return Err(self.invalid_working_cut());
        }
        // The active input ends the original selection. Configuration and
        // steering after it re-enter in the next turn's original transcript.
        let through = retained
            .context
            .current_input_index
            .and_then(|index| (index as usize).checked_add(1))
            .ok_or_else(|| self.invalid_working_cut())?;
        Ok(Some((
            covered,
            retained.context.copy_range(0..through, self.io)?,
        )))
    }

    /// Carry only the original first-step cut. Later cuts can consume part of
    /// this turn's transcript, which the next turn selects independently.
    pub(super) async fn working_context_at_settlement(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<Option<WorkingContextPrefix>> {
        let Some(selected) = input.selected_context.as_ref() else {
            return Ok(None);
        };
        if journal.working_checkpoint_authority().is_none()
            || matches!(
                self.compaction_policy()?,
                crate::context::CompactionPolicy::Disabled
            )
        {
            return Ok(None);
        }
        let (_, records) = self
            .model_records(journal, input.operation_id, 0, ModelPurpose::Response)
            .await?;
        if let Some(projection) = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ContextCompacted { projection, .. } => Some(projection),
            _ => None,
        }) && let Some(checkpoint) =
            transcript::working_reference(journal, projection, self.io).await?
        {
            return Ok(Some(WorkingContextPrefix {
                checkpoint,
                operation: input.operation_id,
                messages: selected.selection.message_ids.len() as u64,
                selection: preparation_digest(&selected.selection.message_ids.as_slice(), self.io)?,
            }));
        }
        let Some(previous) = journal.preceding_working_context() else {
            return Ok(None);
        };
        if self.matching_prefix(input, previous)?.is_none() {
            return Ok(None);
        }
        Ok(Some(WorkingContextPrefix {
            checkpoint: crate::model::clone_file(&previous.checkpoint, self.io)?,
            operation: previous.operation,
            messages: previous.messages,
            selection: previous.selection,
        }))
    }

    async fn refresh_working_cut(
        &self,
        cache: &mut TailCache<'a>,
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
    ) -> Result<()> {
        // A running request can skip rebuilding its base, but a new compacted
        // cut still has to authenticate and retire the driver's old tail.
        if journal.cache_identity().is_none()
            || cache.retained.as_ref().is_none_or(|cached| {
                &cached.reference != reference || cached.journal != journal.cache_identity()
            })
        {
            cache.retained = Some(self.load_working_cut(journal, reference).await?);
        }
        Ok(())
    }

    pub(super) async fn synchronize_working_cut(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior: usize,
        reference: Option<&FileRef>,
    ) -> Result<()> {
        let mut cache = self.working_tail.lock().await;
        let Some(reference) = reference else {
            return if cache.prior_offset == 0 {
                Ok(())
            } else {
                Err(self.invalid_working_cut())
            };
        };
        self.refresh_working_cut(&mut cache, journal, reference)
            .await?;
        let retained = cache
            .retained
            .as_ref()
            .ok_or_else(|| self.invalid_working_cut())?;
        self.validate_working_cut(
            retained,
            input,
            step,
            self.prior_count(cache.prior_offset, prior)?,
        )?;
        Ok(())
    }

    pub(super) fn invalid_working_cut(&self) -> Error {
        stock_boundary(
            self.io,
            crate::wire::ErrorCode::Storage,
            "working checkpoint differs from its admitted turn",
        )
    }

    fn validate_working_cut(
        &self,
        retained: &RetainedCut<'_>,
        input: &TurnInput,
        step: u32,
        prior: u64,
    ) -> Result<u64> {
        let cut = &retained.cut;
        let consumed = cut.prior_messages;
        if cut.operation != input.operation_id
            || cut.input_digest != self.request_digest(input)?
            || cut.step >= step
            || consumed > prior
        {
            return Err(self.invalid_working_cut());
        }
        Ok(consumed)
    }

    pub(super) async fn load_working_cut(
        &self,
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
    ) -> Result<RetainedCut<'a>> {
        self.checked_working_cut(journal, reference, None).await
    }

    async fn checked_working_cut(
        &self,
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
        expected: Option<&crate::context::CompactionReference>,
    ) -> Result<RetainedCut<'a>> {
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        let io = scope.io();
        let cut = retain_preparation(load_json::<WorkingCut>(journal, reference, io).await?)?;
        if cut.version != 1 {
            return Err(self.invalid_working_cut());
        }
        let authenticated = match (journal.working_checkpoint_authority(), cut.proof) {
            (Some(verifier), Some((first, second))) => {
                let digest = cut.payload_digest(io)?;
                let mut signature = [0; 64];
                signature[..32].copy_from_slice(&first);
                signature[32..].copy_from_slice(&second);
                with_history_io(io, |work| {
                    verifier.verify_working_checkpoint(digest, &signature, work)
                })?;
                true
            }
            (None, None) => false,
            _ => return Err(self.invalid_working_cut()),
        };
        for file in [&cut.source, &cut.retained, &cut.compaction] {
            with_history_io(io, |work| self.stock.limits.validate_file(file, work))?;
            if file.volume() != reference.volume()
                || file.descriptor().media_type() != "application/json"
            {
                return Err(self.invalid_working_cut());
            }
        }
        let original_context = retain_preparation(
            load_json::<crate::context::Context>(journal, &cut.retained, io).await?,
        )?;
        // Both decoded values already own their admitted backing. Keep that
        // custody while validating, then transfer it instead of copying them.
        let decoded = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        // The one bounds walk of the retained context: every step extends it.
        let original_context =
            crate::context::AdmittedContext::admit(original_context, self.stock.limits, io)?;
        if !authenticated || expected.is_some() {
            let proof = retain_preparation(
                load_json::<crate::context::CompactionReference>(journal, &cut.compaction, io)
                    .await?,
            )?;
            if expected.is_some_and(|expected| expected != &proof) {
                return Err(self.invalid_working_cut());
            }
            if !authenticated {
                let source = retain_preparation(
                    load_json::<crate::context::Context>(journal, &cut.source, io).await?,
                )?;
                self.validate_admitted_compaction(
                    &source,
                    &original_context,
                    &proof,
                    (cut.operation, cut.step),
                    io,
                )?;
            }
        }
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let reference = crate::model::clone_file(reference, io)?;
        let hold = {
            let retained = io.map_or(Ok(0), |io| {
                io.owned_bytes()?
                    .checked_sub(before)
                    .and_then(|reference| decoded.checked_add(reference))
                    .ok_or_else(|| self.invalid_working_cut())
            })?;
            scope.finish(retained)?;
            self.io.map(|io| io.guard_owned(retained)).transpose()?
        };
        Ok(RetainedCut {
            journal: journal.cache_identity(),
            reference,
            cut,
            context: original_context,
            _hold: hold,
        })
    }

    pub(super) async fn stage_working_cut(
        &self,
        journal: &dyn ExecutionJournal,
        prepared: WorkingCompaction<'_>,
    ) -> Result<(FileRef, FileRef)> {
        let WorkingCompaction {
            input,
            step,
            prior,
            source,
            retained,
            proof,
        } = prepared;
        // Only this producer may attest semantic admission. Later signed reads
        // need the retained bytes, rather than the evicted source and proof.
        self.validate_working_compaction(
            source,
            retained,
            proof,
            (input.operation_id, step),
            self.io,
        )?;
        let candidate_key = |component| {
            super::compaction::compaction_candidate_key(step, proof, component, self.io)
        };
        let compaction_key = candidate_key("compaction")?;
        let compaction =
            stage_json(journal, input.operation_id, &compaction_key, proof, self.io).await?;
        let source_key = with_history_io(self.io, |work| {
            preparation_format(format_args!("context:{step}:working-source"), work)
        })?;
        let source = stage_json(journal, input.operation_id, &source_key, source, self.io).await?;
        let retained = stage_json(
            journal,
            input.operation_id,
            &candidate_key("working-retained")?,
            retained,
            self.io,
        )
        .await?;
        let mut cut = WorkingCut {
            version: 1,
            operation: input.operation_id,
            input_digest: self.request_digest(input)?,
            step,
            prior_messages: self.absolute_prior_count(prior).await?,
            source,
            retained,
            compaction: crate::model::clone_file(&compaction, self.io)?,
            proof: None,
        };
        if let Some(verifier) = journal.working_checkpoint_authority() {
            let digest = cut.payload_digest(self.io)?;
            let proof = with_history_io(self.io, |work| {
                Ok::<_, Error>(verifier.working_checkpoint_proof(digest, work))
            })?
            .await?;
            let mut first = [0; 32];
            let mut second = [0; 32];
            first.copy_from_slice(&proof[..32]);
            second.copy_from_slice(&proof[32..]);
            cut.proof = Some((first, second));
        }
        let checkpoint = stage_json(
            journal,
            input.operation_id,
            &candidate_key("working-checkpoint")?,
            &cut,
            self.io,
        )
        .await?;
        Ok((compaction, checkpoint))
    }

    fn validate_working_compaction(
        &self,
        source: &crate::context::Context,
        retained: &crate::context::Context,
        proof: &crate::context::CompactionReference,
        identity: (OperationId, u32),
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        crate::context::validate_projected_context(retained, self.stock.limits, io)?;
        self.validate_admitted_compaction(source, retained, proof, identity, io)
    }

    /// [`Self::validate_working_compaction`] of a retained context already
    /// admitted under the stock limits.
    fn validate_admitted_compaction(
        &self,
        source: &crate::context::Context,
        retained: &crate::context::Context,
        proof: &crate::context::CompactionReference,
        identity: (OperationId, u32),
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        crate::context::validate_projected_context(source, self.stock.limits, io)?;
        crate::context::validate_compaction(proof, source, retained, io)?;
        if proof
            .summary
            .as_ref()
            .is_none_or(|summary| summary.operation_id != identity.0 || summary.step > identity.1)
        {
            return Err(self.invalid_working_cut());
        }
        Ok(())
    }

    pub(super) async fn validate_compacted_working_cut(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        reference: &FileRef,
        proof: &crate::context::CompactionReference,
    ) -> Result<()> {
        let retained = self
            .checked_working_cut(journal, reference, Some(proof))
            .await?;
        if retained.cut.operation != input.operation_id
            || retained.cut.step != step
            || retained.cut.input_digest != self.request_digest(input)?
        {
            return Err(self.invalid_working_cut());
        }
        Ok(())
    }
}
