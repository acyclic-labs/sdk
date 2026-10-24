//! A private pre-stage working checkpoint, carried by every later projection.
use super::*;

mod history;
mod retirement;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkingCut {
    operation: OperationId,
    input_digest: [u8; 32],
    step: u32,
    prior_messages: u64,
    source: FileRef,
    retained: FileRef,
    compaction: FileRef,
    proof: ([u8; 32], [u8; 32]),
}

impl WorkingCut {
    fn payload_digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        preparation_digest(
            &(
                "working-context-cut/v1",
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

#[cfg(feature = "filesystem")]
impl PreparationRecord for WorkingCut {
    fn container_collection_slots() -> bool {
        true
    }
    fn maximum_slot_bytes() -> u64 {
        crate::filesystem::preparation_file_record_slot_bytes()
    }
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
    context: crate::context::Context,
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
    pub(super) compaction: &'a FileRef,
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
    ) -> Result<(ContextInput, crate::context::Context, Option<FileRef>)> {
        let reference = if !matches!(
            self.compaction_policy()?,
            crate::context::CompactionPolicy::Disabled
        ) && let Some(previous) = step.checked_sub(1)
        {
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
            if self.working_tail.lock().await.prior_offset != 0 {
                return Err(self.invalid_working_cut());
            }
            let mut context_input = ContextInput::copy_for_turn(input, step, prior, self.io)?;
            let base = self
                .canonical_base_for_turn(journal, input.operation_id, &mut context_input)
                .await?;
            return Ok((context_input, base, None));
        };
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
        let base = with_history_io(self.io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let (messages, claim) = crate::context::clone_messages(
                &retained.context.messages,
                None,
                &context_input.prior_messages,
                quota.work(),
            )?;
            quota.adopt(claim)?;
            let (metadata, claim) =
                crate::context::clone_metadata(&retained.context.metadata, quota.work())?;
            quota.adopt(claim)?;
            quota.transfer();
            Ok::<_, Error>(crate::context::Context {
                messages,
                metadata,
                current_input_index: retained.context.current_input_index,
            })
        })?;
        crate::context::validate_projected_context(&base, self.stock.limits, self.io)?;
        Ok((context_input, base, Some(reference)))
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
        #[cfg(feature = "filesystem")]
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        #[cfg(feature = "filesystem")]
        let io = scope.io();
        #[cfg(not(feature = "filesystem"))]
        let io = self.io;
        let cut = retain_preparation(load_json::<WorkingCut>(journal, reference, io).await?)?;
        let verifier = journal
            .working_checkpoint_authority()
            .ok_or_else(|| self.invalid_working_cut())?;
        let digest = cut.payload_digest(io)?;
        let mut signature = [0; 64];
        signature[..32].copy_from_slice(&cut.proof.0);
        signature[32..].copy_from_slice(&cut.proof.1);
        with_history_io(io, |work| {
            verifier.verify_working_checkpoint(digest, &signature, work)
        })?;
        for file in [&cut.source, &cut.retained, &cut.compaction] {
            with_history_io(io, |work| self.stock.limits.validate_file(file, work))?;
            if file.volume() != reference.volume()
                || file.descriptor().media_type() != "application/json"
            {
                return Err(self.invalid_working_cut());
            }
        }
        let source = retain_preparation(
            load_json::<crate::context::Context>(journal, &cut.source, io).await?,
        )?;
        let original_context = retain_preparation(
            load_json::<crate::context::Context>(journal, &cut.retained, io).await?,
        )?;
        let proof = retain_preparation(
            load_json::<crate::context::CompactionReference>(journal, &cut.compaction, io).await?,
        )?;
        crate::context::validate_projected_context(&source, self.stock.limits, io)?;
        crate::context::validate_projected_context(&original_context, self.stock.limits, io)?;
        crate::context::validate_compaction(&proof, &source, &original_context, io)?;
        if proof
            .summary
            .as_ref()
            .is_none_or(|summary| summary.operation_id != cut.operation || summary.step != cut.step)
        {
            return Err(self.invalid_working_cut());
        }
        #[cfg(feature = "filesystem")]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let reference = crate::model::clone_file(reference, io)?;
        let retained_cut = WorkingCut {
            operation: cut.operation,
            input_digest: cut.input_digest,
            step: cut.step,
            prior_messages: cut.prior_messages,
            source: crate::model::clone_file(&cut.source, io)?,
            retained: crate::model::clone_file(&cut.retained, io)?,
            compaction: crate::model::clone_file(&cut.compaction, io)?,
            proof: cut.proof,
        };
        let context = crate::context::copy_context(&original_context, io)?;
        drop(source);
        drop(original_context);
        drop(proof);
        drop(cut);
        #[cfg(feature = "filesystem")]
        let hold = {
            let retained = io.map_or(Ok(0), |io| {
                io.owned_bytes()?
                    .checked_sub(before)
                    .ok_or_else(|| self.invalid_working_cut())
            })?;
            scope.finish(retained)?;
            self.io.map(|io| io.guard_owned(retained)).transpose()?
        };
        #[cfg(not(feature = "filesystem"))]
        let hold = None;
        Ok(RetainedCut {
            journal: journal.cache_identity(),
            reference,
            cut: retained_cut,
            context,
            _hold: hold,
        })
    }

    pub(super) async fn stage_working_cut(
        &self,
        journal: &dyn ExecutionJournal,
        prepared: WorkingCompaction<'_>,
    ) -> Result<FileRef> {
        let WorkingCompaction {
            input,
            step,
            prior,
            source,
            retained,
            compaction,
        } = prepared;
        let key = |suffix| {
            with_history_io(self.io, |work| {
                preparation_format(format_args!("context:{step}:working-{suffix}"), work)
            })
        };
        let source = stage_json(
            journal,
            input.operation_id,
            &key("source")?,
            source,
            self.io,
        )
        .await?;
        let retained = stage_json(
            journal,
            input.operation_id,
            &key("retained")?,
            retained,
            self.io,
        )
        .await?;
        let mut cut = WorkingCut {
            operation: input.operation_id,
            input_digest: self.request_digest(input)?,
            step,
            prior_messages: self.absolute_prior_count(prior).await?,
            source,
            retained,
            compaction: crate::model::clone_file(compaction, self.io)?,
            proof: ([0; 32], [0; 32]),
        };
        let verifier = journal
            .working_checkpoint_authority()
            .ok_or_else(|| self.invalid_working_cut())?;
        let digest = cut.payload_digest(self.io)?;
        let proof = with_history_io(self.io, |work| {
            verifier.working_checkpoint_proof(digest, work)
        })?;
        cut.proof.0.copy_from_slice(&proof[..32]);
        cut.proof.1.copy_from_slice(&proof[32..]);
        stage_json(
            journal,
            input.operation_id,
            &key("checkpoint")?,
            &cut,
            self.io,
        )
        .await
    }

    pub(super) async fn validate_compacted_working_cut(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        reference: &FileRef,
        proof: &crate::context::CompactionReference,
    ) -> Result<()> {
        let retained = self.load_working_cut(journal, reference).await?;
        if retained.cut.operation != input.operation_id
            || retained.cut.step != step
            || retained.cut.input_digest != self.request_digest(input)?
        {
            return Err(self.invalid_working_cut());
        }
        let stored = load_json::<crate::context::CompactionReference>(
            journal,
            &retained.cut.compaction,
            self.io,
        )
        .await?;
        if &stored.0 != proof {
            return Err(self.invalid_working_cut());
        }
        Ok(())
    }
}
