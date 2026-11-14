//! Case files use the UI reader's original cut, indexed reads and live query.
use super::*;
use crate::agent::HistoryCheckpoint;

pub(super) struct Prepared {
    stream: Retained<StreamPath>,
    record: NativeStreamOutput<acyclic_stream::Record>,
    entry: Retained<ConversationEntry>,
    before: Retained<HistoryCheckpoint>,
    after: Retained<HistoryCheckpoint>,
}

impl crate::evals::Inputs for StreamUiCut {
    fn stream(&self) -> Result<&StreamPath, DemandError> {
        self.evaluation
            .as_ref()
            .map(|case| &*case.stream)
            .ok_or(DemandError::SourceUnavailable)
    }
    fn tail(&self) -> u64 {
        self.tail.unwrap_or(0)
    }
    fn entry(
        &self,
        sequence: u64,
    ) -> Result<(&acyclic_stream::Record, &ConversationEntry), DemandError> {
        let case = self
            .evaluation
            .as_ref()
            .filter(|case| case.record.value().sequence == sequence)
            .ok_or(DemandError::SourceUnavailable)?;
        Ok((case.record.value(), &case.entry))
    }
    fn checkpoint(&self, sequence: u64) -> Result<&HistoryCheckpoint, DemandError> {
        let case = self
            .evaluation
            .as_ref()
            .ok_or(DemandError::SourceUnavailable)?;
        if case.record.value().sequence == sequence {
            Ok(&case.before)
        } else if case.record.value().sequence.checked_add(1) == Some(sequence) {
            Ok(&case.after)
        } else {
            Err(DemandError::SourceUnavailable)
        }
    }
}

impl<P: ?Sized, F: ?Sized, L> StreamUiProvider<P, F, L> {
    /// Select original conversation/history inputs with the same reader.
    /// Construction performs no IO and grants no additional source authority.
    pub(crate) fn evaluation_inputs(mut self) -> Self {
        if !self.evaluations {
            let mut hash = blake3::Hasher::new();
            hash.update(b"acyclic:ui:evaluation-inputs:v1");
            hash.update(&self.identity);
            self.identity
                .copy_from_slice(&hash.finalize().as_bytes()[..16]);
            self.evaluations = true;
        }
        self
    }

    /// Render case files through the same Computed adapter as UI files.
    pub fn into_evaluation_source(self) -> Computed<crate::evals::View<StreamUiCut>, Self>
    where
        Self: ViewProvider<Snapshot = StreamUiCut, Record = Affected>,
    {
        Computed::new(
            Arc::new(crate::evals::View::default()),
            self.evaluation_inputs(),
        )
    }
}

impl<P, F, L> StreamUiProvider<P, F, L>
where
    P: NativeStreamProvider + ?Sized,
    F: ContentResidencyVerifier + ?Sized,
    L: Fn(&StreamPath, &mut KernelAccounting<'_>) -> Result<OwnedSourceChunk, DemandError>
        + Send
        + Sync,
{
    pub(super) async fn prepare_evaluation(
        &self,
        source: &SourceReference,
        query: ViewQuery<'_>,
        original: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<StreamUiCut, DemandError> {
        let at = self.revision(source)?;
        // Authenticate the selected cut even for a directory-only demand.
        // Its original tail is sufficient: listing reads no turn bodies.
        let heads = self
            .heads(Inputs::Evaluations, Some(at.clone()), original, ctx)
            .await?;
        let mut cut = StreamUiCut::empty(self.identity, &self.agent, ctx)?;
        let tail = heads
            .value()
            .heads
            .first()
            .and_then(Option::as_ref)
            .map_or(0, |head| head.tail);
        cut.tail = Some(tail);
        drop(heads);
        let path = match query {
            ViewQuery::Lookup(path)
            | ViewQuery::Open(path)
            | ViewQuery::Range { path, .. }
            | ViewQuery::Directory { path, .. } => path,
        };
        let selected = match crate::evals::sequence(path, ctx) {
            Ok(selected) => selected,
            Err(DemandError::Absent) => return Ok(cut),
            Err(error) => return Err(error),
        };
        if let Some(sequence) = selected.filter(|sequence| *sequence < tail) {
            let stream = native_path(
                &self.agent.stream_parts(AgentStream::Conversation),
                original,
            )?;
            let allocation =
                RetainedAllocation::claim(std::mem::size_of::<StreamPath>() as u64, 1, ctx)?;
            let record = self
                .read_record(AgentStream::Conversation, sequence, &at, original, ctx)
                .await?;
            let entry = ConversationEntry::decode_accounted(&record.value().value, ctx)
                .map_err(record_error)?;
            let before = self.checkpoint(sequence, &at, original, ctx).await?;
            let after = self
                .checkpoint(
                    sequence.checked_add(1).ok_or(DemandError::StaleSource)?,
                    &at,
                    original,
                    ctx,
                )
                .await?;
            cut.evaluation = Some(Prepared {
                stream: allocation.into_value(stream),
                record,
                entry,
                before,
                after,
            });
        }
        Ok(cut)
    }

    async fn checkpoint(
        &self,
        sequence: u64,
        at: &Revision,
        original: &NativeMaterializationWork,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Retained<HistoryCheckpoint>, DemandError> {
        let record = self
            .read_record(AgentStream::History, sequence, at, original, ctx)
            .await?;
        let checkpoint = HistoryCheckpoint::decode_accounted(&record.value().value, ctx)
            .map_err(record_error)?;
        if checkpoint.conversation != sequence {
            return Err(DemandError::StaleSource);
        }
        Ok(checkpoint)
    }
}
