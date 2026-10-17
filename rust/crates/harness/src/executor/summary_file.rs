use super::*;

impl StockExecution<'_> {
    pub(super) async fn summarize_file(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        source: &FileRef,
    ) -> Result<ExecutionProgress<TurnOutput>> {
        let scope = self
            .original_context()
            .map_or(&self.stock.tool_scope, |context| context.scope());
        with_history_io(self.io, |work| {
            self.stock.limits.validate_file(source, work)?;
            scope.limits().validate_file(source, work)?;
            if source.volume().class() != VolumeClass::AgentPrivate
                || source.descriptor().media_type() != "application/json"
            {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "summary source is not private context JSON",
                ));
            }
            if !crate::runtime::read_granted_with_work(scope.grants(), source, work)? {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Unauthorized,
                    "summary source is outside the admitted read grant",
                ));
            }
            Ok(())
        })?;
        let bytes = journal.load(source, self.io)?.await?;
        #[cfg(feature = "filesystem")]
        let _bytes_hold = self.io.map(|io| io.guard_bytes(&bytes)).transpose()?;
        let source_hold: Option<crate::conversation::ContentHold<'_>>;
        let captured: crate::context::SummarySource = match self.io {
            #[cfg(feature = "filesystem")]
            Some(io) => {
                let (source, hold) = io.decode(
                    std::marker::PhantomData::<crate::context::SummarySource>,
                    &bytes,
                    crate::wire::ErrorCode::Invalid,
                )?;
                source_hold = Some(hold);
                source
            }
            _ => {
                source_hold = None;
                crate::contract::json_from_slice(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
        };
        drop(bytes);
        #[cfg(feature = "filesystem")]
        drop(_bytes_hold);
        let (instruction, _owned) = with_history_io(self.io, |work| {
            crate::model::clone_content(&input.input, work)
        })?;
        #[cfg(feature = "filesystem")]
        let _instruction_hold = self.io.map(|io| io.guard_owned(_owned)).transpose()?;
        let boundary = captured.boundary;
        let (summary, text) = self
            .summarize(
                journal,
                Some(input),
                input.operation_id,
                0,
                captured.context,
                instruction,
            )
            .await?;
        let checkpoint = crate::context::SummaryCheckpoint {
            operation: input.operation_id,
            boundary,
            source: crate::model::clone_file(source, self.io)?,
            summary: summary.output,
        };
        let metadata = match self.io {
            #[cfg(feature = "filesystem")]
            Some(io) => {
                let bytes = io.encode(&checkpoint, self.stock.limits.file_bytes, true)?;
                let _hold = io.guard_bytes(&bytes)?;
                let decoded = io.decode(
                    std::marker::PhantomData::<Value>,
                    &bytes,
                    crate::wire::ErrorCode::Invalid,
                )?;
                crate::filesystem::retain_decoded(decoded)?
            }
            _ => serde_json::to_value(&checkpoint)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        drop(checkpoint);
        drop(source_hold);
        Ok(ExecutionProgress::Ready(TurnOutput {
            text,
            attachments: Vec::new(),
            metadata,
            steps: 1,
        }))
    }
}
