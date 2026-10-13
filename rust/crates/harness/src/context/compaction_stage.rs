use super::*;

/// Deterministic reusable compaction stage retaining a bounded message tail.
pub struct CompactionStage {
    /// Maximum messages retained after compaction.
    pub max_messages: usize,
    /// Optional caller-produced summary prepended when compaction occurs.
    pub summary: Option<ContextSummary>,
    /// Consumer-declared mandatory roles and native media retention.
    pub retention: CompactionRetention,
}

impl ContextStage for CompactionStage {
    fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        self.retention.validate(io)?;
        if self.max_messages == 0 {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "compaction max_messages must be positive",
            ));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "compaction"
    }

    fn contract(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io.is_some_and(|io| io.original_native_work().is_some()) {
            self.validate(io)?;
        }
        #[derive(Serialize)]
        struct Contract<'a> {
            name: &'static str,
            revision: &'static str,
            max_messages: usize,
            summary: &'a Option<ContextSummary>,
            retention: &'a CompactionRetention,
        }
        context_contract(
            &Contract {
                name: "compaction",
                revision: "3",
                max_messages: self.max_messages,
                summary: &self.summary,
                retention: &self.retention,
            },
            io,
        )
    }

    fn apply<'a>(
        &'a self,
        _: &'a ContextInput,
        context: Context,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        self.validate(io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            validate_projected_context(&context, limits, io)?;
            let summary = crate::executor::with_history_io(io, |work| {
                self.summary
                    .as_ref()
                    .map(|summary| {
                        Ok::<_, crate::Error>(ContextSummary {
                            operation_id: summary.operation_id,
                            step: summary.step,
                            source_messages: summary.source_messages,
                            source_digest: summary.source_digest,
                            output: summary.output.copy(work)?,
                        })
                    })
                    .transpose()
            })?;
            let compacted = DurableContextProvider::compact(
                &context,
                self.max_messages,
                summary,
                &self.retention,
                io,
            )?
            .0;
            validate_projected_context(&compacted, limits, io)?;
            Ok(compacted)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        true
    }
}
