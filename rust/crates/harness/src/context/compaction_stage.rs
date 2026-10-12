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
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            self.validate(io)?;
            #[derive(Serialize)]
            struct Contract<'a> {
                name: &'static str,
                revision: &'static str,
                max_messages: usize,
                summary: &'a Option<ContextSummary>,
                retention: &'a CompactionRetention,
            }
            let (value, custody) = context_contract_native(
                &Contract {
                    name: "compaction",
                    revision: "3",
                    max_messages: self.max_messages,
                    summary: &self.summary,
                    retention: &self.retention,
                },
                work,
            )?;
            io.expect("native ledger").absorb_native_custody(custody)?;
            return Ok(value);
        }
        let _ = io;
        Ok(
            serde_json::json!({"name": self.name(), "revision": "3", "max_messages": self.max_messages,
            "summary": self.summary, "retention": self.retention}),
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
                        Ok(ContextSummary {
                            operation_id: summary.operation_id,
                            step: summary.step,
                            source_messages: summary.source_messages,
                            source_digest: summary.source_digest,
                            output: summary.output.copy(work)?,
                        })
                    })
                    .transpose()
            })?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let retention = if let Some(work) =
                io.and_then(crate::executor::PreparationIo::original_native_work)
            {
                let (retention, custody) = clone_retention_native(&self.retention, work)?;
                io.expect("native ledger").absorb_native_custody(custody)?;
                retention
            } else {
                self.retention.clone()
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let retention = self.retention.clone();
            let compacted = DurableContextProvider::compact(
                &context,
                self.max_messages,
                summary,
                retention,
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
