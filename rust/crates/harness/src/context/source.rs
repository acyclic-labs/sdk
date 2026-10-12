use super::*;
use crate::{executor::stock_boundary, wire::ErrorCode};

/// Replaceable memory/retrieval/skill source used by reusable stock stages.
pub trait ContextSource: acyclic_stream::ProviderPlatform {
    /// Resolves messages under the admitted limits and optional original ledger.
    fn load<'a>(
        &'a self,
        input: &'a ContextInput,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>>;
    /// Reports original-ledger support before a stage invokes the source.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        false
    }
}

/// Placement of source messages relative to existing context.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextPlacement {
    /// Insert before current messages.
    Prepend,
    /// Insert after current messages.
    Append,
}

/// Reusable stage for memory, retrieval, or skills.
pub struct SourceStage {
    name: String,
    revision: String,
    source: Arc<dyn ContextSource>,
    placement: ContextPlacement,
}

impl SourceStage {
    /// Creates a named source stage; `memory`, `retrieval`, and `skills` are ordinary names.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        revision: impl Into<String>,
        source: Arc<dyn ContextSource>,
        placement: ContextPlacement,
    ) -> Self {
        Self {
            name: name.into(),
            revision: revision.into(),
            source,
            placement,
        }
    }

    /// Copies native identities before construction; the caller retains the source provider's custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        name: &str,
        revision: &str,
        source: Arc<dyn ContextSource>,
        placement: ContextPlacement,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        work.remaining()?;
        if !crate::is_valid_component_label(name) || !crate::is_valid_component_label(revision) {
            return Err(crate::Error::NativeBoundary {
                code: ErrorCode::Invalid,
                reason: "context source identity is invalid",
            });
        }
        if !source.supports_native() {
            return Err(crate::Error::NativeBoundary {
                code: ErrorCode::Unsupported,
                reason: "context source has no native capability",
            });
        }
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
        let name = crate::core::copy_text(name, &mut quota)?;
        let revision = crate::core::copy_text(revision, &mut quota)?;
        quota.transfer();
        drop(quota);
        drop(observed);
        Ok((
            Self::new(name, revision, source, placement),
            io.into_custody()?,
        ))
    }
}

impl ContextStage for SourceStage {
    fn validate(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        if !crate::is_valid_component_label(&self.name)
            || !crate::is_valid_component_label(&self.revision)
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "context source identity is invalid",
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io
            .and_then(crate::executor::PreparationIo::original_native_work)
            .is_some()
            && !self.source.supports_native()
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Unsupported,
                "context source has no native capability",
            ));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn contract(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        self.validate(io)?;
        #[derive(Serialize)]
        struct Contract<'a> {
            name: &'a str,
            revision: &'a str,
            placement: ContextPlacement,
        }
        let contract = Contract {
            name: &self.name,
            revision: &self.revision,
            placement: self.placement,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (value, custody) = context_contract_native(&contract, work)?;
            io.expect("native ledger").absorb_native_custody(custody)?;
            return Ok(value);
        }
        serde_json::to_value(contract).map_err(|error| crate::Error::Invalid(error.to_string()))
    }

    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io
            .and_then(crate::executor::PreparationIo::original_native_work)
            .is_some()
        {
            self.validate(io)?;
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            validate_projected_context(&context, limits, io)?;
            let loaded = self.source.load(input, limits, io)?.await?;
            selection::apply_context_projection(
                context,
                loaded,
                selection::ContextRenderMode::Prompt,
                self.placement,
                limits,
                io,
            )
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        self.source.supports_native()
    }
}
