use super::*;

/// Rendering mode for the same immutable source state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextRenderMode {
    /// Rebuild the component in its configured prompt position.
    Prompt,
    /// Append an authoritative notification after cached context.
    Update,
}

/// Replaceable typed renderer; transformations compose as ordinary later stages.
pub trait ContextRenderer: acyclic_stream::ProviderPlatform {
    /// Immutable implementation identity included in execution admission.
    fn contract(&self, io: Option<&PreparationIo>) -> Result<Value>;
    /// Renders a host-approved selection; must honor supplied finite limits.
    fn render<'a>(
        &'a self,
        selection: &'a ContextSelection,
        mode: ContextRenderMode,
        input: &'a ContextInput,
        limits: Limits,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>>;
    /// Reports original-ledger support before rendering.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        false
    }
}

/// Selection/renderer adapter, reusing the existing stage engine and content verifier.
pub struct SelectionStage {
    name: String,
    selection: ContextSelection,
    renderer: Arc<dyn ContextRenderer>,
    verifier: Arc<dyn ContentResidencyVerifier>,
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
}

impl SelectionStage {
    /// Registers a pinned source and renderer. Construction starts no reads/effects.
    pub fn new(
        name: String,
        selection: ContextSelection,
        renderer: Arc<dyn ContextRenderer>,
        verifier: Arc<dyn ContentResidencyVerifier>,
        mode: ContextRenderMode,
        placement: ContextPlacement,
        limits: Limits,
    ) -> Result<Self> {
        crate::contract::validate_component_label(&name, "selection stage")?;
        selection.validate(limits, &mut None)?;
        if !renderer.contract(None)?.is_object() {
            return Err(Error::Invalid(
                "context renderer contract must be an object".into(),
            ));
        }
        Ok(Self {
            name,
            selection,
            renderer,
            verifier,
            mode,
            placement,
            limits,
        })
    }
}

impl ContextStage for SelectionStage {
    fn name(&self) -> &str {
        &self.name
    }

    fn contract(&self, io: Option<&PreparationIo>) -> Result<Value> {
        self.validate(io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let renderer_hold;
        let renderer = self.renderer.contract(io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            renderer_hold = match io.filter(|io| io.original_native_work().is_some()) {
                Some(io) => {
                    let retained = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        crate::executor::stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "renderer contract released outer custody",
                        )
                    })?;
                    Some(io.guard_owned(retained)?)
                }
                None => None,
            };
        }
        if !renderer.is_object() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "context renderer contract must be an object",
            ));
        }
        #[derive(Serialize)]
        struct Contract<'a> {
            name: &'a str,
            selection: &'a ContextSelection,
            renderer: &'a Value,
            mode: ContextRenderMode,
            placement: ContextPlacement,
            limits: Limits,
        }
        let contract = Contract {
            name: &self.name,
            selection: &self.selection,
            renderer: &renderer,
            mode: self.mode,
            placement: self.placement,
            limits: self.limits,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let io = io.expect("native ledger");
            crate::executor::with_history_io(Some(io), |work| observe_value(&renderer, work))?;
            let (value, custody) = context_contract_native(&contract, work)?;
            drop(renderer);
            drop(renderer_hold);
            io.absorb_native_custody(custody)?;
            return Ok(value);
        }
        serde_json::to_value(contract).map_err(|error| Error::Invalid(error.to_string()))
    }

    fn apply<'a>(
        &'a self,
        input: &'a ContextInput,
        context: Context,
        limits: Limits,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Context>> {
        self.validate(io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let limits = restrict_context_limits(self.limits, limits, io)?;
            validate_projected_context(&context, limits, io)?;
            crate::executor::with_history_io(io, |work| self.selection.validate(limits, work))?;
            if let ContextSourceValue::File { file } = &self.selection.source {
                self.verifier.verify(file, limits.file_bytes, io)?.await?;
            }
            let messages = self
                .renderer
                .render(&self.selection, self.mode, input, limits, io)?
                .await?;
            apply_context_projection(context, messages, self.mode, self.placement, limits, io)
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        self.renderer.supports_native()
    }
    fn validate(&self, io: Option<&PreparationIo>) -> Result<()> {
        if !crate::is_valid_component_label(&self.name) {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "selection stage identity is invalid",
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            work.remaining()?;
            if !self.renderer.supports_native() {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "context renderer has no native capability",
                ));
            }
            crate::executor::with_history_io(io, |work| {
                self.selection.validate(self.limits, work)
            })?;
        }
        Ok(())
    }
}
