use super::*;

/// Immutable code-defined runtime composition.
#[derive(Clone)]
pub struct HarnessBundle {
    /// Human-readable constructor identity.
    pub(super) name: String,
    /// Complete replaceable executor.
    pub(super) executor: Option<Arc<dyn Executor>>,
    /// Capabilities requested by this composition.
    pub(super) capabilities: Capabilities,
    /// Central content, projection, and loop bounds for this composition.
    pub(super) limits: Limits,
    /// Typed task runtime sharing this bundle's immutable tool contracts.
    pub(super) runtime: Arc<AgentHarness>,
    pub(super) journal: Option<Arc<dyn ExecutionJournal>>,
    pub(super) agent_loop: bool,
    pub(super) stock_task: bool,
}

impl HarnessBundle {
    /// Creates a new immutable view pinned to a later parent fork snapshot.
    pub fn at_parent_snapshot(&self, preparer: Arc<dyn crate::fork::ForkPreparer>) -> Result<Self> {
        let mut next = self.clone();
        next.runtime = self.runtime.at_parent_snapshot(preparer)?;
        Ok(next)
    }

    /// Returns the constructor identity pinned at composition.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the immutable effective capability set.
    #[must_use]
    pub const fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Returns the immutable executor bound to this composition.
    #[must_use]
    pub fn executor(&self) -> Option<Arc<dyn Executor>> {
        self.executor.clone()
    }

    /// Runs or resumes an admitted typed turn through the pinned journal.
    pub async fn run(&self, input: TurnInput) -> Result<TurnOutput> {
        if input.max_steps == 0
            || usize::try_from(input.max_steps).unwrap_or(usize::MAX) > self.limits.model_steps
        {
            return Err(Error::Invalid(
                "turn exceeds the bound model step limit".into(),
            ));
        }
        if self.stock_task {
            let definition = self
                .runtime
                .task::<TurnInput, TurnOutput>("acyclic.stock_turn@2")?;
            let admitted = self
                .runtime
                .spawn_local_with_operation(input.operation_id, &definition, input)
                .await?;
            let crate::runtime::RuntimeTask::Live(handle) = admitted else {
                return Err(Error::Unsupported(
                    "stock turn requires ordinary local task admission".into(),
                ));
            };
            return match handle.result().await {
                crate::Outcome::Succeeded(result) => result,
                crate::Outcome::Failed { message } => Err(Error::Conflict(message)),
                crate::Outcome::Cancelled => Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled,
                )),
                crate::Outcome::Indeterminate { operation_id } => {
                    Err(Error::Indeterminate(operation_id))
                }
            };
        }
        let executor = self
            .executor
            .as_ref()
            .ok_or_else(|| Error::Unsupported("turn executor is not bound".into()))?;
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| Error::Unsupported("turn journal is not bound".into()))?;
        executor.execute(input, journal.as_ref()).await
    }

    /// Runs the typed custom agent loop as a local task, preserving its full terminal outcome and task identity.
    pub async fn run_agent(&self, input: AgentInput) -> Result<AgentRunOutput> {
        if !self.agent_loop {
            return Err(Error::Unsupported("custom agent loop is not bound".into()));
        }
        if input.prompt.is_empty() || input.prompt.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid(
                "agent prompt exceeds limits or is empty".into(),
            ));
        }
        if input.attachments.len() > self.limits.attachments {
            return Err(Error::Invalid("agent attachments exceed limit".into()));
        }
        for attachment in &input.attachments {
            attachment.validate(&mut None)?;
            self.limits.validate_file(&attachment.file, &mut None)?;
            self.runtime.verify_readable_file(&attachment.file).await?;
        }
        let definition = self
            .runtime
            .task::<AgentInput, crate::agent_loop::AgentOutput>("acyclic.agent_loop@2")?;
        let task = self.runtime.spawn(&definition, input).await?;
        let task_id = task.identity();
        let outcome = task.result().await?;
        if let crate::Outcome::Succeeded(output) = &outcome {
            if output.text.len() as u64 > self.limits.file_bytes
                || output.attachments.len() > self.limits.attachments
                || crate::contract::canonical_json_bytes(&output.metadata)?.len() as u64
                    > self.limits.file_bytes
            {
                return Err(Error::Invalid("agent output exceeds limits".into()));
            }
            for attachment in &output.attachments {
                attachment.validate(&mut None)?;
                self.limits.validate_file(&attachment.file, &mut None)?;
                self.runtime.verify_readable_file(&attachment.file).await?;
            }
        }
        Ok(AgentRunOutput { task_id, outcome })
    }

    /// Returns the checked limits bound to this composition.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }

    /// Returns the pinned typed task runtime.
    #[must_use]
    pub fn runtime(&self) -> Arc<AgentHarness> {
        self.runtime.clone()
    }
}
