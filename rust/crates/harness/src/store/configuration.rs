use super::*;

impl<P: StreamProvider> StreamAggregate<P> {
    /// Moves the reducer out without copying or replaying it.
    pub fn into_reducer(self) -> Reducer {
        self.reducer
    }

    /// Configures retirement of originally indexed successful/failed effects.
    pub fn with_resident_terminal_effect_limit(mut self, maximum: usize) -> Result<Self> {
        self.reducer.set_resident_terminal_effect_limit(maximum)?;
        Ok(self)
    }

    /// Sets the archive allowance for effect checks and retries.
    pub fn with_effect_history_read_limits(mut self, limits: HistoryReadLimits) -> Result<Self> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "effect history bounds must be positive".into(),
            ));
        }
        self.effect_history_limits = limits;
        Ok(self)
    }

    /// Reconfigures the bounded event/retry cache without moving a borrowed aggregate.
    pub fn set_resident_event_limit(&mut self, maximum: usize) -> Result<()> {
        self.reducer.set_resident_event_limit(maximum)
    }

    /// Retires only closed, checkpoint-covered conversation records from memory.
    pub fn compact_conversation_projection(&mut self) -> Result<()> {
        self.reducer.compact_conversation_projection(&mut None)
    }

    /// Installs the provider boundary that verifies every message file before admission.
    #[must_use]
    pub fn with_content_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.content_verifier = Some(verifier);
        self
    }

    /// Installs the provider boundary that checks a prepared child before publication.
    #[must_use]
    pub fn with_fork_verifier(mut self, verifier: Arc<CompositeForkVerifier>) -> Self {
        self.fork_verifier = Some(verifier);
        self
    }

    /// Installs the provider proof for parent-controlled project joins.
    #[must_use]
    pub fn with_merge_verifier(mut self, verifier: Arc<dyn ProjectMergeVerifier>) -> Self {
        self.merge_verifier = Some(verifier);
        self
    }

    /// Requires an installed, exact-digest migration implementation before accepting any `MigrateExtensionState` event.
    #[must_use]
    pub fn with_extension_migrations(mut self, migrations: Arc<NativeExtensionMigrations>) -> Self {
        self.extension_migrations = Some(migrations);
        self
    }

    /// Updates admission bounds without discarding the verified projection.
    pub fn set_limits(&mut self, limits: Limits) -> Result<()> {
        limits.validate(&mut None)?;
        self.limits = limits;
        Ok(())
    }

    /// Returns the current deterministic projection.
    #[must_use]
    pub const fn reducer(&self) -> &Reducer {
        &self.reducer
    }
}
