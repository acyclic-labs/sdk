//! Session-wide swarm admission and resource accounting.
//!
//! The projection in this module is deliberately independent of a storage
//! provider. A host persists the event returned by an admission operation
//! before dispatching a fork, then applies the same event while projecting its
//! durable coordinator log. This keeps admission-before-dispatch and replay
//! semantics in one place while allowing Stream, memory, and local providers
//! to share the contract.

use crate::{
    Error, IdempotencyKey, OperationId, Result, contract::canonical_json_bytes,
    runtime::TaskAdmissionRecord,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Maximum number of agents one budget projection may retain.
pub const MAX_SWARM_AGENTS: u64 = 1_000_000;
/// Maximum recursion depth accepted by one projection.
pub const MAX_SWARM_DEPTH: u32 = 1_024;
/// Maximum model steps admitted by one session.
pub const MAX_SWARM_MODEL_STEPS: u64 = 1_000_000_000;
/// Maximum output bytes admitted by one session.
pub const MAX_SWARM_OUTPUT_BYTES: u64 = 1_u64 << 50;
/// Maximum execution time in milliseconds admitted by one session.
pub const MAX_SWARM_EXECUTION_TIME_MS: u64 = 31_536_000_000;

/// Session-wide limits shared by the root and every descendant.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmBudgetLimits {
    /// Maximum simultaneously reserved or running agents, including the root.
    pub max_active_agents: u64,
    /// Maximum agents admitted over the lifetime of the session, including root.
    pub max_total_agents: u64,
    /// Maximum child depth below the root. The root is depth zero.
    pub max_recursion_depth: u32,
    /// Maximum model steps consumed or reserved by the complete session.
    pub max_model_steps: u64,
    /// Maximum model output bytes consumed or reserved by the complete session.
    pub max_output_bytes: u64,
    /// Maximum execution time in milliseconds consumed or reserved by the session.
    pub max_execution_time_ms: u64,
}

impl SwarmBudgetLimits {
    /// Validates bounds before a session can be opened.
    pub fn validate(self) -> Result<()> {
        if self.max_active_agents == 0
            || self.max_active_agents > MAX_SWARM_AGENTS
            || self.max_total_agents == 0
            || self.max_total_agents > MAX_SWARM_AGENTS
            || self.max_active_agents > self.max_total_agents
            || self.max_recursion_depth > MAX_SWARM_DEPTH
            || self.max_model_steps == 0
            || self.max_model_steps > MAX_SWARM_MODEL_STEPS
            || self.max_output_bytes == 0
            || self.max_output_bytes > MAX_SWARM_OUTPUT_BYTES
            || self.max_execution_time_ms == 0
            || self.max_execution_time_ms > MAX_SWARM_EXECUTION_TIME_MS
        {
            return Err(Error::Invalid("swarm budget limits are invalid".into()));
        }
        Ok(())
    }
}

impl Default for SwarmBudgetLimits {
    fn default() -> Self {
        Self {
            max_active_agents: 8,
            max_total_agents: 64,
            max_recursion_depth: 8,
            max_model_steps: 512,
            max_output_bytes: 64 * 1024 * 1024,
            max_execution_time_ms: 60 * 60 * 1_000,
        }
    }
}

/// Monotonic owner identity used to fence stale workers after recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmOwnerFence {
    /// Durable host or process owner identity.
    pub owner: String,
    /// Generation advanced on every takeover.
    pub generation: u64,
}

impl SwarmOwnerFence {
    /// Creates and validates one owner fence.
    pub fn new(owner: impl Into<String>, generation: u64) -> Result<Self> {
        let value = Self {
            owner: owner.into(),
            generation,
        };
        value.validate()?;
        Ok(value)
    }

    /// Rejects empty, control-bearing, or exhausted owner identities.
    pub fn validate(&self) -> Result<()> {
        if self.owner.is_empty()
            || self.owner.len() > 255
            || self.owner.chars().any(char::is_control)
        {
            return Err(Error::Invalid("swarm owner identity is invalid".into()));
        }
        Ok(())
    }
}

/// Budget requested for one child model invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmResourceRequest {
    /// Maximum model steps this child may consume.
    pub model_steps: u64,
    /// Maximum model output bytes this child may emit.
    pub output_bytes: u64,
    /// Maximum execution time in milliseconds for this child.
    pub execution_time_ms: u64,
}

impl SwarmResourceRequest {
    /// Validates nonzero per-child requests.
    pub fn validate(self) -> Result<()> {
        if self.model_steps == 0 || self.output_bytes == 0 || self.execution_time_ms == 0 {
            return Err(Error::Invalid(
                "swarm child resource request is empty".into(),
            ));
        }
        Ok(())
    }
}

/// Measured usage reported by one child. Values are cumulative per report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmUsage {
    /// Model steps consumed so far.
    pub model_steps: u64,
    /// Output bytes emitted so far.
    pub output_bytes: u64,
    /// Execution time in milliseconds consumed so far.
    pub execution_time_ms: u64,
}

impl SwarmUsage {
    fn checked_delta(self, previous: Self) -> Result<Self> {
        Ok(Self {
            model_steps: self
                .model_steps
                .checked_sub(previous.model_steps)
                .ok_or_else(|| Error::Conflict("child usage moved backwards".into()))?,
            output_bytes: self
                .output_bytes
                .checked_sub(previous.output_bytes)
                .ok_or_else(|| Error::Conflict("child usage moved backwards".into()))?,
            execution_time_ms: self
                .execution_time_ms
                .checked_sub(previous.execution_time_ms)
                .ok_or_else(|| Error::Conflict("child usage moved backwards".into()))?,
        })
    }
}

/// Durable cursor for a provider's cumulative usage receipt stream.
///
/// A cursor is empty before the first receipt. Once a receipt has been
/// accepted, both fields must be present so a restarted issuer cannot reset
/// its sequence while retaining (or losing) only part of its measurement.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmUsageReceiptCursor {
    /// Last durable receipt sequence, or zero before the first receipt.
    pub sequence: u64,
    /// Last cumulative provider measurement, when `sequence` is nonzero.
    pub usage: Option<SwarmUsage>,
}

impl SwarmUsageReceiptCursor {
    /// Creates and validates a restart cursor.
    pub fn new(sequence: u64, usage: Option<SwarmUsage>) -> Result<Self> {
        validate_issuer_cursor(sequence, usage)?;
        Ok(Self { sequence, usage })
    }
}

/// Provider-side guard that stops work before it crosses a child ceiling.
///
/// A runtime should call the step guard before starting a model step, the
/// output guard before accepting emitted bytes, and the elapsed-time guard at
/// each provider scheduling boundary.  Receipt issuance remains a second
/// check against the provider's measured cumulative counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SwarmUsageLimiter {
    limits: SwarmResourceRequest,
    usage: SwarmUsage,
}

impl SwarmUsageLimiter {
    /// Creates a provider guard from the exact admitted child ceiling.
    pub fn new(limits: SwarmResourceRequest) -> Result<Self> {
        Self::resume(limits, SwarmUsage::default())
    }

    /// Restores a provider guard from the last durable cumulative measurement.
    ///
    /// Restarted runtimes must carry forward accepted usage before dispatching
    /// another provider slice. Starting a fresh guard would reopen capacity
    /// already consumed by the same dispatch attempt.
    pub fn resume(limits: SwarmResourceRequest, usage: SwarmUsage) -> Result<Self> {
        limits.validate()?;
        if usage.model_steps > limits.model_steps
            || usage.output_bytes > limits.output_bytes
            || usage.execution_time_ms > limits.execution_time_ms
        {
            return Err(Error::Conflict(
                "restored provider usage exceeds the child reservation ceiling".into(),
            ));
        }
        Ok(Self {
            limits,
            usage,
        })
    }

    /// Returns cumulative usage admitted by the provider guard.
    #[must_use]
    pub const fn usage(self) -> SwarmUsage {
        self.usage
    }

    /// Returns the exact ceiling enforced by this guard.
    #[must_use]
    pub const fn limits(self) -> SwarmResourceRequest {
        self.limits
    }

    /// Returns the execution budget still available to this authenticated
    /// dispatch.
    #[must_use]
    pub const fn remaining_execution_time_ms(self) -> u64 {
        self.limits
            .execution_time_ms
            .saturating_sub(self.usage.execution_time_ms)
    }

    /// Reserves one model step before invoking the model.
    pub fn admit_model_step(&mut self) -> Result<SwarmUsage> {
        let next = self
            .usage
            .model_steps
            .checked_add(1)
            .ok_or_else(|| Error::Conflict("provider model step ceiling exhausted".into()))?;
        if next > self.limits.model_steps {
            return Err(Error::Conflict(
                "provider model step ceiling exhausted".into(),
            ));
        }
        self.usage.model_steps = next;
        Ok(self.usage)
    }

    /// Reserves output bytes before accepting them from the provider.
    pub fn admit_output(&mut self, bytes: u64) -> Result<SwarmUsage> {
        let next = self
            .usage
            .output_bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::Conflict("provider output ceiling exhausted".into()))?;
        if next > self.limits.output_bytes {
            return Err(Error::Conflict("provider output ceiling exhausted".into()));
        }
        self.usage.output_bytes = next;
        Ok(self.usage)
    }

    /// Advances measured elapsed time before allowing another provider slice.
    pub fn admit_execution_time(&mut self, elapsed_ms: u64) -> Result<SwarmUsage> {
        let next = self
            .usage
            .execution_time_ms
            .checked_add(elapsed_ms)
            .ok_or_else(|| Error::Conflict("provider execution time ceiling exhausted".into()))?;
        if next > self.limits.execution_time_ms {
            return Err(Error::Conflict(
                "provider execution time ceiling exhausted".into(),
            ));
        }
        self.usage.execution_time_ms = next;
        Ok(self.usage)
    }
}

/// Provider-issued cumulative usage evidence bound to one dispatch attempt.
///
/// A coordinator persists this receipt alongside the usage projection. Hosts
/// may retry the exact receipt, but cannot release a reservation with an
/// unbound caller-supplied usage value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmUsageReceipt {
    /// Operation whose provider measured the usage.
    pub operation_id: OperationId,
    /// Stable child dispatch or root lease identity bound by the host.
    pub dispatch_id: IdempotencyKey,
    /// Strictly increasing cumulative report sequence.
    pub sequence: u64,
    /// Cumulative measured usage at this sequence.
    pub usage: SwarmUsage,
    /// Provider or host identity that issued the receipt.
    pub provider: String,
}

impl SwarmUsageReceipt {
    /// Constructs provider evidence after a host/provider measurement.
    pub fn new(
        operation_id: OperationId,
        dispatch_id: IdempotencyKey,
        sequence: u64,
        usage: SwarmUsage,
        provider: impl Into<String>,
    ) -> Result<Self> {
        let receipt = Self {
            operation_id,
            dispatch_id,
            sequence,
            usage,
            provider: provider.into(),
        };
        receipt.validate()?;
        Ok(receipt)
    }

    /// Validates the provider evidence shape before binding it to a lease.
    pub fn validate(&self) -> Result<()> {
        if self.operation_id.into_bytes() == [0; 16]
            || self.sequence == 0
            || self.provider.is_empty()
            || self.provider.len() > 255
            || self.provider.chars().any(char::is_control)
        {
            return Err(Error::Invalid("swarm usage receipt is invalid".into()));
        }
        IdempotencyKey::new(self.dispatch_id.0.clone())?;
        Ok(())
    }
}

/// Provider usage evidence that crossed the host's measurement boundary.
///
/// The raw receipt remains serializable for durable replay, while coordinator
/// mutation APIs accept this crate-visible proof wrapper so callers cannot
/// manufacture a budget release by passing an arbitrary usage value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedSwarmUsageReceipt(SwarmUsageReceipt);

impl VerifiedSwarmUsageReceipt {
    /// Binds a provider receipt after the host verifies its measurement.
    fn from_verified(receipt: SwarmUsageReceipt) -> Result<Self> {
        receipt.validate()?;
        Ok(Self(receipt))
    }

    /// Returns the durable receipt for the compound scheduler event.
    #[must_use]
    pub(crate) fn into_receipt(self) -> SwarmUsageReceipt {
        self.0
    }
}

/// Host/provider boundary for reading cumulative work from one dispatch.
///
/// Implementations are owned by the execution route and must read the
/// provider's measured counters rather than values supplied by a task or
/// caller. The coordinator only accepts receipts issued through this source.
pub trait SwarmUsageSource: Send + Sync {
    /// Stable provider identity retained in each receipt.
    fn provider_identity(&self) -> &str;

    /// Reads cumulative usage for the exact operation and dispatch attempt.
    fn cumulative_usage(
        &self,
        operation_id: OperationId,
        dispatch_id: &IdempotencyKey,
    ) -> Result<SwarmUsage>;

}

impl<T: SwarmUsageSource + ?Sized> SwarmUsageSource for Arc<T> {
    fn provider_identity(&self) -> &str {
        (**self).provider_identity()
    }

    fn cumulative_usage(
        &self,
        operation_id: OperationId,
        dispatch_id: &IdempotencyKey,
    ) -> Result<SwarmUsage> {
        (**self).cumulative_usage(operation_id, dispatch_id)
    }

}

/// Monotonic receipt issuer bound to one provider dispatch.
pub struct SwarmUsageReceiptIssuer<S> {
    source: S,
    operation_id: OperationId,
    dispatch_id: IdempotencyKey,
    provider: String,
    limits: Option<SwarmResourceRequest>,
    sequence: u64,
    last_usage: Option<SwarmUsage>,
}

impl<S: SwarmUsageSource> SwarmUsageReceiptIssuer<S> {
    /// Binds a host measurement source to one operation and dispatch.
    pub fn new(source: S, operation_id: OperationId, dispatch_id: IdempotencyKey) -> Result<Self> {
        if operation_id.into_bytes() == [0; 16] {
            return Err(Error::Invalid("swarm usage operation is empty".into()));
        }
        IdempotencyKey::new(dispatch_id.0.clone())?;
        let provider = source.provider_identity().to_owned();
        if provider.is_empty() || provider.len() > 255 || provider.chars().any(char::is_control) {
            return Err(Error::Invalid(
                "swarm usage provider identity is invalid".into(),
            ));
        }
        Ok(Self {
            source,
            operation_id,
            dispatch_id,
            provider: provider.to_owned(),
            limits: None,
            sequence: 0,
            last_usage: None,
        })
    }

    /// Binds the issuer to the exact per-child reservation ceiling.
    ///
    /// Runtime/provider adapters should use this constructor so an over-limit
    /// provider counter cannot be turned into a completion receipt.  The
    /// durable budget remains the final authority when the receipt is applied.
    pub fn with_limits(
        source: S,
        operation_id: OperationId,
        dispatch_id: IdempotencyKey,
        limits: SwarmResourceRequest,
    ) -> Result<Self> {
        limits.validate()?;
        let mut issuer = Self::new(source, operation_id, dispatch_id)?;
        issuer.limits = Some(limits);
        Ok(issuer)
    }

    /// Reopens an issuer from the durable receipt cursor and last measured
    /// cumulative usage retained by the host.
    pub fn resume(
        source: S,
        operation_id: OperationId,
        dispatch_id: IdempotencyKey,
        sequence: u64,
        last_usage: Option<SwarmUsage>,
    ) -> Result<Self> {
        validate_issuer_cursor(sequence, last_usage)?;
        let mut issuer = Self::new(source, operation_id, dispatch_id)?;
        issuer.sequence = sequence;
        issuer.last_usage = last_usage;
        Ok(issuer)
    }

    /// Reopens an issuer from a durable cursor.
    pub fn resume_with_cursor(
        source: S,
        operation_id: OperationId,
        dispatch_id: IdempotencyKey,
        cursor: SwarmUsageReceiptCursor,
    ) -> Result<Self> {
        Self::resume(
            source,
            operation_id,
            dispatch_id,
            cursor.sequence,
            cursor.usage,
        )
    }

    /// Reopens an issuer with both its durable cursor and child ceiling.
    pub fn resume_with_limits(
        source: S,
        operation_id: OperationId,
        dispatch_id: IdempotencyKey,
        sequence: u64,
        last_usage: Option<SwarmUsage>,
        limits: SwarmResourceRequest,
    ) -> Result<Self> {
        validate_issuer_cursor(sequence, last_usage)?;
        let mut issuer = Self::with_limits(source, operation_id, dispatch_id, limits)?;
        if let Some(usage) = last_usage {
            if usage.model_steps > limits.model_steps
                || usage.output_bytes > limits.output_bytes
                || usage.execution_time_ms > limits.execution_time_ms
            {
                return Err(Error::Conflict(
                    "restored provider usage exceeds the child reservation ceiling".into(),
                ));
            }
        }
        issuer.sequence = sequence;
        issuer.last_usage = last_usage;
        Ok(issuer)
    }

    /// Reopens an issuer from a durable cursor and exact child ceiling.
    pub fn resume_with_cursor_and_limits(
        source: S,
        operation_id: OperationId,
        dispatch_id: IdempotencyKey,
        cursor: SwarmUsageReceiptCursor,
        limits: SwarmResourceRequest,
    ) -> Result<Self> {
        Self::resume_with_limits(
            source,
            operation_id,
            dispatch_id,
            cursor.sequence,
            cursor.usage,
            limits,
        )
    }

    /// Reads provider counters and issues the next verified durable receipt.
    pub fn issue(&mut self) -> Result<VerifiedSwarmUsageReceipt> {
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("swarm usage receipt sequence exhausted".into()))?;
        let mut usage = self
            .source
            .cumulative_usage(self.operation_id, &self.dispatch_id)?;
        if let Some(limits) = self.limits {
            if usage.model_steps > limits.model_steps
                || usage.output_bytes > limits.output_bytes
                || usage.execution_time_ms > limits.execution_time_ms
            {
                return Err(Error::Conflict(
                    "provider usage exceeds the child reservation ceiling".into(),
                ));
            }
        }
        if let Some(previous) = self.last_usage {
            usage.checked_delta(previous)?;
        }
        let receipt = SwarmUsageReceipt::new(
            self.operation_id,
            self.dispatch_id.clone(),
            sequence,
            usage,
            self.provider.clone(),
        )?;
        self.sequence = sequence;
        self.last_usage = Some(usage);
        VerifiedSwarmUsageReceipt::from_verified(receipt)
    }

    /// Returns the exact provider dispatch identity bound to this issuer.
    #[must_use]
    pub fn dispatch_id(&self) -> &IdempotencyKey {
        &self.dispatch_id
    }

    /// Returns the operation identity bound to this issuer.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Returns transport provenance authenticated by this provider issuer.
    #[must_use]
    pub fn provider_dispatch_context(
        &self,
        step: u32,
        request_digest: [u8; 32],
    ) -> crate::model::ProviderDispatchContext {
        crate::model::ProviderDispatchContext {
            operation_id: self.operation_id,
            step,
            request_digest,
            dispatch_id: self.dispatch_id.clone(),
        }
    }    /// Returns the next sequence expected from this issuer.
    #[must_use]
    pub fn next_sequence(&self) -> Result<u64> {
        self.sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("swarm usage receipt sequence exhausted".into()))
    }

    /// Returns the last durable issuer cursor.
    #[must_use]
    pub fn cursor(&self) -> SwarmUsageReceiptCursor {
        SwarmUsageReceiptCursor {
            sequence: self.sequence,
            usage: self.last_usage,
        }
    }

    /// Returns the last cumulative provider measurement, when one was issued.
    #[must_use]
    pub const fn last_usage(&self) -> Option<SwarmUsage> {
        self.last_usage
    }
}

/// The complete authorization and measurement boundary for one provider
/// dispatch.
///
/// A runtime should accept this context at its model entry point instead of
/// constructing a limiter or receipt issuer from caller-supplied values. The
/// token carries the admitted child ceiling and dispatch identity; the limiter
/// is created before the provider is invoked; and the issuer reads cumulative
/// usage from the bound measurement source.
pub struct SwarmDispatchContext<S> {
    token: SwarmDispatchToken,
    limiter: SwarmUsageLimiter,
    issuer: SwarmUsageReceiptIssuer<S>,
}

impl<S: SwarmUsageSource> SwarmDispatchContext<S> {
    /// Binds a fresh provider dispatch to its admission token.
    pub fn new(token: SwarmDispatchToken, source: S) -> Result<Self> {
        let limiter = token.usage_limiter()?;
        let issuer = token.usage_receipt_issuer(source)?;
        Ok(Self {
            token,
            limiter,
            issuer,
        })
    }

    /// Restores a provider dispatch from the last durable cumulative receipt.
    ///
    /// The cursor must come from the journal for this operation. Restoring it
    /// before provider work resumes prevents a process restart from reopening
    /// capacity already consumed by the same dispatch.
    pub fn resume(
        token: SwarmDispatchToken,
        source: S,
        cursor: SwarmUsageReceiptCursor,
    ) -> Result<Self> {
        let limiter = token.usage_limiter_from(cursor.usage.unwrap_or_default())?;
        let issuer = token.resume_usage_receipt_issuer(source, cursor)?;
        Ok(Self {
            token,
            limiter,
            issuer,
        })
    }

    /// Returns the dispatch authorization consumed by this context.
    #[must_use]
    pub const fn token(&self) -> &SwarmDispatchToken {
        &self.token
    }

    /// Returns transport provenance for the provider adapter.
    #[must_use]
    pub fn provider_dispatch_context(
        &self,
        step: u32,
        request_digest: [u8; 32],
    ) -> Result<crate::model::ProviderDispatchContext> {
        Ok(self
            .issuer
            .provider_dispatch_context(step, request_digest))
    }    /// Returns the mutable pre-work provider limiter.
    ///
    /// The provider adapter must call its admission methods before each model
    /// step, output write, and elapsed-time slice.
    pub fn limiter_mut(&mut self) -> &mut SwarmUsageLimiter {
        &mut self.limiter
    }

    /// Admits one model step at the provider boundary.
    pub fn admit_model_step(&mut self) -> Result<SwarmUsage> {
        self.limiter.admit_model_step()
    }

    /// Admits provider output measured at the provider boundary.
    pub fn admit_output(&mut self, bytes: u64) -> Result<SwarmUsage> {
        self.limiter.admit_output(bytes)
    }

    /// Admits measured provider execution time at the provider boundary.
    pub fn admit_execution_time(&mut self, elapsed_ms: u64) -> Result<SwarmUsage> {
        self.limiter.admit_execution_time(elapsed_ms)
    }

    /// Returns the mutable provider receipt issuer.
    pub fn issuer_mut(&mut self) -> &mut SwarmUsageReceiptIssuer<S> {
        &mut self.issuer
    }

    /// Reads provider counters and creates the next verified usage receipt.
    pub fn issue_usage_receipt(&mut self) -> Result<VerifiedSwarmUsageReceipt> {
        self.issuer.issue()
    }

    /// Returns the exact operation and dispatch identities for journal
    /// settlement.
    #[must_use]
    pub fn dispatch_identity(&self) -> (OperationId, &IdempotencyKey) {
        (self.issuer.operation_id(), self.issuer.dispatch_id())
    }

    /// Returns the latest provider measurement accepted by this context.
    #[must_use]
    pub const fn usage(&self) -> SwarmUsage {
        self.limiter.usage()
    }

    /// Returns the remaining provider execution ceiling for this dispatch.
    ///
    /// The value is derived from the journal-issued limiter rather than from
    /// model output or a caller supplied timeout.  A provider adapter can use
    /// it to put one deadline around its in-flight stream, including the
    /// interval in which the provider emits no event.
    #[must_use]
    pub fn remaining_execution_time_ms(&self) -> u64 {
        self.limiter
            .limits()
            .execution_time_ms
            .saturating_sub(self.limiter.usage().execution_time_ms)
    }

    /// Returns the issuer cursor, which becomes durable only after the caller
    /// commits the corresponding verified receipt to the journal.
    #[must_use]
    pub fn receipt_cursor(&self) -> SwarmUsageReceiptCursor {
        self.issuer.cursor()
    }
}

/// The complete authorization and measurement boundary for root provider
/// work. Root work has no child publication token, so its context is bound to
/// the canonical root scheduler lease and the session's current remaining
/// ceiling instead.
pub struct SwarmRootDispatchContext<S> {
    limiter: SwarmUsageLimiter,
    issuer: SwarmUsageReceiptIssuer<S>,
}

impl<S: SwarmUsageSource> SwarmRootDispatchContext<S> {
    fn new(limiter: SwarmUsageLimiter, issuer: SwarmUsageReceiptIssuer<S>) -> Self {
        Self {
            limiter,
            issuer,
        }
    }

    /// Returns transport provenance for the root provider adapter.
    #[must_use]
    pub fn provider_dispatch_context(
        &self,
        step: u32,
        request_digest: [u8; 32],
    ) -> crate::model::ProviderDispatchContext {
        self.issuer.provider_dispatch_context(step, request_digest)
    }    /// Returns the mutable pre-work provider limiter.
    pub fn limiter_mut(&mut self) -> &mut SwarmUsageLimiter {
        &mut self.limiter
    }

    /// Admits one root model step at the provider boundary.
    pub fn admit_model_step(&mut self) -> Result<SwarmUsage> {
        self.limiter.admit_model_step()
    }

    /// Admits root provider output measured at the provider boundary.
    pub fn admit_output(&mut self, bytes: u64) -> Result<SwarmUsage> {
        self.limiter.admit_output(bytes)
    }

    /// Admits measured root provider execution time at the provider boundary.
    pub fn admit_execution_time(&mut self, elapsed_ms: u64) -> Result<SwarmUsage> {
        self.limiter.admit_execution_time(elapsed_ms)
    }

    /// Returns the mutable provider receipt issuer.
    pub fn issuer_mut(&mut self) -> &mut SwarmUsageReceiptIssuer<S> {
        &mut self.issuer
    }

    /// Reads provider counters and creates the next verified root receipt.
    pub fn issue_usage_receipt(&mut self) -> Result<VerifiedSwarmUsageReceipt> {
        self.issuer.issue()
    }

    /// Returns the exact root operation and dispatch identities for journal
    /// settlement.
    #[must_use]
    pub fn dispatch_identity(&self) -> (OperationId, &IdempotencyKey) {
        (self.issuer.operation_id(), self.issuer.dispatch_id())
    }

    /// Returns the latest provider measurement accepted by this context.
    #[must_use]
    pub const fn usage(&self) -> SwarmUsage {
        self.limiter.usage()
    }

    /// Returns the remaining root provider execution ceiling.
    #[must_use]
    pub fn remaining_execution_time_ms(&self) -> u64 {
        self.limiter
            .limits()
            .execution_time_ms
            .saturating_sub(self.limiter.usage().execution_time_ms)
    }

    /// Returns the issuer cursor, which becomes durable only after the caller
    /// commits the corresponding verified receipt to the journal.
    #[must_use]
    pub fn receipt_cursor(&self) -> SwarmUsageReceiptCursor {
        self.issuer.cursor()
    }
}

fn validate_issuer_cursor(sequence: u64, last_usage: Option<SwarmUsage>) -> Result<()> {
    if (sequence == 0) != last_usage.is_none() {
        return Err(Error::Invalid(
            "swarm usage issuer cursor and cumulative usage must advance together".into(),
        ));
    }
    Ok(())
}

/// Parent and child identity submitted before a model fork is dispatched.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmForkRequest {
    /// Stable operation identity assigned before admission.
    pub operation_id: OperationId,
    /// Caller-retained retry identity.
    pub idempotency_key: IdempotencyKey,
    /// Direct parent operation, or `None` for a root child at depth one.
    pub parent_operation_id: Option<OperationId>,
    /// Child depth below the root.
    pub depth: u32,
    /// Requested child resources.
    pub resources: SwarmResourceRequest,
    /// Canonical task admission digest, including its prerequisite set.
    /// `None` is retained for low-level projection tests; production callers
    /// should use `DistributedCoordinator::admit_swarm_after`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_digest: Option<[u8; 32]>,
}

impl SwarmForkRequest {
    /// Validates identity and request bounds independent of session state.
    pub fn validate(&self) -> Result<()> {
        if self.operation_id.into_bytes() == [0; 16]
            || self
                .parent_operation_id
                .is_some_and(|id| id.into_bytes() == [0; 16])
            || self.depth == 0
        {
            return Err(Error::Invalid(
                "swarm fork identity or depth is invalid".into(),
            ));
        }
        IdempotencyKey::new(self.idempotency_key.0.clone())?;
        if self.admission_digest == Some([0; 32]) {
            return Err(Error::Invalid(
                "swarm task admission digest is empty".into(),
            ));
        }
        self.resources.validate()
    }

    /// Builds a fork request from the canonical durable task admission.
    ///
    /// The task admission remains the source of operation identity and
    /// prerequisite dependencies; this request only adds the session-wide
    /// resource reservation and parent operation binding.
    pub fn from_task_admission(
        admission: &TaskAdmissionRecord,
        idempotency_key: IdempotencyKey,
        parent_operation_id: Option<OperationId>,
        depth: u32,
        resources: SwarmResourceRequest,
    ) -> Result<Self> {
        let request = Self {
            operation_id: admission.operation_id,
            idempotency_key,
            parent_operation_id,
            depth,
            resources,
            admission_digest: Some(crate::contract::canonical_json_digest(
                &admission.canonical_value(),
            )?),
        };
        request.validate()?;
        request.resources.validate_against_admission(admission)?;
        Ok(request)
    }
}

impl SwarmResourceRequest {
    /// Binds child resource ceilings to the canonical task admission bounds.
    /// Execution time has no relative field in `TaskRunLimits`, so it is
    /// pinned to the same protocol ceiling used by the durable swarm budget.
    pub fn validate_against_admission(&self, admission: &TaskAdmissionRecord) -> Result<()> {
        admission.validate()?;
        let max_steps = admission
            .run_limits
            .max_steps
            .map_or(admission.limits.model_steps, |value| {
                value.min(admission.limits.model_steps)
            });
        let max_steps = u64::try_from(max_steps)
            .map_err(|_| Error::Invalid("task model step limit is not representable".into()))?;
        let max_output = admission
            .limits
            .file_bytes
            .min(admission.limits.render_bytes);
        if self.model_steps > max_steps
            || self.output_bytes > max_output
            || self.execution_time_ms > MAX_SWARM_EXECUTION_TIME_MS
        {
            return Err(Error::Conflict(
                "swarm child resources exceed canonical task admission limits".into(),
            ));
        }
        Ok(())
    }
}

/// Evidence that a complete model exchange and workspace publication finished.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkPublication {
    /// Child operation whose fork was published.
    pub operation_id: OperationId,
    /// Direct parent operation used for admission.
    pub parent_operation_id: Option<OperationId>,
    /// Digest of the completed authoritative model boundary.
    pub completed_boundary_digest: [u8; 32],
    /// Digest of the published child workspace generation.
    pub workspace_generation_digest: [u8; 32],
}

impl ForkPublication {
    /// Validates publication evidence before a child can obtain a dispatch token.
    pub fn validate(&self) -> Result<()> {
        if self.operation_id.into_bytes() == [0; 16]
            || self.completed_boundary_digest == [0; 32]
            || self.workspace_generation_digest == [0; 32]
            || self
                .parent_operation_id
                .is_some_and(|id| id.into_bytes() == [0; 16])
        {
            return Err(Error::Invalid(
                "fork publication evidence is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Publication evidence that has crossed the model/workspace verification
/// boundary owned by the Harness composition layer.
///
/// The constructor is crate-visible so a local runtime must obtain this value
/// from the verified model-fork helper before it can authorize dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedForkPublication(ForkPublication);

impl VerifiedForkPublication {
    /// Binds publication evidence after the authoritative helper verifies it.
    pub(crate) fn from_verified(publication: ForkPublication) -> Result<Self> {
        publication.validate()?;
        Ok(Self(publication))
    }

    /// Returns the verified evidence for durable activation.
    #[must_use]
    pub(crate) fn into_publication(self) -> ForkPublication {
        self.0
    }
}

/// Lifecycle of an admitted child reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmReservationState {
    /// Admission is persisted but fork publication is incomplete.
    Reserved,
    /// Fork publication completed and dispatch may proceed.
    Active,
    /// Child completed and no longer consumes active capacity.
    Completed,
    /// Child was cancelled and no longer consumes active capacity.
    Cancelled,
}

/// Durable reservation held for one child operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmForkReservation {
    /// Stable child operation identity.
    pub operation_id: OperationId,
    /// Direct parent operation identity.
    pub parent_operation_id: Option<OperationId>,
    /// Child depth below the root.
    pub depth: u32,
    /// Owner fence at the last mutation.
    pub owner: SwarmOwnerFence,
    /// Held child resource ceiling.
    pub resources: SwarmResourceRequest,
    /// Caller-retained retry identity bound to this reservation.
    pub idempotency_key: IdempotencyKey,
    /// Cumulative measured child usage.
    pub usage: SwarmUsage,
    /// Last durable provider receipt sequence accepted for this child.
    #[serde(default)]
    pub usage_sequence: u64,
    /// Provider dispatch identity recorded when publication is activated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_id: Option<IdempotencyKey>,
    /// Current reservation lifecycle.
    pub state: SwarmReservationState,
    /// Publication evidence, present before activation.
    pub publication: Option<ForkPublication>,
    /// Digest of the exact request and retry identity.
    pub request_digest: [u8; 32],
    /// Canonical task admission digest retained through recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_digest: Option<[u8; 32]>,
}

impl SwarmForkReservation {
    /// Returns the durable provider receipt cursor for this child.
    pub fn usage_cursor(&self) -> Result<SwarmUsageReceiptCursor> {
        SwarmUsageReceiptCursor::new(
            self.usage_sequence,
            (self.usage_sequence != 0).then_some(self.usage),
        )
    }
}

/// Session usage projection, including resources held by admitted children.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmBudgetUsage {
    /// Currently reserved or active agents, including the root.
    pub active_agents: u64,
    /// Lifetime admitted agents, including the root.
    pub total_agents: u64,
    /// Consumption already observed and never refunded.
    pub consumed: SwarmUsage,
    /// Unconsumed ceilings held by live reservations.
    pub reserved: SwarmUsage,
}

/// Durable changes to a budget projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SwarmBudgetEvent {
    /// Starts a session with one root agent at depth zero.
    Started {
        /// Stable root/session operation identity.
        session_id: OperationId,
        /// Owner fence retained by the session.
        owner: SwarmOwnerFence,
        /// Immutable session budget.
        limits: SwarmBudgetLimits,
        /// Canonical provider lease for root usage receipts, when the host
        /// opened the session with an active root scheduler reservation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        root_dispatch_id: Option<IdempotencyKey>,
    },
    /// Persists an admission before a fork is dispatched.
    ChildReserved {
        /// Exact child reservation.
        reservation: SwarmForkReservation,
    },
    /// Publishes complete fork evidence and enables child model dispatch.
    ChildActivated {
        /// Child operation identity.
        operation_id: OperationId,
        /// Exact owner fence used for activation.
        owner: SwarmOwnerFence,
        /// Complete model/workspace publication evidence.
        publication: ForkPublication,
        /// Provider dispatch identity, when activation crossed a provider
        /// dispatch boundary.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dispatch_id: Option<IdempotencyKey>,
    },
    /// Retains host-measured time for one exact generic Harness effect until
    /// the next authenticated provider receipt settles it.  The event is
    /// keyed by operation, dispatch, and effect identity so a retry cannot
    /// charge the same native tool or fork publication twice.
    HarnessEffectMeasured {
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        dispatch_id: IdempotencyKey,
        effect_id: IdempotencyKey,
        elapsed_ms: u64,
    },
    /// Adds cumulative measured usage to a live child.
    UsageReported {
        /// Child operation identity.
        operation_id: OperationId,
        /// Exact owner fence used for reporting.
        owner: SwarmOwnerFence,
        /// Cumulative measured child usage.
        usage: SwarmUsage,
        /// Provider evidence retained with the durable usage transition.
        receipt: SwarmUsageReceipt,
    },
    /// Adds cumulative measured usage from the root operation.
    RootUsageReported {
        /// Exact owner fence used for reporting.
        owner: SwarmOwnerFence,
        /// Cumulative measured root usage.
        usage: SwarmUsage,
        /// Provider evidence retained with the durable usage transition.
        receipt: SwarmUsageReceipt,
    },
    /// Marks a child complete and releases only its unconsumed reservation.
    ChildCompleted {
        /// Child operation identity.
        operation_id: OperationId,
        /// Exact owner fence used for completion.
        owner: SwarmOwnerFence,
        /// Final cumulative child usage.
        usage: SwarmUsage,
        /// Provider evidence retained with the durable usage transition.
        receipt: SwarmUsageReceipt,
    },
    /// Cancels a child and releases active/unconsumed resources without refunding usage.
    ChildCancelled {
        /// Child operation identity.
        operation_id: OperationId,
        /// Exact owner fence used for cancellation.
        owner: SwarmOwnerFence,
    },
    /// Takes ownership after restart and fences all prior mutations.
    OwnerTakenOver {
        /// New owner fence; generation must advance by one.
        owner: SwarmOwnerFence,
    },
}

/// Result of one atomic child admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwarmAdmissionReceipt {
    /// Exact reservation to retain for later publication and dispatch.
    pub reservation: SwarmForkReservation,
    /// Whether the request was an exact idempotent replay.
    pub replayed: bool,
}

impl SwarmAdmissionReceipt {
    /// Returns the durable event that must be persisted before fork dispatch.
    #[must_use]
    pub fn durable_event(&self) -> SwarmBudgetEvent {
        SwarmBudgetEvent::ChildReserved {
            reservation: self.reservation.clone(),
        }
    }
}

/// Token authorizing model dispatch after publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwarmDispatchToken {
    /// Child operation identity.
    operation_id: OperationId,
    /// Direct parent operation identity.
    parent_operation_id: Option<OperationId>,
    /// Current owner fence.
    owner: SwarmOwnerFence,
    /// Exact model boundary published for this child.
    completed_boundary_digest: [u8; 32],
    /// Exact workspace generation published for this child.
    workspace_generation_digest: [u8; 32],
    /// Provider dispatch identity bound to this activation, when available.
    dispatch_id: Option<IdempotencyKey>,
    /// Exact per-child provider ceiling carried into the dispatcher.
    resources: SwarmResourceRequest,
}

impl SwarmDispatchToken {
    /// Returns the admitted child operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Returns the direct parent operation identity.
    #[must_use]
    pub const fn parent_operation_id(&self) -> Option<OperationId> {
        self.parent_operation_id
    }

    /// Returns the provider dispatch identity bound to this activation.
    #[must_use]
    pub const fn dispatch_id(&self) -> Option<&IdempotencyKey> {
        self.dispatch_id.as_ref()
    }

    /// Returns the provider dispatch identity required for measured receipts.
    pub fn required_dispatch_id(&self) -> Result<&IdempotencyKey> {
        self.dispatch_id.as_ref().ok_or_else(|| {
            Error::Unauthorized("provider dispatch identity is not bound to the token".into())
        })
    }

    /// Returns the exact resource ceiling the provider must enforce before
    /// doing additional model work.
    #[must_use]
    pub const fn resources(&self) -> SwarmResourceRequest {
        self.resources
    }

    /// Creates the provider-side guard from the exact admitted ceiling.  A
    /// dispatcher should hold this guard for the whole model invocation and
    /// call it before each step, output write, and elapsed-time slice.
    pub fn usage_limiter(&self) -> Result<SwarmUsageLimiter> {
        SwarmUsageLimiter::new(self.resources)
    }

    /// Restores a provider guard from usage already accepted for this token.
    pub fn usage_limiter_from(&self, usage: SwarmUsage) -> Result<SwarmUsageLimiter> {
        SwarmUsageLimiter::resume(self.resources, usage)
    }

    /// Binds a provider measurement source to this dispatch authorization.
    ///
    /// The source receives the operation and durable dispatch identity from
    /// the token; callers cannot substitute a second lease while issuing
    /// receipts.
    pub fn usage_receipt_issuer<S: SwarmUsageSource>(
        &self,
        source: S,
    ) -> Result<SwarmUsageReceiptIssuer<S>> {
        let dispatch_id = self.required_dispatch_id()?.clone();
        SwarmUsageReceiptIssuer::with_limits(source, self.operation_id, dispatch_id, self.resources)
    }

    /// Reopens a provider measurement issuer from its durable cursor.
    pub fn resume_usage_receipt_issuer<S: SwarmUsageSource>(
        &self,
        source: S,
        cursor: SwarmUsageReceiptCursor,
    ) -> Result<SwarmUsageReceiptIssuer<S>> {
        let dispatch_id = self.required_dispatch_id()?.clone();
        SwarmUsageReceiptIssuer::resume_with_cursor_and_limits(
            source,
            self.operation_id,
            dispatch_id,
            cursor,
            self.resources,
        )
    }

    /// Creates the complete provider dispatch boundary before model work.
    pub fn usage_context<S: SwarmUsageSource>(&self, source: S) -> Result<SwarmDispatchContext<S>> {
        SwarmDispatchContext::new(self.clone(), source)
    }

    /// Restores the complete provider dispatch boundary from a durable cursor.
    pub fn resume_usage_context<S: SwarmUsageSource>(
        &self,
        source: S,
        cursor: SwarmUsageReceiptCursor,
    ) -> Result<SwarmDispatchContext<S>> {
        SwarmDispatchContext::resume(self.clone(), source, cursor)
    }

    /// Returns the owner fence bound to this dispatch authorization.
    #[must_use]
    pub const fn owner(&self) -> &SwarmOwnerFence {
        &self.owner
    }

    /// Returns the publication evidence bound to this dispatch authorization.
    #[must_use]
    pub fn publication(&self) -> ForkPublication {
        ForkPublication {
            operation_id: self.operation_id,
            parent_operation_id: self.parent_operation_id,
            completed_boundary_digest: self.completed_boundary_digest,
            workspace_generation_digest: self.workspace_generation_digest,
        }
    }
}

#[derive(Clone, Debug)]
struct SwarmBudgetState {
    session_id: OperationId,
    limits: SwarmBudgetLimits,
    owner: SwarmOwnerFence,
    usage: SwarmBudgetUsage,
    root_usage: SwarmUsage,
    root_usage_sequence: u64,
    root_dispatch_id: Option<IdempotencyKey>,
    reservations: BTreeMap<OperationId, SwarmForkReservation>,
    idempotency: BTreeMap<IdempotencyKey, ([u8; 32], OperationId)>,
    harness_effects: BTreeMap<(OperationId, IdempotencyKey, IdempotencyKey), u64>,
}

/// Thread-safe, atomically updated budget projection for one session.
#[derive(Clone, Debug)]
pub struct SwarmBudget {
    state: Arc<Mutex<SwarmBudgetState>>,
}

impl SwarmBudget {
    /// Opens a new session and accounts for its root agent.
    pub fn new(
        session_id: OperationId,
        owner: SwarmOwnerFence,
        limits: SwarmBudgetLimits,
    ) -> Result<Self> {
        Self::new_with_root_dispatch(session_id, owner, limits, None)
    }

    /// Opens a session and binds root usage to the host-issued scheduler
    /// lease.  Durable providers should use this constructor whenever root
    /// work has a real lease; the compatibility constructor above leaves the
    /// in-memory, receipt-free projection usable for scheduler setup.
    pub fn new_with_root_dispatch(
        session_id: OperationId,
        owner: SwarmOwnerFence,
        limits: SwarmBudgetLimits,
        root_dispatch_id: Option<IdempotencyKey>,
    ) -> Result<Self> {
        if session_id.into_bytes() == [0; 16] {
            return Err(Error::Invalid("swarm session identity is empty".into()));
        }
        owner.validate()?;
        limits.validate()?;
        if let Some(dispatch_id) = &root_dispatch_id {
            IdempotencyKey::new(dispatch_id.0.clone())?;
        }
        let usage = SwarmBudgetUsage {
            active_agents: 1,
            total_agents: 1,
            consumed: SwarmUsage::default(),
            reserved: SwarmUsage::default(),
        };
        if limits.max_active_agents == 0 || limits.max_total_agents == 0 {
            return Err(Error::Invalid("swarm root exceeds agent budget".into()));
        }
        Ok(Self {
            state: Arc::new(Mutex::new(SwarmBudgetState {
                session_id,
                limits,
                owner,
                usage,
                root_usage: SwarmUsage::default(),
                root_usage_sequence: 0,
                root_dispatch_id,
                reservations: BTreeMap::new(),
                idempotency: BTreeMap::new(),
                harness_effects: BTreeMap::new(),
            })),
        })
    }

    /// Reopens a projection from a durable event stream.
    pub fn replay(events: impl IntoIterator<Item = SwarmBudgetEvent>) -> Result<Self> {
        let mut projection: Option<Self> = None;
        for event in events {
            match (&projection, &event) {
                (
                    None,
                    SwarmBudgetEvent::Started {
                        session_id,
                        owner,
                        limits,
                        root_dispatch_id,
                    },
                ) => {
                    projection = Some(Self::new_with_root_dispatch(
                        *session_id,
                        owner.clone(),
                        *limits,
                        root_dispatch_id.clone(),
                    )?);
                }
                (None, _) => {
                    return Err(Error::Conflict("swarm events start without session".into()));
                }
                (Some(_), SwarmBudgetEvent::Started { .. }) => {
                    return Err(Error::Conflict("swarm session started twice".into()));
                }
                _ => {}
            }
            if matches!(&event, SwarmBudgetEvent::Started { .. }) {
                continue;
            }
            if let Some(value) = &projection {
                value.apply_event(event)?;
            }
        }
        projection.ok_or_else(|| Error::NotFound("swarm session".into()))
    }

    /// Returns the immutable session identity and limits.
    pub fn descriptor(&self) -> Result<(OperationId, SwarmOwnerFence, SwarmBudgetLimits)> {
        let state = self.lock()?;
        Ok((state.session_id, state.owner.clone(), state.limits))
    }

    /// Returns current usage. Counters are monotonic except active/reserved release.
    pub fn usage(&self) -> Result<SwarmBudgetUsage> {
        Ok(self.lock()?.usage)
    }

    /// Returns a reservation snapshot for recovery or reconciliation.
    pub fn reservation(&self, operation_id: OperationId) -> Result<Option<SwarmForkReservation>> {
        Ok(self.lock()?.reservations.get(&operation_id).cloned())
    }

    /// Returns the current owner fence.
    pub fn owner(&self) -> Result<SwarmOwnerFence> {
        Ok(self.lock()?.owner.clone())
    }

    /// Returns the canonical provider lease bound to root usage receipts.
    pub fn root_dispatch_id(&self) -> Result<Option<IdempotencyKey>> {
        Ok(self.lock()?.root_dispatch_id.clone())
    }

    /// Returns the durable provider receipt cursor for root usage.
    pub fn root_usage_cursor(&self) -> Result<SwarmUsageReceiptCursor> {
        let state = self.lock()?;
        SwarmUsageReceiptCursor::new(
            state.root_usage_sequence,
            (state.root_usage_sequence != 0).then_some(state.root_usage),
        )
    }

    /// Returns the root's current cumulative ceiling after descendant
    /// reservations and measured descendant consumption are accounted for.
    ///
    /// A root provider must use this ceiling before doing more work.  Using
    /// the immutable session limit directly would let root work consume
    /// capacity already held by a live descendant reservation.
    pub fn root_resource_limits(&self) -> Result<SwarmResourceRequest> {
        let state = self.lock()?;
        root_resource_limits(&state)
    }

    /// Creates a provider guard for root work from the current session
    /// projection.  The guard starts at the root's cumulative measured usage
    /// and excludes capacity already consumed or reserved by descendants.
    pub fn root_usage_limiter(&self) -> Result<SwarmUsageLimiter> {
        let state = self.lock()?;
        let limits = root_resource_limits(&state)?;
        SwarmUsageLimiter::resume(limits, state.root_usage)
    }

    /// Binds a provider measurement source to the canonical root dispatch
    /// lease and the current root receipt cursor.
    pub fn root_usage_receipt_issuer<S: SwarmUsageSource>(
        &self,
        source: S,
    ) -> Result<SwarmUsageReceiptIssuer<S>> {
        let state = self.lock()?;
        let dispatch_id = state.root_dispatch_id.clone().ok_or_else(|| {
            Error::Unauthorized("canonical root dispatch lease required".into())
        })?;
        let limits = root_resource_limits(&state)?;
        let cursor = SwarmUsageReceiptCursor::new(
            state.root_usage_sequence,
            (state.root_usage_sequence != 0).then_some(state.root_usage),
        )?;
        SwarmUsageReceiptIssuer::resume_with_cursor_and_limits(
            source,
            state.session_id,
            dispatch_id,
            cursor,
            limits,
        )
    }

    /// Creates the complete provider boundary for root work from the current
    /// session projection. The limiter includes root usage already accepted
    /// and excludes both measured and reserved descendant capacity.
    pub fn root_usage_context<S: SwarmUsageSource>(
        &self,
        source: S,
    ) -> Result<SwarmRootDispatchContext<S>> {
        let state = self.lock()?;
        let limits = root_resource_limits(&state)?;
        let limiter = SwarmUsageLimiter::resume(limits, state.root_usage)?;
        let dispatch_id = state.root_dispatch_id.clone().ok_or_else(|| {
            Error::Unauthorized("canonical root dispatch lease required".into())
        })?;
        let cursor = SwarmUsageReceiptCursor::new(
            state.root_usage_sequence,
            (state.root_usage_sequence != 0).then_some(state.root_usage),
        )?;
        let issuer = SwarmUsageReceiptIssuer::resume_with_cursor_and_limits(
            source,
            state.session_id,
            dispatch_id,
            cursor,
            limits,
        )?;
        Ok(SwarmRootDispatchContext::new(limiter, issuer))
    }

    /// Atomically admits one child. Persist the corresponding
    /// `ChildReserved` event before invoking any fork/model provider.
    pub fn reserve_child(&self, request: SwarmForkRequest) -> Result<SwarmAdmissionReceipt> {
        request.validate()?;
        let mut state = self.lock()?;
        let digest = request_digest(&request)?;
        if let Some((existing_digest, operation_id)) =
            state.idempotency.get(&request.idempotency_key)
        {
            if existing_digest != &digest || operation_id != &request.operation_id {
                return Err(Error::Conflict("swarm retry identity reused".into()));
            }
            let reservation = state
                .reservations
                .get(operation_id)
                .cloned()
                .ok_or_else(|| Error::Storage("swarm reservation index is incomplete".into()))?;
            return Ok(SwarmAdmissionReceipt {
                reservation,
                replayed: true,
            });
        }
        if state.reservations.contains_key(&request.operation_id) {
            return Err(Error::Conflict(
                "swarm operation identity already exists".into(),
            ));
        }
        if request.operation_id == state.session_id {
            return Err(Error::Conflict(
                "swarm child cannot reuse session identity".into(),
            ));
        }
        if let Some(parent) = request.parent_operation_id {
            let parent = state
                .reservations
                .get(&parent)
                .ok_or_else(|| Error::NotFound(format!("swarm parent {parent}")))?;
            if !matches!(
                parent.state,
                SwarmReservationState::Reserved | SwarmReservationState::Active
            ) || request.depth != parent.depth.saturating_add(1)
            {
                return Err(Error::Conflict(
                    "swarm parent is not an active direct ancestor".into(),
                ));
            }
            let remaining = SwarmResourceRequest {
                model_steps: parent
                    .resources
                    .model_steps
                    .saturating_sub(parent.usage.model_steps),
                output_bytes: parent
                    .resources
                    .output_bytes
                    .saturating_sub(parent.usage.output_bytes),
                execution_time_ms: parent
                    .resources
                    .execution_time_ms
                    .saturating_sub(parent.usage.execution_time_ms),
            };
            // A parent may admit more than one child. Each live direct child
            // holds a portion of the parent's ceiling, so a retry or a
            // concurrent sibling cannot reserve the same remainder twice.
            // A completed child has consumed part of its parent's ceiling.
            // Keep that usage charged when admitting a later sibling; only
            // the still-unspent portion of a live child is available again.
            let allocated_to_children = state
                .reservations
                .values()
                .filter(|child| child.parent_operation_id == Some(parent.operation_id))
                .try_fold(SwarmUsage::default(), |allocated, child| {
                    let child_usage = match child.state {
                        SwarmReservationState::Reserved | SwarmReservationState::Active => {
                            SwarmUsage {
                                model_steps: child.resources.model_steps,
                                output_bytes: child.resources.output_bytes,
                                execution_time_ms: child.resources.execution_time_ms,
                            }
                        }
                        SwarmReservationState::Completed | SwarmReservationState::Cancelled => {
                            reservation_subtree_usage(&state, child.operation_id)?
                        }
                    };
                    add_usage(allocated, child_usage)
                })?;
            let remaining = SwarmResourceRequest {
                model_steps: remaining
                    .model_steps
                    .checked_sub(allocated_to_children.model_steps)
                    .ok_or_else(|| {
                        Error::Conflict("swarm parent child budget is exhausted".into())
                    })?,
                output_bytes: remaining
                    .output_bytes
                    .checked_sub(allocated_to_children.output_bytes)
                    .ok_or_else(|| {
                        Error::Conflict("swarm parent child budget is exhausted".into())
                    })?,
                execution_time_ms: remaining
                    .execution_time_ms
                    .checked_sub(allocated_to_children.execution_time_ms)
                    .ok_or_else(|| {
                        Error::Conflict("swarm parent child budget is exhausted".into())
                    })?,
            };
            if request.resources.model_steps > remaining.model_steps
                || request.resources.output_bytes > remaining.output_bytes
                || request.resources.execution_time_ms > remaining.execution_time_ms
            {
                return Err(Error::Conflict(
                    "swarm descendant exceeds parent remaining resource budget".into(),
                ));
            }
        } else if request.depth != 1 {
            return Err(Error::Conflict("root child must have depth one".into()));
        }
        if request.depth > state.limits.max_recursion_depth {
            return Err(Error::Conflict("swarm recursion depth exceeded".into()));
        }
        if state.usage.active_agents >= state.limits.max_active_agents
            || state.usage.total_agents >= state.limits.max_total_agents
        {
            return Err(Error::Conflict("swarm agent limit exceeded".into()));
        }
        let limits = state.limits;
        reserve_resources(&mut state.usage, request.resources, limits)?;
        state.usage.active_agents += 1;
        state.usage.total_agents += 1;
        let reservation = SwarmForkReservation {
            operation_id: request.operation_id,
            parent_operation_id: request.parent_operation_id,
            depth: request.depth,
            owner: state.owner.clone(),
            resources: request.resources,
            idempotency_key: request.idempotency_key.clone(),
            usage: SwarmUsage::default(),
            usage_sequence: 0,
            dispatch_id: None,
            state: SwarmReservationState::Reserved,
            publication: None,
            request_digest: digest,
            admission_digest: request.admission_digest,
        };
        state
            .idempotency
            .insert(request.idempotency_key, (digest, request.operation_id));
        state
            .reservations
            .insert(request.operation_id, reservation.clone());
        Ok(SwarmAdmissionReceipt {
            reservation,
            replayed: false,
        })
    }

    /// Records complete model/workspace publication and returns a dispatch token.
    pub(crate) fn activate(
        &self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        publication: ForkPublication,
    ) -> Result<SwarmDispatchToken> {
        self.activate_with_dispatch(operation_id, owner, publication, None)
    }

    /// Records publication and the provider attempt identity atomically.
    pub(crate) fn activate_with_dispatch(
        &self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        publication: ForkPublication,
        dispatch_id: Option<IdempotencyKey>,
    ) -> Result<SwarmDispatchToken> {
        publication.validate()?;
        if let Some(dispatch_id) = &dispatch_id {
            IdempotencyKey::new(dispatch_id.0.clone())?;
        }
        let mut state = self.lock()?;
        require_owner(&state, &owner)?;
        let mut reservation = state
            .reservations
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("swarm reservation {operation_id}")))?;
        if reservation.owner != owner {
            return Err(Error::Conflict("stale swarm reservation generation".into()));
        }
        if publication.operation_id != operation_id
            || publication.parent_operation_id != reservation.parent_operation_id
        {
            return Err(Error::Conflict(
                "fork publication does not match admission".into(),
            ));
        }
        match reservation.state {
            SwarmReservationState::Reserved => {
                reservation.state = SwarmReservationState::Active;
                reservation.publication = Some(publication);
                reservation.dispatch_id = dispatch_id.clone();
            }
            SwarmReservationState::Active => {
                if reservation.publication.as_ref() != Some(&publication) {
                    return Err(Error::Conflict("fork activation retry differs".into()));
                }
                if reservation.dispatch_id != dispatch_id {
                    return Err(Error::Conflict("fork dispatch retry differs".into()));
                }
            }
            SwarmReservationState::Completed | SwarmReservationState::Cancelled => {
                return Err(Error::Conflict("swarm reservation is terminal".into()));
            }
        }
        let parent_operation_id = reservation.parent_operation_id;
        let resources = reservation.resources;
        state.reservations.insert(operation_id, reservation);
        Ok(SwarmDispatchToken {
            operation_id,
            parent_operation_id,
            owner,
            completed_boundary_digest: publication.completed_boundary_digest,
            workspace_generation_digest: publication.workspace_generation_digest,
            dispatch_id,
            resources,
        })
    }

    /// Reports cumulative usage and retains the unconsumed remainder.
    pub(crate) fn report_usage(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
    ) -> Result<SwarmForkReservation> {
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        update_usage(&mut state, operation_id, owner, usage, false, None)
    }

    /// Completes a child and releases only its unconsumed active reservation.
    pub(crate) fn complete(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
    ) -> Result<SwarmForkReservation> {
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        update_usage(&mut state, operation_id, owner, usage, true, None)
    }

    /// Reports cumulative root usage against the same session-wide limits as
    /// descendants. Root consumption is never refunded and reduces the
    /// resources that descendants may reserve.
    pub(crate) fn report_root_usage(
        &self,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
    ) -> Result<SwarmUsage> {
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        update_root_usage(&mut state, owner, usage, None)
    }

    fn report_usage_event(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
    ) -> Result<SwarmForkReservation> {
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        update_usage(&mut state, operation_id, owner, usage, false, receipt)
    }

    fn complete_event(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
    ) -> Result<SwarmForkReservation> {
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        update_usage(&mut state, operation_id, owner, usage, true, receipt)
    }

    fn report_root_usage_event(
        &self,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
    ) -> Result<SwarmUsage> {
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        update_root_usage(&mut state, owner, usage, receipt)
    }

    /// Records one authenticated Harness effect measurement in the same
    /// projection as reservations and provider usage. Replaying the exact
    /// event is idempotent; a changed measurement for the same effect is a
    /// conflict rather than a second charge.
    pub(crate) fn record_harness_effect(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        dispatch_id: &IdempotencyKey,
        effect_id: &IdempotencyKey,
        elapsed_ms: u64,
    ) -> Result<bool> {
        if elapsed_ms == 0 {
            return Err(Error::Invalid(
                "Harness effect measurement must be positive".into(),
            ));
        }
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        if operation_id == state.session_id {
            if state.root_dispatch_id.as_ref() != Some(dispatch_id) {
                return Err(Error::Conflict(
                    "Harness effect dispatch is not the canonical root lease".into(),
                ));
            }
        } else {
            let reservation = state
                .reservations
                .get(&operation_id)
                .ok_or_else(|| Error::NotFound(format!("swarm reservation {operation_id}")))?;
            if reservation.dispatch_id.as_ref() != Some(dispatch_id)
                || reservation.state != SwarmReservationState::Active
            {
                return Err(Error::Conflict(
                    "Harness effect dispatch is not an active reservation".into(),
                ));
            }
        }
        let key = (operation_id, dispatch_id.clone(), effect_id.clone());
        match state.harness_effects.get(&key) {
            Some(existing) if *existing == elapsed_ms => Ok(false),
            // The first durable event is authoritative when a caller retries
            // after losing its acknowledgement. Keep that original measured
            // value instead of charging or replacing it with a new sample.
            Some(_) => Ok(false),
            None => {
                let next = state
                    .usage
                    .consumed
                    .execution_time_ms
                    .checked_add(elapsed_ms)
                    .ok_or_else(|| Error::Conflict("Harness effect time exhausted".into()))?;
                if next
                    .checked_add(state.usage.reserved.execution_time_ms)
                    .is_none_or(|value| value > state.limits.max_execution_time_ms)
                {
                    return Err(Error::Conflict(
                        "Harness effect exceeds remaining swarm execution budget".into(),
                    ));
                }
                state.usage.consumed.execution_time_ms = next;
                state.harness_effects.insert(key, elapsed_ms);
                Ok(true)
            }
        }
    }


    /// Cancels a child and releases active/unconsumed resources without refunding consumed usage.
    pub fn cancel(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
    ) -> Result<SwarmForkReservation> {
        let mut state = self.lock()?;
        require_owner(&state, owner)?;
        let reservation = state
            .reservations
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("swarm reservation {operation_id}")))?;
        if reservation.owner != *owner {
            return Err(Error::Conflict("stale swarm reservation generation".into()));
        }
        if matches!(reservation.state, SwarmReservationState::Completed) {
            return Err(Error::Conflict(
                "completed swarm child cannot be cancelled".into(),
            ));
        }
        if state.reservations.values().any(|descendant| {
            descendant.parent_operation_id == Some(operation_id)
                && matches!(
                    descendant.state,
                    SwarmReservationState::Reserved | SwarmReservationState::Active
                )
        }) {
            return Err(Error::Conflict(
                "swarm child with live descendants cannot be cancelled".into(),
            ));
        }
        if reservation.state == SwarmReservationState::Cancelled {
            return Ok(reservation);
        }
        release_remaining(&mut state.usage, &reservation)?;
        state.usage.active_agents = state.usage.active_agents.saturating_sub(1);
        let reservation = state
            .reservations
            .get_mut(&operation_id)
            .ok_or_else(|| Error::NotFound(format!("swarm reservation {operation_id}")))?;
        reservation.state = SwarmReservationState::Cancelled;
        Ok(reservation.clone())
    }

    /// Takes ownership after restart and advances the generation exactly once.
    /// The expected fence must be authenticated by the caller before recovery.
    /// Active reservations are rebound to the new fence; stale tokens cannot
    /// mutate them.
    pub fn takeover(
        &self,
        expected_owner: &SwarmOwnerFence,
        owner: impl Into<String>,
    ) -> Result<SwarmOwnerFence> {
        let mut state = self.lock()?;
        if &state.owner != expected_owner {
            return Err(Error::Conflict("swarm owner generation is stale".into()));
        }
        let generation = state
            .owner
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("swarm owner generation exhausted".into()))?;
        let fence = SwarmOwnerFence::new(owner, generation)?;
        state.owner = fence.clone();
        for reservation in state.reservations.values_mut() {
            if matches!(
                reservation.state,
                SwarmReservationState::Reserved | SwarmReservationState::Active
            ) {
                reservation.owner = fence.clone();
            }
        }
        Ok(fence)
    }

    /// Applies a previously persisted event exactly once at the projection layer.
    pub(crate) fn apply_event(&self, event: SwarmBudgetEvent) -> Result<()> {
        match event {
            SwarmBudgetEvent::Started { .. } => {
                Err(Error::Conflict("swarm session already exists".into()))
            }
            SwarmBudgetEvent::ChildReserved { reservation } => {
                let request = request_from_reservation(&reservation)?;
                let receipt = self.reserve_child(request)?;
                if receipt.reservation != reservation {
                    return Err(Error::Conflict(
                        "persisted swarm reservation differs".into(),
                    ));
                }
                Ok(())
            }
            SwarmBudgetEvent::ChildActivated {
                operation_id,
                owner,
                publication,
                dispatch_id,
            } => self
                .activate_with_dispatch(operation_id, owner, publication, dispatch_id)
                .map(|_| ()),
            SwarmBudgetEvent::HarnessEffectMeasured {
                operation_id,
                owner,
                dispatch_id,
                effect_id,
                elapsed_ms,
            } => self
                .record_harness_effect(
                    operation_id,
                    &owner,
                    &dispatch_id,
                    &effect_id,
                    elapsed_ms,
                )
                .map(|_| ()),
            SwarmBudgetEvent::UsageReported {
                operation_id,
                owner,
                usage,
                receipt,
            } => self
                .report_usage_event(operation_id, &owner, usage, Some(&receipt))
                .map(|_| ()),
            SwarmBudgetEvent::RootUsageReported {
                owner,
                usage,
                receipt,
            } => self
                .report_root_usage_event(&owner, usage, Some(&receipt))
                .map(|_| ()),
            SwarmBudgetEvent::ChildCompleted {
                operation_id,
                owner,
                usage,
                receipt,
            } => self
                .complete_event(operation_id, &owner, usage, Some(&receipt))
                .map(|_| ()),
            SwarmBudgetEvent::ChildCancelled {
                operation_id,
                owner,
            } => self.cancel(operation_id, &owner).map(|_| ()),
            SwarmBudgetEvent::OwnerTakenOver { owner } => {
                let state = self.lock()?;
                let expected = state.owner.clone();
                drop(state);
                let next_generation = expected
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("swarm owner generation exhausted".into()))?;
                if owner.generation != next_generation {
                    return Err(Error::Conflict(
                        "persisted takeover generation does not advance exactly once".into(),
                    ));
                }
                let observed = self.takeover(&expected, owner.owner.clone())?;
                if observed != owner {
                    return Err(Error::Conflict("persisted takeover differs".into()));
                }
                Ok(())
            }
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, SwarmBudgetState>> {
        self.state
            .lock()
            .map_err(|_| Error::Storage("swarm budget lock is poisoned".into()))
    }
}

fn request_digest(request: &SwarmForkRequest) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&canonical_json_bytes(request)?).as_bytes())
}

fn request_from_reservation(reservation: &SwarmForkReservation) -> Result<SwarmForkRequest> {
    let request = SwarmForkRequest {
        operation_id: reservation.operation_id,
        idempotency_key: reservation.idempotency_key.clone(),
        parent_operation_id: reservation.parent_operation_id,
        depth: reservation.depth,
        resources: reservation.resources,
        admission_digest: reservation.admission_digest,
    };
    if request_digest(&request)? != reservation.request_digest {
        return Err(Error::Conflict(
            "persisted swarm request digest differs".into(),
        ));
    }
    Ok(request)
}

fn require_owner(state: &SwarmBudgetState, owner: &SwarmOwnerFence) -> Result<()> {
    if &state.owner != owner {
        return Err(Error::Conflict("stale swarm owner generation".into()));
    }
    Ok(())
}

fn add_usage(current: SwarmUsage, delta: SwarmUsage) -> Result<SwarmUsage> {
    Ok(SwarmUsage {
        model_steps: current
            .model_steps
            .checked_add(delta.model_steps)
            .ok_or_else(|| Error::Invalid("swarm step usage exhausted".into()))?,
        output_bytes: current
            .output_bytes
            .checked_add(delta.output_bytes)
            .ok_or_else(|| Error::Invalid("swarm output usage exhausted".into()))?,
        execution_time_ms: current
            .execution_time_ms
            .checked_add(delta.execution_time_ms)
            .ok_or_else(|| Error::Invalid("swarm time usage exhausted".into()))?,
    })
}

fn reservation_subtree_usage(
    state: &SwarmBudgetState,
    operation_id: OperationId,
) -> Result<SwarmUsage> {
    let reservation = state
        .reservations
        .get(&operation_id)
        .ok_or_else(|| Error::NotFound(format!("swarm reservation {operation_id}")))?;
    let mut total = reservation.usage;
    for child in state
        .reservations
        .values()
        .filter(|child| child.parent_operation_id == Some(operation_id))
    {
        total = add_usage(total, reservation_subtree_usage(state, child.operation_id)?)?;
    }
    Ok(total)
}

fn reservation_resources(reservation: &SwarmForkReservation) -> SwarmUsage {
    SwarmUsage {
        model_steps: reservation.resources.model_steps,
        output_bytes: reservation.resources.output_bytes,
        execution_time_ms: reservation.resources.execution_time_ms,
    }
}

fn root_resource_limits(state: &SwarmBudgetState) -> Result<SwarmResourceRequest> {
    let ceiling = |limit: u64, consumed: u64, root: u64, reserved: u64| {
        limit
            .checked_sub(consumed)
            .and_then(|value| value.checked_sub(reserved))
            .and_then(|remaining| root.checked_add(remaining))
            .ok_or_else(|| Error::Storage("swarm root resource projection underflow".into()))
    };
    let limits = SwarmResourceRequest {
        model_steps: ceiling(
            state.limits.max_model_steps,
            state.usage.consumed.model_steps,
            state.root_usage.model_steps,
            state.usage.reserved.model_steps,
        )?,
        output_bytes: ceiling(
            state.limits.max_output_bytes,
            state.usage.consumed.output_bytes,
            state.root_usage.output_bytes,
            state.usage.reserved.output_bytes,
        )?,
        execution_time_ms: ceiling(
            state.limits.max_execution_time_ms,
            state.usage.consumed.execution_time_ms,
            state.root_usage.execution_time_ms,
            state.usage.reserved.execution_time_ms,
        )?,
    };
    if limits.model_steps == 0 || limits.output_bytes == 0 || limits.execution_time_ms == 0 {
        return Err(Error::Conflict(
            "swarm root has no remaining session resource budget".into(),
        ));
    }
    Ok(limits)
}

/// Returns the commitment held by an operation's direct descendants.  A live
/// descendant retains its full admitted ceiling until it completes or is
/// cancelled; a terminal descendant contributes only measured usage.  This is
/// the same distinction used by sibling admission and prevents a parent from
/// spending capacity that is already held by a descendant provider.
fn direct_descendant_commitment(
    state: &SwarmBudgetState,
    operation_id: OperationId,
    replacing: OperationId,
    replacement_usage: SwarmUsage,
    replacement_complete: bool,
) -> Result<SwarmUsage> {
    state
        .reservations
        .values()
        .filter(|child| child.parent_operation_id == Some(operation_id))
        .try_fold(SwarmUsage::default(), |total, child| {
            let commitment = if child.operation_id == replacing {
                if replacement_complete {
                    let mut measured = replacement_usage;
                    for descendant in state.reservations.values().filter(|descendant| {
                        descendant.parent_operation_id == Some(child.operation_id)
                    }) {
                        measured = add_usage(
                            measured,
                            reservation_subtree_usage(state, descendant.operation_id)?,
                        )?;
                    }
                    measured
                } else {
                    reservation_resources(child)
                }
            } else if matches!(
                child.state,
                SwarmReservationState::Reserved | SwarmReservationState::Active
            ) {
                reservation_resources(child)
            } else {
                reservation_subtree_usage(state, child.operation_id)?
            };
            add_usage(total, commitment)
        })
}

/// Checks the operation's own ceiling and every ancestor ceiling before a
/// cumulative usage transition mutates the projection.  Session-wide limits
/// alone are insufficient: a parent can otherwise consume its full ceiling
/// after already reserving the same capacity for a grandchild.
fn validate_ancestor_ceilings(
    state: &SwarmBudgetState,
    reservation: &SwarmForkReservation,
    usage: SwarmUsage,
    complete: bool,
) -> Result<()> {
    let own_descendants = direct_descendant_commitment(
        state,
        reservation.operation_id,
        reservation.operation_id,
        usage,
        complete,
    )?;
    let own_total = add_usage(usage, own_descendants)?;
    if own_total.model_steps > reservation.resources.model_steps
        || own_total.output_bytes > reservation.resources.output_bytes
        || own_total.execution_time_ms > reservation.resources.execution_time_ms
    {
        return Err(Error::Conflict(
            "swarm usage exceeds its remaining descendant resource budget".into(),
        ));
    }

    let mut child_id = reservation.operation_id;
    let mut parent_id = reservation.parent_operation_id;
    while let Some(parent_operation_id) = parent_id {
        let parent = state
            .reservations
            .get(&parent_operation_id)
            .ok_or_else(|| Error::Storage("swarm ancestor reservation is missing".into()))?;
        let descendants =
            direct_descendant_commitment(state, parent_operation_id, child_id, usage, complete)?;
        let total = add_usage(parent.usage, descendants)?;
        if total.model_steps > parent.resources.model_steps
            || total.output_bytes > parent.resources.output_bytes
            || total.execution_time_ms > parent.resources.execution_time_ms
        {
            return Err(Error::Conflict(
                "swarm usage exceeds an ancestor resource budget".into(),
            ));
        }
        child_id = parent_operation_id;
        parent_id = parent.parent_operation_id;
    }
    Ok(())
}

fn reserve_resources(
    usage: &mut SwarmBudgetUsage,
    request: SwarmResourceRequest,
    limits: SwarmBudgetLimits,
) -> Result<()> {
    let next = |consumed: u64, reserved: u64, requested: u64, limit: u64| -> Result<u64> {
        consumed
            .checked_add(reserved)
            .and_then(|value| value.checked_add(requested))
            .filter(|value| *value <= limit)
            .ok_or_else(|| Error::Conflict("swarm session resource budget exceeded".into()))
    };
    let values = [
        (
            usage.consumed.model_steps,
            usage.reserved.model_steps,
            request.model_steps,
        ),
        (
            usage.consumed.output_bytes,
            usage.reserved.output_bytes,
            request.output_bytes,
        ),
        (
            usage.consumed.execution_time_ms,
            usage.reserved.execution_time_ms,
            request.execution_time_ms,
        ),
    ];
    // Evaluate each dimension as a fallible operation.  Keeping these as an
    // array of `Result`s would only construct the errors and then discard
    // them, allowing a request that exceeds one dimension to be admitted.
    next(
        values[0].0,
        values[0].1,
        values[0].2,
        limits.max_model_steps,
    )?;
    next(
        values[1].0,
        values[1].1,
        values[1].2,
        limits.max_output_bytes,
    )?;
    next(
        values[2].0,
        values[2].1,
        values[2].2,
        limits.max_execution_time_ms,
    )?;
    usage.reserved.model_steps = usage
        .reserved
        .model_steps
        .checked_add(request.model_steps)
        .ok_or_else(|| Error::Invalid("swarm step reservation exhausted".into()))?;
    usage.reserved.output_bytes = usage
        .reserved
        .output_bytes
        .checked_add(request.output_bytes)
        .ok_or_else(|| Error::Invalid("swarm output reservation exhausted".into()))?;
    usage.reserved.execution_time_ms = usage
        .reserved
        .execution_time_ms
        .checked_add(request.execution_time_ms)
        .ok_or_else(|| Error::Invalid("swarm time reservation exhausted".into()))?;
    Ok(())
}

fn release_remaining(
    usage: &mut SwarmBudgetUsage,
    reservation: &SwarmForkReservation,
) -> Result<()> {
    usage.reserved.model_steps = usage
        .reserved
        .model_steps
        .checked_sub(
            reservation
                .resources
                .model_steps
                .saturating_sub(reservation.usage.model_steps),
        )
        .ok_or_else(|| Error::Storage("swarm step reservation underflow".into()))?;
    usage.reserved.output_bytes = usage
        .reserved
        .output_bytes
        .checked_sub(
            reservation
                .resources
                .output_bytes
                .saturating_sub(reservation.usage.output_bytes),
        )
        .ok_or_else(|| Error::Storage("swarm output reservation underflow".into()))?;
    usage.reserved.execution_time_ms = usage
        .reserved
        .execution_time_ms
        .checked_sub(
            reservation
                .resources
                .execution_time_ms
                .saturating_sub(reservation.usage.execution_time_ms),
        )
        .ok_or_else(|| Error::Storage("swarm time reservation underflow".into()))?;
    Ok(())
}

fn update_usage(
    state: &mut SwarmBudgetState,
    operation_id: OperationId,
    owner: &SwarmOwnerFence,
    usage: SwarmUsage,
    complete: bool,
    receipt: Option<&SwarmUsageReceipt>,
) -> Result<SwarmForkReservation> {
    let mut reservation = state
        .reservations
        .get(&operation_id)
        .cloned()
        .ok_or_else(|| Error::NotFound(format!("swarm reservation {operation_id}")))?;
    if reservation.owner != *owner {
        return Err(Error::Conflict("stale swarm reservation generation".into()));
    }
    if let Some(receipt) = receipt {
        validate_child_receipt(&reservation, operation_id, usage, receipt)?;
    }
    if reservation.state == SwarmReservationState::Completed
        && complete
        && usage == reservation.usage
    {
        return Ok(reservation);
    }
    if reservation.state != SwarmReservationState::Active {
        return Err(Error::Conflict("swarm child is not active".into()));
    }
    if complete && has_live_descendant(state, operation_id) {
        return Err(Error::Conflict(
            "swarm child with live descendants cannot complete".into(),
        ));
    }
    let delta = usage.checked_delta(reservation.usage)?;
    if usage.model_steps > reservation.resources.model_steps
        || usage.output_bytes > reservation.resources.output_bytes
        || usage.execution_time_ms > reservation.resources.execution_time_ms
    {
        return Err(Error::Conflict(
            "child usage exceeds its reservation".into(),
        ));
    }
    validate_ancestor_ceilings(state, &reservation, usage, complete)?;
    state.usage.reserved.model_steps = state
        .usage
        .reserved
        .model_steps
        .checked_sub(delta.model_steps)
        .ok_or_else(|| Error::Storage("swarm step reservation underflow".into()))?;
    state.usage.reserved.output_bytes = state
        .usage
        .reserved
        .output_bytes
        .checked_sub(delta.output_bytes)
        .ok_or_else(|| Error::Storage("swarm output reservation underflow".into()))?;
    state.usage.reserved.execution_time_ms = state
        .usage
        .reserved
        .execution_time_ms
        .checked_sub(delta.execution_time_ms)
        .ok_or_else(|| Error::Storage("swarm time reservation underflow".into()))?;
    state.usage.consumed.model_steps = state
        .usage
        .consumed
        .model_steps
        .checked_add(delta.model_steps)
        .ok_or_else(|| Error::Invalid("swarm step usage exhausted".into()))?;
    state.usage.consumed.output_bytes = state
        .usage
        .consumed
        .output_bytes
        .checked_add(delta.output_bytes)
        .ok_or_else(|| Error::Invalid("swarm output usage exhausted".into()))?;
    state.usage.consumed.execution_time_ms = state
        .usage
        .consumed
        .execution_time_ms
        .checked_add(delta.execution_time_ms)
        .ok_or_else(|| Error::Invalid("swarm time usage exhausted".into()))?;
    reservation.usage = usage;
    if let Some(receipt) = receipt {
        reservation.usage_sequence = receipt.sequence;
    }
    if complete {
        release_remaining(&mut state.usage, &reservation)?;
        state.usage.active_agents = state.usage.active_agents.saturating_sub(1);
        reservation.state = SwarmReservationState::Completed;
    }
    state.reservations.insert(operation_id, reservation.clone());
    Ok(reservation.clone())
}

fn validate_child_receipt(
    reservation: &SwarmForkReservation,
    operation_id: OperationId,
    usage: SwarmUsage,
    receipt: &SwarmUsageReceipt,
) -> Result<()> {
    receipt.validate()?;
    let expected_sequence = reservation
        .usage_sequence
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("swarm usage receipt sequence exhausted".into()))?;
    if receipt.operation_id != operation_id
        || receipt.usage != usage
        || receipt.sequence != expected_sequence
        || reservation
            .dispatch_id
            .as_ref()
            .is_some_and(|dispatch_id| dispatch_id != &receipt.dispatch_id)
    {
        return Err(Error::Conflict(
            "swarm child usage receipt is stale or mismatched".into(),
        ));
    }
    Ok(())
}

fn update_root_usage(
    state: &mut SwarmBudgetState,
    _owner: &SwarmOwnerFence,
    usage: SwarmUsage,
    receipt: Option<&SwarmUsageReceipt>,
) -> Result<SwarmUsage> {
    if let Some(receipt) = receipt {
        receipt.validate()?;
        let expected_sequence = state
            .root_usage_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("swarm root usage receipt sequence exhausted".into()))?;
        let root_dispatch_id = state.root_dispatch_id.as_ref().ok_or_else(|| {
            Error::Unauthorized("canonical root dispatch lease required".into())
        })?;
        if receipt.operation_id != state.session_id
            || receipt.usage != usage
            || receipt.sequence != expected_sequence
            || root_dispatch_id != &receipt.dispatch_id
        {
            return Err(Error::Conflict(
                "swarm root usage receipt is stale or mismatched".into(),
            ));
        }
    }
    let delta = usage.checked_delta(state.root_usage)?;
    let next = add_usage(state.usage.consumed, delta)?;
    if next
        .model_steps
        .checked_add(state.usage.reserved.model_steps)
        .is_none_or(|value| value > state.limits.max_model_steps)
        || next
            .output_bytes
            .checked_add(state.usage.reserved.output_bytes)
            .is_none_or(|value| value > state.limits.max_output_bytes)
        || next
            .execution_time_ms
            .checked_add(state.usage.reserved.execution_time_ms)
            .is_none_or(|value| value > state.limits.max_execution_time_ms)
    {
        return Err(Error::Conflict(
            "root usage exceeds remaining swarm resource budget".into(),
        ));
    }
    state.root_usage = usage;
    state.usage.consumed = next;
    if let Some(receipt) = receipt {
        state.root_usage_sequence = receipt.sequence;
    }
    Ok(usage)
}

fn has_live_descendant(state: &SwarmBudgetState, operation_id: OperationId) -> bool {
    let mut pending = state
        .reservations
        .values()
        .filter(|reservation| {
            reservation.parent_operation_id == Some(operation_id)
                && matches!(
                    reservation.state,
                    SwarmReservationState::Reserved | SwarmReservationState::Active
                )
        })
        .map(|reservation| reservation.operation_id)
        .collect::<Vec<_>>();
    while let Some(candidate) = pending.pop() {
        if state.reservations.values().any(|reservation| {
            reservation.parent_operation_id == Some(candidate)
                && matches!(
                    reservation.state,
                    SwarmReservationState::Reserved | SwarmReservationState::Active
                )
        }) {
            return true;
        }
        pending.extend(
            state
                .reservations
                .values()
                .filter(|reservation| {
                    reservation.parent_operation_id == Some(candidate)
                        && matches!(
                            reservation.state,
                            SwarmReservationState::Reserved | SwarmReservationState::Active
                        )
                })
                .map(|reservation| reservation.operation_id),
        );
    }
    state.reservations.values().any(|reservation| {
        reservation.parent_operation_id == Some(operation_id)
            && matches!(
                reservation.state,
                SwarmReservationState::Reserved | SwarmReservationState::Active
            )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Capabilities,
        conversation::Limits,
        runtime::{TaskAdmissionRecord, TaskRunLimits},
    };
    use serde_json::json;
    use std::sync::Mutex;
    fn id(byte: u8) -> OperationId {
        OperationId::from_bytes([byte; 16])
    }

    fn owner(generation: u64) -> SwarmOwnerFence {
        SwarmOwnerFence::new("host", generation).expect("owner")
    }

    fn limits() -> SwarmBudgetLimits {
        SwarmBudgetLimits {
            max_active_agents: 8,
            max_total_agents: 8,
            max_recursion_depth: 2,
            max_model_steps: 10,
            max_output_bytes: 100,
            max_execution_time_ms: 1_000,
        }
    }

    fn request(byte: u8, parent: Option<OperationId>) -> SwarmForkRequest {
        SwarmForkRequest {
            operation_id: id(byte),
            idempotency_key: IdempotencyKey::new(format!("key-{byte}")).expect("key"),
            parent_operation_id: parent,
            depth: if parent.is_some() { 2 } else { 1 },
            resources: SwarmResourceRequest {
                model_steps: 4,
                output_bytes: 40,
                execution_time_ms: 400,
            },
            admission_digest: None,
        }
    }

    fn publication(operation_id: OperationId, parent: Option<OperationId>) -> ForkPublication {
        ForkPublication {
            operation_id,
            parent_operation_id: parent,
            completed_boundary_digest: [1; 32],
            workspace_generation_digest: [2; 32],
        }
    }

    #[test]
    fn child_resources_cannot_widen_canonical_task_limits() -> Result<()> {
        let admission = TaskAdmissionRecord::from_parts(
            id(8),
            "example.task",
            "1",
            json!(7),
            json!({"type":"integer"}),
            json!({"type":"string"}),
            &std::collections::BTreeSet::new(),
            &[9; 32],
            None,
            Capabilities::new([] as [&str; 0]),
            Limits::default(),
            TaskRunLimits::default(),
            None,
            None,
            None,
        )?;
        assert!(
            SwarmResourceRequest {
                model_steps: 65,
                output_bytes: 1,
                execution_time_ms: 1,
            }
            .validate_against_admission(&admission)
            .is_err()
        );
        assert!(
            SwarmResourceRequest {
                model_steps: 1,
                output_bytes: Limits::default().render_bytes + 1,
                execution_time_ms: 1,
            }
            .validate_against_admission(&admission)
            .is_err()
        );
        assert!(
            SwarmResourceRequest {
                model_steps: 1,
                output_bytes: 1,
                execution_time_ms: MAX_SWARM_EXECUTION_TIME_MS + 1,
            }
            .validate_against_admission(&admission)
            .is_err()
        );
        Ok(())
    }

    struct MeasuredSource {
        snapshots: Mutex<Vec<SwarmUsage>>,
    }

    impl SwarmUsageSource for MeasuredSource {
        fn provider_identity(&self) -> &str {
            "local-provider"
        }

        fn cumulative_usage(
            &self,
            _operation_id: OperationId,
            _dispatch_id: &IdempotencyKey,
        ) -> Result<SwarmUsage> {
            self.snapshots
                .lock()
                .map_err(|_| Error::Storage("measurement source lock poisoned".into()))?
                .pop()
                .ok_or_else(|| Error::Storage("measurement source exhausted".into()))
        }
    }

    #[test]
    fn receipt_issuer_reads_each_provider_dimension_and_sequences_retries() -> Result<()> {
        let operation_id = id(18);
        let source = MeasuredSource {
            snapshots: Mutex::new(vec![
                SwarmUsage {
                    model_steps: 7,
                    output_bytes: 4096,
                    execution_time_ms: 120,
                },
                SwarmUsage {
                    model_steps: 3,
                    output_bytes: 1024,
                    execution_time_ms: 40,
                },
            ]),
        };
        let mut issuer = SwarmUsageReceiptIssuer::new(
            source,
            operation_id,
            IdempotencyKey::new("dispatch-measured")?,
        )?;
        let first = issuer.issue()?.into_receipt();
        assert_eq!(first.sequence, 1);
        assert_eq!(
            first.usage,
            SwarmUsage {
                model_steps: 3,
                output_bytes: 1024,
                execution_time_ms: 40,
            }
        );
        let second = issuer.issue()?.into_receipt();
        assert_eq!(second.sequence, 2);
        assert_eq!(
            second.usage,
            SwarmUsage {
                model_steps: 7,
                output_bytes: 4096,
                execution_time_ms: 120,
            }
        );
        assert_eq!(issuer.next_sequence()?, 3);
        Ok(())
    }

    #[test]
    fn bounded_receipt_issuer_rejects_any_provider_dimension_over_ceiling() -> Result<()> {
        let source = MeasuredSource {
            snapshots: Mutex::new(vec![SwarmUsage {
                model_steps: 5,
                output_bytes: 10,
                execution_time_ms: 100,
            }]),
        };
        let mut issuer = SwarmUsageReceiptIssuer::with_limits(
            source,
            id(19),
            IdempotencyKey::new("dispatch-bounded")?,
            SwarmResourceRequest {
                model_steps: 4,
                output_bytes: 10,
                execution_time_ms: 100,
            },
        )?;
        assert!(matches!(issuer.issue(), Err(Error::Conflict(_))));
        Ok(())
    }

    #[test]
    fn receipt_issuer_resume_preserves_sequence_and_cumulative_cursor() -> Result<()> {
        let source = MeasuredSource {
            snapshots: Mutex::new(vec![SwarmUsage {
                model_steps: 8,
                output_bytes: 2048,
                execution_time_ms: 90,
            }]),
        };
        let mut issuer = SwarmUsageReceiptIssuer::resume_with_limits(
            source,
            id(22),
            IdempotencyKey::new("dispatch-resume")?,
            3,
            Some(SwarmUsage {
                model_steps: 7,
                output_bytes: 1024,
                execution_time_ms: 80,
            }),
            SwarmResourceRequest {
                model_steps: 8,
                output_bytes: 2048,
                execution_time_ms: 90,
            },
        )?;
        let receipt = issuer.issue()?.into_receipt();
        assert_eq!(receipt.sequence, 4);
        assert_eq!(issuer.next_sequence()?, 5);
        assert!(
            SwarmUsageReceiptIssuer::resume(
                MeasuredSource {
                    snapshots: Mutex::new(Vec::new()),
                },
                id(22),
                IdempotencyKey::new("dispatch-resume")?,
                2,
                None,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn provider_limiter_stops_before_each_independent_dimension_exceeds() -> Result<()> {
        let limits = SwarmResourceRequest {
            model_steps: 1,
            output_bytes: 4,
            execution_time_ms: 10,
        };
        let mut limiter = SwarmUsageLimiter::new(limits)?;
        limiter.admit_model_step()?;
        assert!(matches!(
            limiter.admit_model_step(),
            Err(Error::Conflict(_))
        ));
        limiter.admit_output(4)?;
        assert!(matches!(limiter.admit_output(1), Err(Error::Conflict(_))));
        limiter.admit_execution_time(10)?;
        assert!(matches!(
            limiter.admit_execution_time(1),
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            limiter.usage(),
            SwarmUsage {
                model_steps: 1,
                output_bytes: 4,
                execution_time_ms: 10,
            }
        );
        Ok(())
    }

    #[test]
    fn provider_limiter_resume_preserves_consumed_capacity() -> Result<()> {
        let limits = SwarmResourceRequest {
            model_steps: 2,
            output_bytes: 8,
            execution_time_ms: 20,
        };
        let mut limiter = SwarmUsageLimiter::resume(
            limits,
            SwarmUsage {
                model_steps: 1,
                output_bytes: 8,
                execution_time_ms: 10,
            },
        )?;
        assert!(limiter.admit_output(1).is_err());
        limiter.admit_model_step()?;
        assert!(limiter.admit_model_step().is_err());
        limiter.admit_execution_time(10)?;
        assert_eq!(limiter.usage().model_steps, 2);
        Ok(())
    }

    #[test]
    fn root_provider_limiter_excludes_reserved_descendant_capacity() -> Result<()> {
        let budget = SwarmBudget::new(id(25), owner(0), limits())?;
        budget.reserve_child(request(26, None))?;
        let mut limiter = budget.root_usage_limiter()?;
        for _ in 0..6 {
            limiter.admit_model_step()?;
        }
        assert!(limiter.admit_model_step().is_err());
        Ok(())
    }

    #[test]
    fn reservation_cursor_requires_a_durable_sequence() -> Result<()> {
        let budget = SwarmBudget::new(id(23), owner(0), limits())?;
        let reservation = budget.reserve_child(request(24, None))?.reservation;
        assert_eq!(
            reservation.usage_cursor()?,
            SwarmUsageReceiptCursor::default()
        );
        assert!(SwarmUsageReceiptCursor::new(1, None).is_err());
        Ok(())
    }

    #[test]
    fn durable_receipt_must_match_activated_dispatch_identity() -> Result<()> {
        let budget = SwarmBudget::new(id(20), owner(0), limits())?;
        let child = budget.reserve_child(request(21, None))?.reservation;
        let dispatch_id = IdempotencyKey::new("dispatch-bound")?;
        budget.activate_with_dispatch(
            child.operation_id,
            owner(0),
            publication(child.operation_id, None),
            Some(dispatch_id),
        )?;
        let usage = SwarmUsage::default();
        let receipt = SwarmUsageReceipt::new(
            child.operation_id,
            IdempotencyKey::new("dispatch-forged")?,
            1,
            usage,
            "provider",
        )?;
        assert!(matches!(
            budget.apply_event(SwarmBudgetEvent::UsageReported {
                operation_id: child.operation_id,
                owner: owner(0),
                usage,
                receipt,
            }),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn duplicate_admission_is_replayed_without_double_counting() -> Result<()> {
        let budget = SwarmBudget::new(id(9), owner(0), limits())?;
        let first = budget.reserve_child(request(1, None))?;
        let second = budget.reserve_child(request(1, None))?;
        assert!(!first.replayed);
        assert!(second.replayed);
        assert_eq!(budget.usage()?.active_agents, 2);
        assert_eq!(budget.usage()?.total_agents, 2);
        Ok(())
    }

    #[test]
    fn publication_is_required_before_dispatch_and_depth_is_bounded() -> Result<()> {
        let budget = SwarmBudget::new(id(9), owner(0), limits())?;
        let child = budget.reserve_child(request(1, None))?.reservation;
        assert!(
            budget
                .report_usage(child.operation_id, &owner(0), SwarmUsage::default())
                .is_err()
        );
        let token = budget.activate(
            child.operation_id,
            owner(0),
            publication(child.operation_id, None),
        )?;
        assert_eq!(token.operation_id, child.operation_id);
        assert_eq!(token.resources(), child.resources);
        budget.report_usage(
            child.operation_id,
            &owner(0),
            SwarmUsage {
                model_steps: 1,
                output_bytes: 10,
                execution_time_ms: 100,
            },
        )?;
        assert!(
            budget
                .reserve_child(request(2, Some(child.operation_id)))
                .is_err()
        );
        let mut smaller = request(2, Some(child.operation_id));
        smaller.resources = SwarmResourceRequest {
            model_steps: 3,
            output_bytes: 30,
            execution_time_ms: 300,
        };
        let grandchild = budget.reserve_child(smaller)?.reservation;
        assert_eq!(grandchild.depth, 2);
        let mut sibling = request(4, Some(child.operation_id));
        sibling.resources = SwarmResourceRequest {
            model_steps: 1,
            output_bytes: 10,
            execution_time_ms: 100,
        };
        assert!(budget.reserve_child(sibling).is_err());
        assert!(
            budget
                .reserve_child(request(3, Some(grandchild.operation_id)))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn restart_replays_unknown_dispatch_as_reserved_capacity() -> Result<()> {
        let session = id(9);
        let owner = owner(0);
        let limits = limits();
        let budget = SwarmBudget::new(session, owner.clone(), limits)?;
        let reservation = budget.reserve_child(request(6, None))?.reservation;
        let recovered = SwarmBudget::replay(vec![
            SwarmBudgetEvent::Started {
                session_id: session,
                owner,
                limits,
                root_dispatch_id: None,
            },
            SwarmBudgetEvent::ChildReserved {
                reservation: reservation.clone(),
            },
        ])?;
        let usage = recovered.usage()?;
        assert_eq!(usage.active_agents, 2);
        assert_eq!(usage.total_agents, 2);
        assert_eq!(
            usage.reserved,
            SwarmUsage {
                model_steps: 4,
                output_bytes: 40,
                execution_time_ms: 400,
            }
        );
        assert_eq!(
            recovered
                .reservation(reservation.operation_id)?
                .map(|value| value.state),
            Some(SwarmReservationState::Reserved)
        );
        Ok(())
    }

    #[test]
    fn session_budget_enforces_each_resource_dimension_independently() -> Result<()> {
        for (byte, resources) in [
            (
                20,
                SwarmResourceRequest {
                    model_steps: 11,
                    output_bytes: 40,
                    execution_time_ms: 400,
                },
            ),
            (
                21,
                SwarmResourceRequest {
                    model_steps: 4,
                    output_bytes: 101,
                    execution_time_ms: 400,
                },
            ),
            (
                22,
                SwarmResourceRequest {
                    model_steps: 4,
                    output_bytes: 40,
                    execution_time_ms: 1_001,
                },
            ),
        ] {
            let budget = SwarmBudget::new(id(byte), owner(0), limits())?;
            let mut candidate = request(byte.saturating_add(1), None);
            candidate.resources = resources;
            assert!(budget.reserve_child(candidate).is_err());
        }
        Ok(())
    }

    #[test]
    fn parent_cancellation_requires_descendants_to_release_first() -> Result<()> {
        let budget = SwarmBudget::new(id(9), owner(0), limits())?;
        let parent = budget.reserve_child(request(1, None))?.reservation;
        let child = budget
            .reserve_child(request(2, Some(parent.operation_id)))?
            .reservation;
        assert!(budget.cancel(parent.operation_id, &owner(0)).is_err());
        budget.cancel(child.operation_id, &owner(0))?;
        budget.cancel(parent.operation_id, &owner(0))?;
        assert_eq!(budget.usage()?.active_agents, 1);
        Ok(())
    }

    #[test]
    fn sibling_reservations_share_the_parent_remaining_ceiling() -> Result<()> {
        let mut limits = limits();
        limits.max_model_steps = 32;
        limits.max_output_bytes = 320;
        limits.max_execution_time_ms = 3_200;
        let budget = SwarmBudget::new(id(9), owner(0), limits)?;
        let mut parent_request = request(1, None);
        parent_request.resources = SwarmResourceRequest {
            model_steps: 10,
            output_bytes: 100,
            execution_time_ms: 1_000,
        };
        let parent = budget.reserve_child(parent_request)?.reservation;
        let mut first_request = request(2, Some(parent.operation_id));
        first_request.resources = SwarmResourceRequest {
            model_steps: 6,
            output_bytes: 60,
            execution_time_ms: 600,
        };
        budget.reserve_child(first_request)?;
        let mut second_request = request(3, Some(parent.operation_id));
        second_request.resources = SwarmResourceRequest {
            model_steps: 6,
            output_bytes: 60,
            execution_time_ms: 600,
        };
        assert!(budget.reserve_child(second_request).is_err());
        Ok(())
    }

    #[test]
    fn completed_child_usage_remains_charged_to_parent_ceiling() -> Result<()> {
        let mut limits = limits();
        limits.max_model_steps = 32;
        limits.max_output_bytes = 320;
        limits.max_execution_time_ms = 3_200;
        let budget = SwarmBudget::new(id(9), owner(0), limits)?;
        let mut parent_request = request(1, None);
        parent_request.resources = SwarmResourceRequest {
            model_steps: 10,
            output_bytes: 100,
            execution_time_ms: 1_000,
        };
        let parent = budget.reserve_child(parent_request)?.reservation;
        let mut child_request = request(2, Some(parent.operation_id));
        child_request.resources = SwarmResourceRequest {
            model_steps: 6,
            output_bytes: 60,
            execution_time_ms: 600,
        };
        let child = budget.reserve_child(child_request)?.reservation;
        budget.activate(
            child.operation_id,
            owner(0),
            publication(child.operation_id, Some(parent.operation_id)),
        )?;
        budget.complete(
            child.operation_id,
            &owner(0),
            SwarmUsage {
                model_steps: 6,
                output_bytes: 60,
                execution_time_ms: 600,
            },
        )?;
        let mut sibling = request(3, Some(parent.operation_id));
        sibling.resources = SwarmResourceRequest {
            model_steps: 5,
            output_bytes: 50,
            execution_time_ms: 500,
        };
        assert!(budget.reserve_child(sibling).is_err());
        Ok(())
    }

    #[test]
    fn parent_completion_waits_for_live_descendants() -> Result<()> {
        let budget = SwarmBudget::new(id(9), owner(0), limits())?;
        let parent = budget.reserve_child(request(1, None))?.reservation;
        budget.activate(
            parent.operation_id,
            owner(0),
            publication(parent.operation_id, None),
        )?;
        let child = budget
            .reserve_child(request(2, Some(parent.operation_id)))?
            .reservation;
        budget.activate(
            child.operation_id,
            owner(0),
            publication(child.operation_id, Some(parent.operation_id)),
        )?;
        assert!(
            budget
                .complete(parent.operation_id, &owner(0), SwarmUsage::default())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn completed_grandchild_usage_remains_charged_to_all_ancestors() -> Result<()> {
        let mut limits = limits();
        limits.max_recursion_depth = 3;
        limits.max_model_steps = 40;
        limits.max_output_bytes = 400;
        limits.max_execution_time_ms = 4_000;
        let budget = SwarmBudget::new(id(9), owner(0), limits)?;
        let mut parent_request = request(1, None);
        parent_request.resources = SwarmResourceRequest {
            model_steps: 10,
            output_bytes: 100,
            execution_time_ms: 1_000,
        };
        let parent = budget.reserve_child(parent_request)?.reservation;
        let mut child_request = request(2, Some(parent.operation_id));
        child_request.resources = SwarmResourceRequest {
            model_steps: 10,
            output_bytes: 100,
            execution_time_ms: 1_000,
        };
        let child = budget.reserve_child(child_request)?.reservation;
        budget.activate(
            child.operation_id,
            owner(0),
            publication(child.operation_id, Some(parent.operation_id)),
        )?;
        let mut grandchild_request = request(3, Some(child.operation_id));
        grandchild_request.depth = 3;
        grandchild_request.resources = SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 40,
            execution_time_ms: 400,
        };
        let grandchild = budget.reserve_child(grandchild_request)?.reservation;
        budget.activate(
            grandchild.operation_id,
            owner(0),
            publication(grandchild.operation_id, Some(child.operation_id)),
        )?;
        budget.complete(
            grandchild.operation_id,
            &owner(0),
            SwarmUsage {
                model_steps: 4,
                output_bytes: 40,
                execution_time_ms: 400,
            },
        )?;
        budget.complete(child.operation_id, &owner(0), SwarmUsage::default())?;
        let mut sibling = request(4, Some(parent.operation_id));
        sibling.resources = SwarmResourceRequest {
            model_steps: 7,
            output_bytes: 70,
            execution_time_ms: 700,
        };
        assert!(budget.reserve_child(sibling).is_err());
        Ok(())
    }

    #[test]
    fn cancellation_releases_active_capacity_but_keeps_consumed_usage() -> Result<()> {
        let budget = SwarmBudget::new(id(9), owner(0), limits())?;
        let child = budget.reserve_child(request(1, None))?.reservation;
        budget.activate(
            child.operation_id,
            owner(0),
            publication(child.operation_id, None),
        )?;
        let root_owner = owner(0);
        budget.report_usage(
            child.operation_id,
            &root_owner,
            SwarmUsage {
                model_steps: 1,
                output_bytes: 10,
                execution_time_ms: 100,
            },
        )?;
        budget.cancel(child.operation_id, &owner(0))?;
        let usage = budget.usage()?;
        assert_eq!(usage.active_agents, 1);
        assert_eq!(usage.consumed.model_steps, 1);
        assert_eq!(usage.consumed.output_bytes, 10);
        assert_eq!(usage.reserved, SwarmUsage::default());
        Ok(())
    }

    #[test]
    fn root_usage_reduces_descendant_remaining_budget() -> Result<()> {
        let budget = SwarmBudget::new(
            id(9),
            owner(0),
            SwarmBudgetLimits {
                max_model_steps: 10,
                max_output_bytes: 100,
                max_execution_time_ms: 1_000,
                ..limits()
            },
        )?;
        budget.report_root_usage(
            &owner(0),
            SwarmUsage {
                model_steps: 7,
                output_bytes: 70,
                execution_time_ms: 700,
            },
        )?;
        assert!(budget.reserve_child(request(1, None)).is_err());
        assert!(
            budget
                .report_root_usage(&owner(0), SwarmUsage::default())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn takeover_rebinds_live_reservations_and_fences_old_owner() -> Result<()> {
        let budget = SwarmBudget::new(id(9), owner(0), limits())?;
        let child = budget.reserve_child(request(1, None))?.reservation;
        let current = budget.takeover(&owner(0), "restarted")?;
        assert_eq!(current.generation, 1);
        assert!(
            budget
                .activate(
                    child.operation_id,
                    owner(0),
                    publication(child.operation_id, None)
                )
                .is_err()
        );
        budget.activate(
            child.operation_id,
            current.clone(),
            publication(child.operation_id, None),
        )?;
        Ok(())
    }

    #[test]
    fn replay_rejects_takeover_generation_gaps_before_mutation() -> Result<()> {
        let events = [SwarmBudgetEvent::Started {
            session_id: id(9),
            owner: owner(0),
            limits: limits(),
            root_dispatch_id: None,
        }];
        let restored = SwarmBudget::replay(events)?;
        assert!(
            restored
                .apply_event(SwarmBudgetEvent::OwnerTakenOver {
                    owner: SwarmOwnerFence::new("restarted", 2)?,
                })
                .is_err()
        );
        assert_eq!(restored.descriptor()?.1, owner(0));
        Ok(())
    }

    #[test]
    fn durable_event_replay_preserves_reservation_without_dispatch() -> Result<()> {
        let budget = SwarmBudget::new(id(9), owner(0), limits())?;
        let receipt = budget.reserve_child(request(1, None))?;
        let started = SwarmBudgetEvent::Started {
            session_id: id(9),
            owner: owner(0),
            limits: limits(),
            root_dispatch_id: None,
        };
        let replayed = SwarmBudget::replay([started, receipt.durable_event()])?;
        assert_eq!(replayed.usage()?, budget.usage()?);
        let restored = replayed.reservation(id(1))?.expect("restored reservation");
        assert_eq!(restored.state, SwarmReservationState::Reserved);
        assert!(
            replayed
                .report_usage(id(1), &owner(0), SwarmUsage::default())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn concurrent_admissions_are_atomic_at_active_and_total_limits() -> Result<()> {
        let budget = Arc::new(SwarmBudget::new(
            id(9),
            owner(0),
            SwarmBudgetLimits {
                max_active_agents: 5,
                max_total_agents: 5,
                max_model_steps: 16,
                max_output_bytes: 160,
                max_execution_time_ms: 1_600,
                ..limits()
            },
        )?);
        let mut joins = Vec::new();
        for byte in 1..=16 {
            let budget = Arc::clone(&budget);
            joins.push(std::thread::spawn(move || {
                budget.reserve_child(request(byte, None))
            }));
        }
        let accepted = joins
            .into_iter()
            .filter_map(|join| join.join().expect("admission thread").ok())
            .count();
        assert_eq!(accepted, 4);
        let usage = budget.usage()?;
        assert_eq!(usage.active_agents, 5);
        assert_eq!(usage.total_agents, 5);
        Ok(())
    }
}
