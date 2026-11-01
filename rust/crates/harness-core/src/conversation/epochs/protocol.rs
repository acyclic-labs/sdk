//! Deterministic swap reducer. Storage serializes each accepted transition by
//! conditional commit; a refused transition has no effects to publish.

/// Identity of one summary attempt, shared by consumers of that preparation.
pub type SummaryId = [u8; 32];

/// Input headroom after the selected model's output reservation. The soft
/// watermark is harness policy, never an independently configurable limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Budget {
    hard: u64,
}

/// A verified request either keeps its existing epoch, prepares in the
/// background, or must resolve the pending producer before dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pressure {
    /// No preparation is needed yet.
    Below,
    /// Prepare once; continue with the unchanged request prefix.
    Soft,
    /// Resolve the exact pending preparation before admitting this request.
    Hard,
}

impl Budget {
    /// A reservation at or beyond the context capacity leaves no input budget.
    #[must_use]
    pub const fn new(capacity: u64, output: u64) -> Option<Self> {
        match capacity.checked_sub(output) {
            Some(hard) if hard > 0 => Some(Self { hard }),
            _ => None,
        }
    }

    /// Maximum admitted input, independently of retained history length.
    #[must_use]
    pub const fn hard(self) -> u64 {
        self.hard
    }

    /// Sixty percent, rounded up, without overflowing at maximum capacity.
    #[must_use]
    pub const fn soft(self) -> u64 {
        self.hard / 5 * 3 + (self.hard % 5 * 3).div_ceil(5)
    }

    /// Equality at the hard boundary resolves preparation before dispatch.
    #[must_use]
    pub const fn pressure(self, verified_input: u64) -> Pressure {
        if verified_input >= self.hard {
            Pressure::Hard
        } else if verified_input >= self.soft() {
            Pressure::Soft
        } else {
            Pressure::Below
        }
    }
}

/// Durable terminal outcome of the one summary producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Completion {
    /// The immutable summary body has committed.
    Ready,
    /// The producer ended without a summary, including loss of its custody.
    Failed,
}

/// Preparation against exactly one prior epoch. Appends may continue after the
/// cut, but another epoch swap makes this proposal stale.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pending {
    /// Shared producer/result identity.
    pub summary: SummaryId,
    /// Epoch generation fenced by the swap commit.
    pub generation: u64,
    /// Exclusive summary coverage end, fixed at preparation.
    pub through: u64,
    /// Absent while the summary producer still owns the operation.
    pub completion: Option<Completion>,
}

/// Constant-size state indexed with the agent's epoch pointer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct State {
    generation: u64,
    cursor: u64,
    pending: Option<Pending>,
}

/// Action at the hard budget. Waiting retains this exact summary's producer
/// observation; retirement requires reading its committed result, never launching
/// another producer. Physical retirement alone does not establish success.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Demand {
    /// No compaction was prepared at the soft watermark.
    Unprepared,
    /// Wait for the exact producer, then consume its committed terminal result.
    Await(SummaryId),
    /// The epoch can be swapped by conditional commit.
    Swap(SummaryId),
    /// The producer terminated; waiting again cannot make progress.
    Failed(SummaryId),
}

/// A refused protocol transition. No timeout or retry can repair these states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    /// The proposed range is empty, already covered or beyond the source head.
    Range,
    /// A different compaction already owns this epoch.
    Busy,
    /// The expected epoch or summary no longer matches.
    Stale,
    /// No durable successful summary exists yet.
    NotReady,
    /// Two terminal outcomes disagree for the same producer.
    Contradiction,
    /// The durable generation cannot be represented.
    Exhausted,
}

impl State {
    /// Restores the indexed epoch and its pending identity without reading old
    /// history. A crash must not turn an existing preparation into a new job.
    pub fn restore(
        generation: u64,
        cursor: u64,
        pending: Option<Pending>,
        head: u64,
    ) -> Result<Self, Refusal> {
        if cursor > head {
            return Err(Refusal::Range);
        }
        if let Some(prepared) = pending {
            if prepared.generation != generation {
                return Err(Refusal::Stale);
            }
            if prepared.through <= cursor || prepared.through > head {
                return Err(Refusal::Range);
            }
        }
        Ok(Self {
            generation,
            cursor,
            pending,
        })
    }

    /// Current conditional-commit generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Exclusive end of the summarized prefix.
    #[must_use]
    pub const fn cursor(self) -> u64 {
        self.cursor
    }

    /// Existing preparation, retained across a crash until its terminal swap.
    #[must_use]
    pub const fn pending(self) -> Option<Pending> {
        self.pending
    }

    /// Pins one source cut. Repeating the exact intent reuses it. A competing
    /// intent refuses rather than starting another summary.
    pub fn prepare(&mut self, summary: SummaryId, through: u64, head: u64) -> Result<(), Refusal> {
        if through <= self.cursor || through > head {
            return Err(Refusal::Range);
        }
        if let Some(pending) = self.pending {
            return if pending.summary == summary && pending.through == through {
                Ok(())
            } else {
                Err(Refusal::Busy)
            };
        }
        self.pending = Some(Pending {
            summary,
            generation: self.generation,
            through,
            completion: None,
        });
        Ok(())
    }

    /// Observes the producer's durable terminal event exactly once. Replaying
    /// the same result is harmless; an inconsistent result is never accepted.
    pub fn complete(&mut self, summary: SummaryId, completion: Completion) -> Result<(), Refusal> {
        let pending = self
            .pending
            .as_mut()
            .filter(|pending| pending.summary == summary)
            .ok_or(Refusal::Stale)?;
        if pending.completion.is_some_and(|prior| prior != completion) {
            return Err(Refusal::Contradiction);
        }
        pending.completion = Some(completion);
        Ok(())
    }

    /// Replaces only a durably failed attempt. A live or successful producer
    /// cannot be displaced by foreground demand. The caller conditionally
    /// publishes this transition before starting the replacement producer.
    pub fn retry_failed(
        &mut self,
        failed: SummaryId,
        replacement: SummaryId,
        through: u64,
        head: u64,
    ) -> Result<(), Refusal> {
        let pending = self.pending.ok_or(Refusal::Stale)?;
        if pending.summary != failed || replacement == failed {
            return Err(Refusal::Stale);
        }
        if pending.completion != Some(Completion::Failed) {
            return Err(Refusal::Busy);
        }
        if through < pending.through || through <= self.cursor || through > head {
            return Err(Refusal::Range);
        }
        self.pending = Some(Pending {
            summary: replacement,
            through,
            completion: None,
            ..pending
        });
        Ok(())
    }

    /// Resolves hard pressure without changing ownership or starting work.
    #[must_use]
    pub const fn demand(self) -> Demand {
        match self.pending {
            None => Demand::Unprepared,
            Some(Pending {
                summary,
                completion: None,
                ..
            }) => Demand::Await(summary),
            Some(Pending {
                summary,
                completion: Some(Completion::Ready),
                ..
            }) => Demand::Swap(summary),
            Some(Pending {
                summary,
                completion: Some(Completion::Failed),
                ..
            }) => Demand::Failed(summary),
        }
    }

    /// Commits a prepared cut against its original epoch generation. Items
    /// appended after preparation remain verbatim, even if the source grew.
    pub fn swap(&mut self, summary: SummaryId, expected: u64, head: u64) -> Result<(), Refusal> {
        let pending = self.pending.ok_or(Refusal::Stale)?;
        if expected != self.generation
            || pending.generation != expected
            || pending.summary != summary
        {
            return Err(Refusal::Stale);
        }
        if pending.completion != Some(Completion::Ready) {
            return Err(Refusal::NotReady);
        }
        if pending.through > head {
            return Err(Refusal::Range);
        }
        let generation = self.generation.checked_add(1).ok_or(Refusal::Exhausted)?;
        self.generation = generation;
        self.cursor = pending.through;
        self.pending = None;
        Ok(())
    }

    /// Fan-out follows the parent's completed swap. An unfinished or failed
    /// producer cannot be discarded or replicated into children. Forks inherit
    /// only a committed summary contained in their common source prefix.
    pub fn fork(self, common_prefix: u64) -> Result<Self, Refusal> {
        if self.cursor > common_prefix {
            return Err(Refusal::Range);
        }
        if self.pending.is_some() {
            return Err(Refusal::NotReady);
        }
        Ok(self)
    }
}
