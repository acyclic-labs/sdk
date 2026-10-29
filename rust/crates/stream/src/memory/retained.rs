/// One indefinitely retained snapshot entry.
struct Retained {
    fact: RetainedFactRef,
    replay_left: Option<usize>,
    replay_right: Option<usize>,
    replay_height: u8,
    commit_branch: Option<CommitBranch>,
}

/// The original immutable fact and its allocation custody. Derived indexes
/// remain in the retained inventory; a snapshot pins only this ownership chain.
struct RetainedFact {
    previous: Option<RetainedFactRef>,
    next: std::sync::OnceLock<RetainedLink>,
    replay: Option<Bytes>,
    digest: [u8; 32],
    /// The publication's revision index.
    index: u64,
    result: IdempotencyOutcome,
    envelope: Option<CommittedEnvelope>,
    _allocation: Allocation,
    _header: Allocation,
}

/// A successor retains its predecessor, so this link outlives every strong
/// successor owner. Drop the weak reference before releasing its original
/// allocation claim: a dead successor's Arc storage still belongs to the link.
struct RetainedLink {
    fact: retained_owner::RetainedFactWeak,
    _allocation: Allocation,
}

impl RetainedFact {
    fn reserved(header: Allocation) -> RetainedFactRef {
        RetainedFactRef::new(Self {
            previous: None,
            next: std::sync::OnceLock::new(),
            replay: None,
            digest: [0; 32],
            index: 0,
            result: IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: 0 }),
            envelope: None,
            _allocation: None,
            _header: header,
        })
    }

    fn envelope(&self) -> Option<&CommittedEnvelope> {
        match &self.result {
            IdempotencyOutcome::Commit(envelope) => Some(envelope),
            _ => self.envelope.as_ref(),
        }
    }
}

use retained_owner::RetainedFactRef;

// Keep every strong owner, including weak upgrades, on the same retirement
// path. into_inner retires the implicit weak reference before dropping fields;
// an incoming explicit weak link then frees storage before refunding its charge.
mod retained_owner {
    use super::RetainedFact;
    use std::sync::{Arc, Weak};

    #[derive(Clone)]
    pub(super) struct RetainedFactRef(Option<Arc<RetainedFact>>);

    pub(super) struct RetainedFactWeak {
        _fact: Weak<RetainedFact>,
    }

    impl RetainedFactRef {
        pub(super) fn new(fact: RetainedFact) -> Self {
            Self(Some(Arc::new(fact)))
        }

        pub(super) fn get_mut(&mut self) -> Option<&mut RetainedFact> {
            self.0.as_mut().and_then(Arc::get_mut)
        }

        pub(super) fn downgrade(&self) -> RetainedFactWeak {
            RetainedFactWeak {
                _fact: Arc::downgrade(self.arc()),
            }
        }

        fn arc(&self) -> &Arc<RetainedFact> {
            self.0
                .as_ref()
                .unwrap_or_else(|| unreachable!("live fact owner"))
        }
    }

    #[cfg(any(test, feature = "local"))]
    impl RetainedFactWeak {
        pub(super) fn upgrade(&self) -> Option<RetainedFactRef> {
            self._fact.upgrade().map(|fact| RetainedFactRef(Some(fact)))
        }
    }

    impl std::ops::Deref for RetainedFactRef {
        type Target = RetainedFact;

        fn deref(&self) -> &Self::Target {
            self.arc()
        }
    }

    impl Drop for RetainedFactRef {
        fn drop(&mut self) {
            let Some(first) = self.0.take() else {
                return;
            };
            let mut current = Self(Some(first));
            while let Some(fact) = current.0.take() {
                // Unlike try_unwrap, concurrent into_inner callers guarantee
                // one owns the value outside its Arc allocation. Keep the
                // predecessor (and incoming charge) until this value drops.
                let Some(mut fact) = Arc::into_inner(fact) else {
                    break;
                };
                current = fact.previous.take().unwrap_or_else(|| Self(None));
            }
        }
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    mod tests {
        include!("retained_retirement_tests.rs");
    }
}

impl std::ops::Deref for Retained {
    type Target = RetainedFact;

    fn deref(&self) -> &Self::Target {
        &self.fact
    }
}

impl Retained {
    fn envelope(&self) -> Option<&CommittedEnvelope> {
        self.fact.envelope()
    }
}
