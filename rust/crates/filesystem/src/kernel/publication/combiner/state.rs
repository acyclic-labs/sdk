//! The one enqueue/prepare/seal state machine. Synchronization and storage are
//! supplied by the host; no callback runs while this state is borrowed twice.
use super::GroupAbort;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Collecting,
    Admitting,
    Preparing,
    Sealing,
    Appending,
    Complete,
    Aborted,
}

pub(super) struct Member<T, R> {
    pub(super) value: T,
    selected: bool,
    predecessor: Option<usize>,
    root: Option<R>,
    ready: bool,
    withdrawn: bool,
}

pub(super) struct State<T, R> {
    pub(super) members: Vec<Member<T, R>>,
    pub(super) phase: Phase,
    pub(super) abort_cause: GroupAbort,
    preparation_owned: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Plan<R> {
    Waiting,
    Prefix(Option<R>),
    Deferred,
    Aborted,
}

impl<T, R: Copy> State<T, R> {
    pub(super) fn new(members: Vec<Member<T, R>>) -> Self {
        assert!(members.is_empty());
        Self {
            members,
            phase: Phase::Collecting,
            abort_cause: GroupAbort::PhaseEnded,
            preparation_owned: false,
        }
    }

    /// Only the first funded participant can install the prefix continuation.
    pub(super) fn own_preparation(&mut self) -> bool {
        if self.preparation_owned || !matches!(self.phase, Phase::Collecting | Phase::Admitting) {
            return false;
        }
        self.preparation_owned = true;
        true
    }

    pub(super) fn enqueue(&mut self, value: T) -> Result<usize, T> {
        if self.phase != Phase::Collecting || self.members.len() == self.members.capacity() {
            return Err(value);
        }
        let index = self.members.len();
        self.members.push(Member {
            value,
            selected: false,
            predecessor: None,
            root: None,
            ready: false,
            withdrawn: false,
        });
        Ok(index)
    }

    /// Close the bounded host-turn collection window. Members may still be
    /// waiting for the predecessor's durability receipt, without resampling it.
    pub(super) fn close(&mut self) {
        if self.phase == Phase::Collecting {
            self.phase = Phase::Admitting;
        }
    }

    pub(super) fn can_collect(&self, mut admitted: impl FnMut(&T) -> bool) -> bool {
        self.phase == Phase::Admitting
            && self
                .members
                .iter()
                .all(|member| member.withdrawn || admitted(&member.value))
    }

    pub(super) fn collect<E>(
        &mut self,
        mut independent: impl FnMut(&T, &T) -> Result<bool, E>,
        mut admit: impl FnMut(&T) -> Result<bool, E>,
    ) -> Result<(), E> {
        if !matches!(self.phase, Phase::Collecting | Phase::Admitting) {
            return Ok(());
        }
        let mut predecessor = None;
        for index in 0..self.members.len() {
            let Some((prior_members, remaining)) = self.members.split_at_mut_checked(index) else {
                self.phase = Phase::Aborted;
                return Ok(());
            };
            let Some(member) = remaining.first_mut() else {
                self.phase = Phase::Aborted;
                return Ok(());
            };
            member.selected = false;
            member.predecessor = None;
            if member.withdrawn {
                continue;
            }
            let mut selected = true;
            for prior in prior_members {
                if prior.selected && !independent(&member.value, &prior.value)? {
                    selected = false;
                    break;
                }
            }
            if selected {
                selected = admit(&member.value)?;
            }
            member.selected = selected;
            if selected {
                member.predecessor = predecessor;
                predecessor = Some(index);
            }
        }
        self.phase = if self.members.iter().any(|member| member.selected) {
            Phase::Preparing
        } else {
            Phase::Complete
        };
        Ok(())
    }

    pub(super) fn plan(&self, index: usize) -> Plan<R> {
        if self.phase == Phase::Aborted {
            return Plan::Aborted;
        }
        if matches!(self.phase, Phase::Collecting | Phase::Admitting) {
            return Plan::Waiting;
        }
        let Some(member) = self.members.get(index) else {
            return Plan::Aborted;
        };
        if !member.selected {
            return if self.phase == Phase::Complete {
                Plan::Deferred
            } else {
                Plan::Waiting
            };
        }
        match member.predecessor {
            None => Plan::Prefix(None),
            Some(prior) => self
                .members
                .get(prior)
                .and_then(|member| member.root)
                .map_or(Plan::Waiting, |root| Plan::Prefix(Some(root))),
        }
    }

    pub(super) fn planned(&mut self, index: usize, root: R) -> bool {
        if self.phase != Phase::Preparing {
            return false;
        }
        if !matches!(self.plan(index), Plan::Prefix(_)) {
            return false;
        }
        let Some(member) = self.members.get_mut(index) else {
            return false;
        };
        if !member.selected || member.root.is_some() {
            return false;
        }
        member.root = Some(root);
        true
    }

    /// Exactly the final prepared member receives the sealing capability.
    pub(super) fn ready(&mut self, index: usize) -> bool {
        if self.phase != Phase::Preparing {
            return false;
        }
        let Some(member) = self.members.get_mut(index) else {
            return false;
        };
        if !member.selected || member.root.is_none() || member.ready {
            return false;
        }
        member.ready = true;
        if self
            .members
            .iter()
            .all(|member| !member.selected || member.ready)
        {
            self.phase = Phase::Sealing;
            return true;
        }
        false
    }

    pub(super) fn handoff(&mut self) -> bool {
        if self.phase != Phase::Sealing {
            return false;
        }
        self.phase = Phase::Appending;
        true
    }

    pub(super) fn complete(&mut self) -> bool {
        if self.phase != Phase::Appending {
            return false;
        }
        self.phase = Phase::Complete;
        true
    }

    pub(super) fn withdraw(&mut self, index: Option<usize>) {
        let Some(index) = index else {
            return;
        };
        // A pending seal must observe caller loss. A completed seal no longer
        // reads this marker, so the owned durable append remains uncancellable.
        if let Some(member) = self.members.get_mut(index) {
            member.withdrawn = true;
        }
        if matches!(self.phase, Phase::Collecting | Phase::Admitting) {
            // The collection-window owner cannot leave other admitted
            // proposals waiting for a host-turn future that no longer exists.
            if index == 0 {
                self.close();
            }
        } else if self.phase != Phase::Appending {
            self.abort_with(GroupAbort::MemberWithdrawn);
        }
    }

    pub(super) fn abort(&mut self) {
        self.abort_with(GroupAbort::PhaseEnded);
    }

    pub(super) fn abort_with(&mut self, cause: GroupAbort) {
        if self.phase != Phase::Complete {
            if self.phase != Phase::Aborted {
                self.abort_cause = cause;
            }
            self.phase = Phase::Aborted;
        }
    }

    pub(super) fn selected(&self) -> impl Iterator<Item = &T> {
        self.members
            .iter()
            .filter(|member| member.selected)
            .map(|member| &member.value)
    }
    pub(super) fn selected_at(&self, index: usize) -> Option<(&T, bool)> {
        self.members
            .get(index)
            .filter(|member| member.selected)
            .map(|member| (&member.value, member.withdrawn))
    }
    pub(super) fn tail(&self) -> Option<R> {
        self.members
            .iter()
            .rev()
            .find(|member| member.selected)
            .and_then(|member| member.root)
    }
}
