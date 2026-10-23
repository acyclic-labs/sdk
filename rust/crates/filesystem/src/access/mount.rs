use super::{Action, PathTrie, PinnedPath, PreparedPath};

/// Metadata projection of the same decisions used to authorize mount callbacks.
/// Operations still call `decide` to check expiry and attribute the rule at the
/// actual operation boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MountMode {
    actions: u16,
}

impl MountMode {
    /// Whether this action is permitted at the exact path and pinned view.
    #[must_use]
    pub const fn allows(self, action: Action) -> bool {
        self.actions & (1 << action as u16) != 0
    }

    /// Whether a directory listing may disclose this entry's name or metadata.
    #[must_use]
    pub const fn visible(self) -> bool {
        self.allows(Action::List)
    }

    /// Whether every mutation of entries and bytes at this path is denied.
    #[must_use]
    pub const fn read_only(self) -> bool {
        !self.allows(Action::Create)
            && !self.allows(Action::Write)
            && !self.allows(Action::Append)
            && !self.allows(Action::Delete)
            && !self.allows(Action::Rename)
    }

    /// Whether new entries are allowed without read or other mutation authority.
    #[must_use]
    pub const fn create_only(self) -> bool {
        self.allows(Action::Create)
            && !self.allows(Action::Read)
            && !self.allows(Action::Write)
            && !self.allows(Action::Append)
            && !self.allows(Action::Delete)
            && !self.allows(Action::Rename)
    }
}

impl PathTrie {
    /// Conservative permission union for a mount and its descendants. This only
    /// selects physical mount restrictions; callbacks must check the exact path
    /// with `decide`. The bounded compiled graph is traversed once, without
    /// opening files. Invalid native names may widen this upper bound.
    #[must_use]
    pub fn subtree_mode(&self, path: &str, now: u64) -> MountMode {
        MountMode {
            actions: self.subtree_action_mask(path, now),
        }
    }

    /// Derive native mount metadata, including preparation of the raw path.
    /// A stale, expired or malformed view projects no permissions.
    #[must_use]
    pub fn mount_mode(&self, path: &str, now: u64) -> MountMode {
        self.prepare(path)
            .map_or_else(|_| MountMode::default(), |path| path.mount_mode(now))
    }
}

impl PreparedPath<'_> {
    /// Constant-time, allocation-free native mode from the same pinned decisions.
    #[must_use]
    pub fn mount_mode(self, now: u64) -> MountMode {
        MountMode {
            actions: self.action_mask(now),
        }
    }
}

impl PinnedPath {
    /// Project the pinned handle's decisions without refreshing its policy.
    #[must_use]
    pub fn mount_mode(&self, now: u64) -> MountMode {
        MountMode {
            actions: self.action_mask(now),
        }
    }
}
