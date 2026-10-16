//! Interest demand derived from followed layout/open-view files.

use acyclic_stream::StreamPath;
use std::collections::BTreeSet;

/// Fidelity and presentation for one agent subtree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InterestTier {
    /// Latest status and attention remain followed for the whole swarm.
    Overview,
    /// Every event is followed and rendered.
    InView,
    /// Every event is followed, ready for a future viewport change.
    Prefetch,
}

impl InterestTier {
    /// Overview status may conflate under pressure; content never does.
    #[must_use]
    pub const fn delivery(self) -> acyclic_stream::wire::SessionDelivery {
        match self {
            Self::Overview => acyclic_stream::wire::SessionDelivery::Latest,
            Self::InView | Self::Prefetch => acyclic_stream::wire::SessionDelivery::All,
        }
    }
}

/// Disposable demand derived from one layout cut. Tier zero is implicit for the
/// whole swarm: this stores only visible/prefetched agents, never 10k status rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientInterests {
    root: StreamPath,
    in_view: BTreeSet<StreamPath>,
    prefetch: BTreeSet<StreamPath>,
}

impl ClientInterests {
    /// Derives demand from an owner-authorized layout query, bounding unique
    /// selected agents before retaining them. Visible entries precede prefetch.
    /// This value is not a settings store; persist user actions through the fs.
    pub fn from_layout(
        root: StreamPath,
        in_view: impl IntoIterator<Item = StreamPath>,
        prefetch: impl IntoIterator<Item = StreamPath>,
        maximum_selected: usize,
    ) -> crate::Result<Self> {
        let mut value = Self {
            root,
            in_view: BTreeSet::new(),
            prefetch: BTreeSet::new(),
        };
        let visible = in_view.into_iter().map(|path| (InterestTier::InView, path));
        let nearby = prefetch
            .into_iter()
            .map(|path| (InterestTier::Prefetch, path));
        for (tier, path) in visible.chain(nearby) {
            if !value.contains(&path) {
                return Err(crate::Error::Invalid(
                    "interest lies outside the swarm root".into(),
                ));
            }
            if value.full(&path) {
                continue;
            }
            if value
                .in_view
                .len()
                .checked_add(value.prefetch.len())
                .is_none_or(|count| count >= maximum_selected)
            {
                return Err(crate::Error::Invalid("selected agent bound reached".into()));
            }
            match tier {
                InterestTier::InView => {
                    value.in_view.insert(path);
                }
                _ => {
                    value.prefetch.insert(path);
                }
            }
        }
        Ok(value)
    }

    /// Stable discovery root; viewport updates cannot unsubscribe tier zero.
    #[must_use]
    pub fn root(&self) -> &StreamPath {
        &self.root
    }

    /// Visits only agents that need rendering, without scanning the swarm overview.
    pub fn visible(&self) -> impl Iterator<Item = &StreamPath> {
        self.in_view.iter()
    }

    /// Agents whose event follows must remain lossless, including prefetch.
    pub fn full_fidelity(&self) -> impl Iterator<Item = &StreamPath> {
        self.in_view.union(&self.prefetch)
    }

    /// Transport changes contain only additions/removals of full-fidelity demand.
    /// Moving a prefetched agent into view needs no resubscribe or cursor reset.
    pub fn changes<'a>(&'a self, previous: &'a Self) -> crate::Result<InterestChanges<'a>> {
        if self.root != previous.root {
            return Err(crate::Error::Invalid("interest roots differ".into()));
        }
        Ok(InterestChanges {
            current: self,
            previous,
        })
    }

    fn full(&self, path: &StreamPath) -> bool {
        self.in_view.contains(path) || self.prefetch.contains(path)
    }

    /// Returns the tier for an agent inside this swarm.
    #[must_use]
    pub fn tier(&self, agent: &StreamPath) -> Option<InterestTier> {
        if !self.contains(agent) {
            return None;
        }
        Some(if self.in_view.contains(agent) {
            InterestTier::InView
        } else if self.prefetch.contains(agent) {
            InterestTier::Prefetch
        } else {
            InterestTier::Overview
        })
    }

    fn contains(&self, path: &StreamPath) -> bool {
        path == &self.root
            || path
                .as_str()
                .strip_prefix(self.root.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

/// Borrowed interest difference, without copies of paths or whole-swarm scans.
pub struct InterestChanges<'a> {
    current: &'a ClientInterests,
    previous: &'a ClientInterests,
}

impl InterestChanges<'_> {
    /// Agents that need a snapshot at C followed losslessly from C.
    pub fn subscribe(&self) -> impl Iterator<Item = &StreamPath> {
        self.current
            .full_fidelity()
            .filter(move |path| !self.previous.full(path))
    }

    /// Agents whose overview/attention remains followed while content is released.
    pub fn unsubscribe(&self) -> impl Iterator<Item = &StreamPath> {
        self.previous
            .full_fidelity()
            .filter(move |path| !self.current.full(path))
    }
}
