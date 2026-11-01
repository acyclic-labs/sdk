use acyclic_stream::StreamPath;
use std::collections::BTreeSet;

/// Optional caller-selected view capacity, independent of storage population.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DemandPolicy {
    selected: Option<usize>,
    expanded: Option<usize>,
}

impl DemandPolicy {
    /// Set optional combined visible/focused and expanded-node capacities.
    /// Absent capacities admit every explicit interest.
    ///
    /// # Errors
    /// Refuses explicitly selected zero capacities.
    pub fn new(selected: Option<usize>, expanded: Option<usize>) -> Result<Self, DemandError> {
        if selected == Some(0) || expanded == Some(0) {
            return Err(DemandError::InvalidPolicy);
        }
        Ok(Self { selected, expanded })
    }

    /// Maximum distinct agents with full record follows.
    #[must_use]
    pub fn selected_capacity(self) -> Option<usize> {
        self.selected
    }

    /// Maximum nodes with retained child-page windows.
    #[must_use]
    pub fn expanded_capacity(self) -> Option<usize> {
        self.expanded
    }
}

/// A rejected demand never replaces the caller's previously admitted view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DemandError {
    /// Policy selects a zero capacity.
    InvalidPolicy,
    /// Visible/focused rows or their union exceed the selected-agent cap.
    SelectedCapacity,
    /// Expanded rows exceed the child-page-window cap.
    ExpandedCapacity,
}

impl std::fmt::Display for DemandError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(match self {
            Self::InvalidPolicy => "invalid client demand policy",
            Self::SelectedCapacity => "selected agent capacity exceeded",
            Self::ExpandedCapacity => "expanded node capacity exceeded",
        })
    }
}

impl std::error::Error for DemandError {}

/// Only explicit view keys: full follows for visible/focused agents, and lazy
/// child pages for expanded nodes. There is no all-agent status membership map.
/// Streams remains the sole owner of physical follows, cursors and credit.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ViewDemand {
    full: BTreeSet<StreamPath>,
    expanded: BTreeSet<StreamPath>,
}

impl ViewDemand {
    /// Admit one complete replacement before the connection changes any follow.
    /// Overlapping visible/focused keys share one full follow. An explicitly
    /// finite capacity also bounds input consumption, including duplicate keys.
    ///
    /// # Errors
    /// Refuses excess input rows or excess distinct selected/expanded keys.
    pub fn new(
        policy: DemandPolicy,
        visible: impl IntoIterator<Item = StreamPath>,
        focused: impl IntoIterator<Item = StreamPath>,
        expanded: impl IntoIterator<Item = StreamPath>,
    ) -> Result<Self, DemandError> {
        let mut demand = Self::default();
        extend(
            &mut demand.full,
            visible,
            policy.selected,
            DemandError::SelectedCapacity,
        )?;
        extend(
            &mut demand.full,
            focused,
            policy.selected,
            DemandError::SelectedCapacity,
        )?;
        extend(
            &mut demand.expanded,
            expanded,
            policy.expanded,
            DemandError::ExpandedCapacity,
        )?;
        Ok(demand)
    }

    /// Borrow full-follow demand without enumerating undisplayed agents.
    pub fn full(&self) -> impl Iterator<Item = &StreamPath> {
        self.full.iter()
    }

    /// Test explicit full-follow demand without walking the selected set.
    #[must_use]
    pub fn contains_full(&self, agent: &StreamPath) -> bool {
        self.full.contains(agent)
    }

    /// Borrow only nodes whose child pages are needed by the current tree view.
    pub fn expanded(&self) -> impl Iterator<Item = &StreamPath> {
        self.expanded.iter()
    }

    /// Full follows to close before opening newly selected agents.
    pub fn removed_full<'a>(&'a self, previous: &'a Self) -> impl Iterator<Item = &'a StreamPath> {
        previous.full.difference(&self.full)
    }

    /// New full follows; overlapping visible/focused rows are already deduplicated.
    pub fn added_full<'a>(&'a self, previous: &'a Self) -> impl Iterator<Item = &'a StreamPath> {
        self.full.difference(&previous.full)
    }

    /// Child-page windows to release when tree nodes collapse.
    pub fn collapsed<'a>(&'a self, previous: &'a Self) -> impl Iterator<Item = &'a StreamPath> {
        previous.expanded.difference(&self.expanded)
    }

    /// Nodes needing lazy child discovery, without following every child.
    pub fn newly_expanded<'a>(
        &'a self,
        previous: &'a Self,
    ) -> impl Iterator<Item = &'a StreamPath> {
        self.expanded.difference(&previous.expanded)
    }
}

fn extend(
    target: &mut BTreeSet<StreamPath>,
    input: impl IntoIterator<Item = StreamPath>,
    limit: Option<usize>,
    error: DemandError,
) -> Result<(), DemandError> {
    for (index, key) in input.into_iter().enumerate() {
        if limit.is_some_and(|limit| {
            index == limit || (target.len() == limit && !target.contains(&key))
        }) {
            return Err(error);
        }
        target.insert(key);
    }
    Ok(())
}
