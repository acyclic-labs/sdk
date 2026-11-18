//! Compiler-linked public method admission, including request-dependent targets.
use crate::Action;

/// Which record facts an operation can check at admission. Deferred facts must
/// be checked by the owning provider before it exposes each record or child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Check every grant, filter and pin against the exact request facts.
    Exact,
    /// Admit only `streams/follow`, deferring record filters and pins.
    Follow,
    /// Admit only `streams/list`, including discovery above a granted prefix.
    Children,
    /// Admit only `fs/list` on `fs:/`; each full metadata path still needs an
    /// exact check intersected with the current filesystem owner policy.
    ResourceFollow,
}

/// Admission declared by one public method. Product crates generate these rows
/// from their service contracts and use the same rows for session admission.
#[derive(Clone, Copy, Debug)]
pub struct MethodAuth<M: 'static> {
    /// Exhaustive product-owned public method identity.
    pub method: M,
    /// Canonical transport route, shared by client and serving adapters.
    pub route: &'static str,
    /// Actions required independently of request-dependent extraction.
    /// An empty slice does not authorize a method with dynamic rules.
    pub admitted: &'static [Action],
    /// Facts checked at admission versus deferred to record delivery.
    pub scope: Scope,
    /// Conjunctive resource selectors. Every selected target needs admission.
    pub rules: &'static [Rule],
}

impl<M: 'static> MethodAuth<M> {
    /// Enumerate the action ceiling of the method's rules for account-wide
    /// grant selection. Fixed actions take precedence over an extractor's
    /// vocabulary when the extractor only selects resources.
    ///
    /// An action may occur in multiple rules. Request admission still needs
    /// to extract and authorize each concrete action and resource.
    pub fn actions(&self) -> impl Iterator<Item = Action> + use<M> {
        self.rules
            .iter()
            .flat_map(|rule| rule.actions().iter().copied())
    }

    /// Project the same admission onto a public client method. A client that
    /// delegates to another operation retains that operation's original rules.
    #[must_use]
    pub const fn for_method<N: 'static>(&self, method: N, scope: Scope) -> MethodAuth<N> {
        MethodAuth {
            method,
            route: self.route,
            admitted: self.admitted,
            scope,
            rules: self.rules,
        }
    }
}

/// One resource selector and its exact static or request-dependent actions.
#[derive(Clone, Copy, Debug)]
pub struct Rule {
    /// Statically required actions on every resource selected by this rule.
    /// Empty only for a dynamic extractor, whose output carries each action.
    pub admitted: &'static [Action],
    /// The contract's original resource selection mechanism.
    pub resources: Resources,
}

/// Original descriptor resource selection, without a second route classifier.
#[derive(Clone, Copy, Debug)]
pub enum Resources {
    /// Expand every scalar or repeated substitution in the canonical template.
    Template(&'static str),
    /// The product's registered extractor resolves actual request targets.
    /// The vocabulary is an output ceiling, not an unconditional grant request.
    Extractor {
        /// Unique registered product extractor identity.
        name: &'static str,
        /// Every action its output may contain; no other action is admissible.
        possible_actions: &'static [Action],
    },
}

impl Rule {
    /// Fixed rule actions, or the registered vocabulary for dynamic rules.
    #[must_use]
    pub const fn actions(&self) -> &'static [Action] {
        match self.resources {
            Resources::Extractor {
                possible_actions, ..
            } if self.admitted.is_empty() => possible_actions,
            _ => self.admitted,
        }
    }
}
