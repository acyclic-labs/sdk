//! Token grants select declared method admission, never a second action list.
use crate::{StreamMethod, authorization::rpc::StreamRpc};
use acyclic_auth::{
    action::{Action, StreamsAction},
    method::Resources,
};

// These narrow grants select an actual operation of a request-dependent method.
// A registry change removing that operation fails constant evaluation.
const CREATE: &[Action] = &[selected(StreamMethod::Fork, StreamsAction::Create)];
const FORK: &[Action] = &[selected(StreamMethod::Fork, StreamsAction::Fork)];
const COMMIT: &[Action] = &[selected(StreamMethod::Commit, StreamsAction::Commit)];

const fn selected(method: StreamMethod, wanted: StreamsAction) -> Action {
    assert!(
        declares(method, wanted),
        "token operation is absent from its declared method admission"
    );
    Action::Streams(wanted)
}

const fn declares(method: StreamMethod, wanted: StreamsAction) -> bool {
    let row = method.authorization();
    let mut rules = row.rules;
    while let [rule_row, rest @ ..] = rules {
        let mut actions = match rule_row.resources {
            Resources::Template(_) => rule_row.admitted,
            Resources::Extractor {
                possible_actions, ..
            } => possible_actions,
        };
        while let [action, remaining @ ..] = actions {
            if let Action::Streams(action) = action
                && *action as u8 == wanted as u8
            {
                return true;
            }
            actions = remaining;
        }
        rules = rest;
    }
    false
}

macro_rules! operations {
    ($($(#[$doc:meta])* $operation:ident => $name:literal, $first:expr, $second:expr;)*) => {
        /// Hosted access-token operations and their SDK-owned admission.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum TokenOperation { $($(#[$doc])* $operation,)* }

        impl TokenOperation {
            /// Every accepted token operation.
            pub const ALL: &'static [Self] = &[$(Self::$operation),*];

            /// Canonical token request spelling.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$operation => $name),* }
            }

            /// Effective capability vocabulary derived from method admission.
            /// No allocation or catalog scan occurs when selecting a grant.
            /// Commit grants its transaction gate; mutation capabilities must
            /// be selected separately. Dynamic extractor ceilings are not grants.
            pub fn actions(self) -> impl Iterator<Item = Action> {
                let (first, second): (&'static [Action], &'static [Action]) = match self {
                    $(Self::$operation => ($first, $second)),*
                };
                first.iter().chain(second).copied()
            }
        }

        impl core::str::FromStr for TokenOperation {
            type Err = crate::StreamError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($name => Ok(Self::$operation),)*
                    _ => Err(crate::StreamError::InvalidArgument),
                }
            }
        }
    };
}

operations! {
    /// Paged child discovery.
    List => "list", StreamMethod::ChildrenPage.authorization().admitted, &[];
    /// Record reads and tail observation, without live Follow authority.
    Read => "read", StreamRpc::Read.authorization().admitted, StreamMethod::Tail.authorization().admitted;
    /// Live Session Follow together with tail observation.
    Follow => "follow", StreamMethod::Follow.authorization().admitted, StreamMethod::Tail.authorization().admitted;
    /// Record publication.
    Append => "append", StreamMethod::Append.authorization().admitted, &[];
    /// Fork selection, without implicitly granting destination creation.
    Fork => "fork", FORK, &[];
    /// Destination creation, without authority to fork another source.
    Create => "create", CREATE, &[];
    /// The transaction gate, excluding its request-dependent mutations.
    Commit => "commit", COMMIT, &[];
}
