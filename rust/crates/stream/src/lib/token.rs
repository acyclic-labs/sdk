//! Token grants select declared method admission, never a second action list.
use crate::StreamMethod;
use acyclic_auth::{
    action::{Action, StreamsAction},
    method::Resources,
};

// These narrow grants select an actual operation of a request-dependent method.
// A registry change removing that operation fails constant evaluation.
const CREATE: &[Action] = &[selected(StreamMethod::Fork, StreamsAction::Create)];
const COMMIT: &[Action] = &[selected(StreamMethod::Commit, StreamsAction::Commit)];

const fn selected(method: StreamMethod, wanted: StreamsAction) -> Action {
    let row = method.authorization();
    let mut rule = 0;
    while rule < row.rules.len() {
        let actions = match row.rules[rule].resources {
            Resources::Template(_) => row.rules[rule].admitted,
            Resources::Extractor {
                possible_actions, ..
            } => possible_actions,
        };
        let mut index = 0;
        while index < actions.len() {
            if let Action::Streams(action) = actions[index]
                && action as u8 == wanted as u8
            {
                return actions[index];
            }
            index += 1;
        }
        rule += 1;
    }
    panic!("token operation is absent from its declared method admission")
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
    /// Bounded Session Follow together with tail observation.
    Read => "read", StreamMethod::Read.authorization().admitted, StreamMethod::Tail.authorization().admitted;
    /// Live Session Follow together with tail observation.
    Follow => "follow", StreamMethod::Follow.authorization().admitted, StreamMethod::Tail.authorization().admitted;
    /// Record publication.
    Append => "append", StreamMethod::Append.authorization().admitted, &[];
    /// Source and destination admission of a stream fork.
    Fork => "fork", StreamMethod::Fork.authorization().admitted, &[];
    /// Destination creation, without authority to fork another source.
    Create => "create", CREATE, &[];
    /// The transaction gate, excluding its request-dependent mutations.
    Commit => "commit", COMMIT, &[];
}
