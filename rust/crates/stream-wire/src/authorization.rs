//! Public client admission, preserving the operation each client actually sends.
use acyclic_auth_action::{
    Action, FsAction, StreamsAction,
    method::{MethodAuth, Resources, Rule, Scope},
};

/// Compiler-linked service admission. Raw RPC Read and the public bounded
/// Session Follow are distinct operations with distinct admission.
use crate::rpc;

const FOLLOW: MethodAuth<()> = MethodAuth {
    method: (),
    route: "/acyclic.stream.v1.StreamSessionService/Session",
    admitted: &[FOLLOW_ACTION],
    scope: Scope::Follow,
    rules: &[Rule {
        admitted: &[FOLLOW_ACTION],
        resources: Resources::Template("streams:/{path}"),
    }],
};

/// Route used by hosts that open a Follow with a single request envelope.
pub const HOSTED_FOLLOW_ROUTE: &str = "/acyclic.stream.v1.StreamService/Follow";

/// The hosted transport uses the same Follow admission and delivery scope.
pub const HOSTED_FOLLOW: MethodAuth<StreamMethod> = MethodAuth {
    route: HOSTED_FOLLOW_ROUTE,
    ..FOLLOW.for_method(StreamMethod::Follow, Scope::Follow)
};

/// Action for bounded record follows.
pub const FOLLOW_ACTION: Action = Action::Streams(StreamsAction::Follow);
/// Action for indexed filesystem resource observations.
pub const RESOURCE_FOLLOW_ACTION: Action = Action::Fs(FsAction::List);

// A Session observation stores one action in its retained authorization. A
// contract adding another action must change that representation at compile time.
/// Action retained by a head observation.
pub const HEAD_ACTION: Action = observation_action(StreamMethod::Head);
/// Action retained by a tail observation.
pub const TAIL_ACTION: Action = observation_action(StreamMethod::Tail);
/// Action retained by a children-page observation.
pub const CHILDREN_ACTION: Action = observation_action(StreamMethod::ChildrenPage);

const fn observation_action(method: StreamMethod) -> Action {
    let action = method.authorization().single_action();
    assert!(
        action.is_some(),
        "Session observations require exactly one declared action"
    );
    match action {
        Some(action) => action,
        None => unreachable!(),
    }
}

const RESOURCE_FOLLOW: MethodAuth<()> = MethodAuth {
    method: (),
    route: "/acyclic.stream.v1.StreamSessionService/Session",
    admitted: &[RESOURCE_FOLLOW_ACTION],
    scope: Scope::ResourceFollow,
    rules: &[Rule {
        admitted: &[RESOURCE_FOLLOW_ACTION],
        resources: Resources::Template("{resource}"),
    }],
};

macro_rules! methods {
    ($($(#[$docs:meta])* $method:ident => $admission:expr, $scope:ident;)*) => {
        /// Exhaustive public client operations and their original admission.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum StreamMethod { $($(#[$docs])* $method,)* }

        /// Canonical admission for every public client operation.
        pub const AUTHORIZATION: &[MethodAuth<StreamMethod>] = &[
            $(StreamMethod::$method.authorization()),*
        ];

        impl StreamMethod {
            /// Every declared public client operation.
            pub const ALL: &'static [Self] = &[$(Self::$method),*];

            /// Admission of the operation actually sent by this method.
            #[must_use]
            pub const fn authorization(self) -> MethodAuth<Self> {
                match self {
                    $(Self::$method => ($admission).for_method(Self::$method, Scope::$scope)),*
                }
            }
        }
    };
}

methods! {
    /// Read a bounded range through Session Follow.
    Read => FOLLOW, Follow;
    /// Subscribe to future records.
    Follow => FOLLOW, Follow;
    /// Seek through the same bounded Session Follow.
    Seek => FOLLOW, Follow;
    /// Replay records through the same bounded Session Follow.
    Replay => FOLLOW, Follow;
    /// Observe indexed filesystem metadata, with exact checks on delivery.
    FollowResource => RESOURCE_FOLLOW, ResourceFollow;
    /// Read one path head.
    Head => rpc::StreamRpc::Head.authorization(), Exact;
    /// Capture a coherent, request-dependent read and discovery view.
    Heads => rpc::StreamRpc::Heads.authorization(), Exact;
    /// Read the accepted or durable tail.
    Tail => rpc::StreamRpc::Tail.authorization(), Exact;
    /// Read accepted visibility and its durable prefix at one snapshot.
    Watermarks => rpc::StreamRpc::Head.authorization(), Exact;
    /// Confirm a retained prefix through the same head observation.
    DurableReceipt => rpc::StreamRpc::Head.authorization(), Exact;
    /// Append records.
    Append => rpc::StreamRpc::Append.authorization(), Exact;
    /// Append at a conditional tail.
    AppendAt => rpc::StreamRpc::Append.authorization(), Exact;
    /// Append a batch of records.
    AppendBatch => rpc::StreamRpc::Append.authorization(), Exact;
    /// Fork one stream into a destination.
    Fork => rpc::StreamRpc::Fork.authorization(), Exact;
    /// Discover one bounded page of children.
    ChildrenPage => rpc::StreamRpc::ChildrenPage.authorization(), Children;
    /// Discover children through the same paged operation.
    Children => rpc::StreamRpc::ChildrenPage.authorization(), Children;
    /// Publish request-dependent transactional mutations.
    Commit => rpc::StreamRpc::Commit.authorization(), Exact;
    /// Submit a publication through the same transactional operation.
    Submit => rpc::StreamRpc::Commit.authorization(), Exact;
    /// Publish a transaction and retain its accepted envelope.
    CommitAccepted => rpc::StreamRpc::Commit.authorization(), Exact;
    /// Read a committed envelope's selected resources.
    ReadCommit => rpc::StreamRpc::ReadCommit.authorization(), Exact;
    /// Inspect a retained request's selected resources.
    InspectIdempotency => rpc::StreamRpc::InspectIdempotency.authorization(), Exact;
}

#[cfg(test)]
mod rpc_admission_tests {
    use super::{Action, StreamsAction, rpc::StreamRpc};

    #[test]
    fn every_stream_rpc_resolves_to_its_declared_actions() {
        use StreamsAction::{
            Append, Commit, Create, Fork, InspectIdempotency, List, Read, ReadCommit, Tail,
        };

        for &method in StreamRpc::ALL {
            let row = method.authorization();
            let expected: &[StreamsAction] = match method {
                StreamRpc::InspectIdempotency => &[InspectIdempotency],
                StreamRpc::Append => &[Append],
                StreamRpc::Head | StreamRpc::Read => &[Read],
                StreamRpc::Heads => &[Read, List, Tail],
                StreamRpc::Tail => &[Tail],
                StreamRpc::Fork => &[Fork, Create],
                StreamRpc::ChildrenPage => &[List],
                StreamRpc::Commit => &[Read, Commit, Append, Fork, Create],
                StreamRpc::ReadCommit => &[ReadCommit],
            };
            assert_eq!(StreamRpc::from_route(row.route), Some(method));
            assert!(
                row.actions()
                    .eq(expected.iter().copied().map(Action::Streams)),
                "{method:?}: {:?}",
                row.actions().collect::<Vec<_>>()
            );
        }
        assert_eq!(
            StreamRpc::from_route("/acyclic.stream.v1.StreamService/Unmapped"),
            None
        );
    }

    #[test]
    fn each_generated_rpc_has_one_hosted_http_route() {
        for &method in StreamRpc::ALL {
            assert_eq!(
                crate::HTTP_RESPONSE_CONTRACT
                    .iter()
                    .filter(|(path, _)| crate::hosted_http_rpc(path) == Some((*path, method)))
                    .count(),
                1,
                "{method:?}"
            );
        }
        assert_eq!(crate::hosted_http_rpc("tokens/create"), None);
        assert_eq!(crate::hosted_http_rpc("unmapped"), None);
    }
}
