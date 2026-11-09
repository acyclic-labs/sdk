//! Public client admission, preserving the operation each client actually sends.
use acyclic_auth::{
    action::{Action, FsAction, StreamsAction},
    method::{MethodAuth, Resources, Rule, Scope},
};

/// Compiler-linked unary service admission. Raw RPC Read and the public bounded
/// Session Follow are distinct operations with distinct admission.
pub mod rpc {
    use acyclic_auth::action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/authorization.rs"));
}

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

pub(crate) const FOLLOW_ACTION: Action = Action::Streams(StreamsAction::Follow);
pub(crate) const RESOURCE_FOLLOW_ACTION: Action = Action::Fs(FsAction::List);

// A Session observation stores one action in its retained authorization. A
// contract adding another action must change that representation at compile time.
pub(crate) const HEAD_ACTION: Action = observation_action(StreamMethod::Head);
pub(crate) const CHILDREN_ACTION: Action = observation_action(StreamMethod::ChildrenPage);

const fn observation_action(method: StreamMethod) -> Action {
    match method.authorization().admitted {
        [action] => *action,
        _ => panic!("Session observations require exactly one declared action"),
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
