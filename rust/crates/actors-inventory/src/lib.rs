//! Actors operation metadata shared by the clients and code generators.

/// Applies one consumer macro to the canonical native Actors operations.
#[macro_export]
macro_rules! native_operations {
    ($apply:ident) => { $apply! {
        ("DeployNamespace", deploy_namespace, 0, DeploymentRequest, DeploymentResponse),
        ("InvokeWorker", invoke_worker, 10, InvokeWorkerRequest, InvokeActorResponse),
        ("DeleteWorker", delete_worker, 11, DeleteWorkerRequest, DeleteWorkerResponse),
        ("DeployWorker", deploy_worker, 9, DeploymentRequest, DeploymentResponse),
        ("UpdateNamespace", update_namespace, 1, UpdateNamespaceRequest, UpdateNamespaceResponse),
        ("InspectActor", inspect_actor, 2, InspectActorRequest, InspectActorResponse),
        ("AddSubscription", add_subscription, 3, AddSubscriptionRequest, AddSubscriptionResponse),
        ("RemoveSubscription", remove_subscription, 4, RemoveSubscriptionRequest, RemoveSubscriptionResponse),
        ("ResumeSubscription", resume_subscription, 5, ResumeSubscriptionRequest, ResumeSubscriptionResponse),
        ("CheckpointActor", checkpoint_actor, 6, CheckpointActorRequest, CheckpointActorResponse),
        ("ForkActor", fork_actor, 12, ForkActorRequest, ForkActorResponse),
        ("InvokeActor", invoke_actor, 7, InvokeActorRequest, InvokeActorResponse),
        ("DeleteAll", delete_all, 8, DeleteAllRequest, DeleteAllResponse)
    } };
}

/// Applies one consumer macro to the canonical Wasm Actors operations.
#[macro_export]
macro_rules! wasm_operations {
    ($apply:ident) => {
        $apply! {
            (deploy_namespace, "DeployNamespace"),
            (deploy_worker, "DeployWorker"),
            (invoke_worker, "InvokeWorker"),
            (delete_worker, "DeleteWorker"),
            (update_namespace, "UpdateNamespace"),
            (inspect_actor, "InspectActor"),
            (add_subscription, "AddSubscription"),
            (remove_subscription, "RemoveSubscription"),
            (resume_subscription, "ResumeSubscription"),
            (checkpoint_actor, "CheckpointActor"),
            (fork_actor, "ForkActor"),
            (invoke_actor, "InvokeActor"),
            (delete_all, "DeleteAll"),
        }
    };
}

/// Rust-owned route names used by the TypeScript transport generator.
pub const HTTP_ROUTES: &[(&str, &str)] = &[
    ("deployNamespace", "v1/actors/namespaces/deploy"),
    ("updateNamespace", "v1/actors/namespaces/update"),
    ("inspectActor", "v1/actors/inspect"),
    ("addSubscription", "v1/actors/subscriptions/add"),
    ("removeSubscription", "v1/actors/subscriptions/remove"),
    ("resumeSubscription", "v1/actors/subscriptions/resume"),
    ("checkpointActor", "v1/actors/checkpoint"),
    ("invokeActor", "v1/actors/invoke"),
    ("deleteAll", "v1/actors/storage/delete-all"),
    ("deployWorker", "v1/workers/deploy"),
    ("invokeWorker", "v1/workers/invoke"),
    ("deleteWorker", "v1/workers/delete"),
    ("forkActor", "v1/actors/fork"),
];

macro_rules! native_inventory {
    ($(($name:literal, $method:ident, $index:literal, $input:ident, $output:ident)),+ $(,)?) => {
        /// Ordered RPC names emitted by the native gRPC and HTTP clients.
        pub const NATIVE_METHODS: &[&str] = &[$(stringify!($method)),+];
    };
}
native_operations!(native_inventory);

macro_rules! wasm_inventory {
    ($(($method:ident, $name:literal)),+ $(,)?) => {
        /// Ordered RPC names emitted by the Wasm client.
        pub const WASM_METHODS: &[&str] = &[$(stringify!($method)),+];
    };
}
wasm_operations!(wasm_inventory);
