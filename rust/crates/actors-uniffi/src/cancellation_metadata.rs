//! Rust-owned metadata and template for the external Kotlin cancellation adapter.
//!
//! UniFFI owns the generated FFI/Kotlin source. This module owns only the
//! package adapter metadata and renderer; the rendered adapter is deliberately
//! emitted outside the generated binding tree.

use std::fmt::Write as _;

#[derive(Clone, Copy)]
pub struct KotlinParameter {
    pub name: &'static str,
    pub ty: &'static str,
}

#[derive(Clone, Copy)]
pub struct KotlinClientMethod {
    pub wrapper_name: &'static str,
    pub generated_name: &'static str,
    pub argument: KotlinParameter,
    pub return_type: &'static str,
}

#[derive(Clone, Copy)]
pub struct KotlinTopLevelMethod {
    pub wrapper_name: &'static str,
    pub generated_alias: &'static str,
    pub parameters: &'static [KotlinParameter],
    pub return_type: &'static str,
}

const ENDPOINT_TOKEN: &[KotlinParameter] = &[
    KotlinParameter { name: "endpoint", ty: "String" },
    KotlinParameter { name: "token", ty: "String" },
];

const ENDPOINT_TOKEN_CA: &[KotlinParameter] = &[
    KotlinParameter { name: "endpoint", ty: "String" },
    KotlinParameter { name: "token", ty: "String" },
    KotlinParameter { name: "caCertificate", ty: "ByteArray?" },
];

/// Async Rust exports whose generated Kotlin method carries an optional
/// CancellationHandle and therefore receives a no-handle package overload.
pub const KOTLIN_CLIENT_METHODS: &[KotlinClientMethod] = &[
    KotlinClientMethod { wrapper_name: "addSubscription", generated_name: "addSubscription", argument: KotlinParameter { name: "request", ty: "AddSubscriptionRequest" }, return_type: "ActorObservation?" },
    KotlinClientMethod { wrapper_name: "checkpointActor", generated_name: "checkpointActor", argument: KotlinParameter { name: "request", ty: "CheckpointActorRequest" }, return_type: "ActorObservation?" },
    KotlinClientMethod { wrapper_name: "createActor", generated_name: "createActor", argument: KotlinParameter { name: "request", ty: "CreateActorRequest" }, return_type: "ActorObservation?" },
    KotlinClientMethod { wrapper_name: "inspectActor", generated_name: "inspectActor", argument: KotlinParameter { name: "actorId", ty: "ActorId" }, return_type: "ActorObservation?" },
    KotlinClientMethod { wrapper_name: "inspectActor", generated_name: "inspectActorRequest", argument: KotlinParameter { name: "request", ty: "InspectActorRequest" }, return_type: "ActorObservation?" },
    KotlinClientMethod { wrapper_name: "invokeActor", generated_name: "invokeActor", argument: KotlinParameter { name: "request", ty: "InvokeActorRequest" }, return_type: "InvokeActorResponse" },
    KotlinClientMethod { wrapper_name: "removeSubscription", generated_name: "removeSubscription", argument: KotlinParameter { name: "request", ty: "RemoveSubscriptionRequest" }, return_type: "ActorObservation?" },
    KotlinClientMethod { wrapper_name: "resumeSubscription", generated_name: "resumeSubscription", argument: KotlinParameter { name: "request", ty: "ResumeSubscriptionRequest" }, return_type: "ActorObservation?" },
    KotlinClientMethod { wrapper_name: "updateActor", generated_name: "updateActor", argument: KotlinParameter { name: "request", ty: "UpdateActorRequest" }, return_type: "ActorObservation?" },
];

/// Async Rust top-level exports whose generated Kotlin function carries an
/// optional CancellationHandle and therefore receives a no-handle overload.
pub const KOTLIN_TOP_LEVEL_METHODS: &[KotlinTopLevelMethod] = &[
    KotlinTopLevelMethod {
        wrapper_name: "connectActors",
        generated_alias: "generatedConnectActors",
        parameters: ENDPOINT_TOKEN,
        return_type: "ActorsClient",
    },
    KotlinTopLevelMethod {
        wrapper_name: "connectActorsWithCa",
        generated_alias: "generatedConnectActorsWithCa",
        parameters: ENDPOINT_TOKEN_CA,
        return_type: "ActorsClient",
    },
];

const KOTLIN_IMPORTS: &[&str] = &[
    "ActorId",
    "ActorObservation",
    "ActorsClient",
    "AddSubscriptionRequest",
    "CancellationHandle",
    "CheckpointActorRequest",
    "CreateActorRequest",
    "InspectActorRequest",
    "InvokeActorRequest",
    "InvokeActorResponse",
    "RemoveSubscriptionRequest",
    "ResumeSubscriptionRequest",
    "UpdateActorRequest",
];

const KOTLIN_HELPER: &str = r#"/**
 * Generated package adapter for Rust async metadata carrying an optional
 * CancellationHandle. Callers never allocate or pass a handle.
 *
 * UniFFI 0.31.0 generated Kotlin has no Job.invokeOnCancellation hook, so
 * this adapter supplies only the cancellation bridge. It does not implement
 * transport, retries, request construction, validation, or response mapping.
 */
 suspend fun <T> automaticRustCancellation(
    operation: suspend (CancellationHandle) -> T,
): T {
    check(currentCoroutineContext()[Job] != null) {
        "automatic Rust cancellation requires a coroutine Job"
    }
    val job = currentCoroutineContext()[Job]!!
    val handle = CancellationHandle()
    val completionLock = Any()
    var operationCompleted = false
    val registration = job.invokeOnCompletion { cause ->
        if (cause is CancellationException) {
            synchronized(completionLock) {
                if (!operationCompleted) {
                    handle.cancel()
                }
            }
        }
    }
    try {
        val result = operation(handle)
        synchronized(completionLock) {
            operationCompleted = true
        }
        registration.dispose()
        return result
    } catch (error: Throwable) {
        if (error !is CancellationException) {
            synchronized(completionLock) {
                operationCompleted = true
            }
            registration.dispose()
        }
        throw error
    }
}
"#;

/// Renders the package-level Kotlin adapter from the Rust-owned export table.
pub fn render_kotlin_adapter() -> String {
    let mut output = String::new();
    output.push_str("package adapter\n\n");
    output.push_str("import kotlinx.coroutines.CancellationException\n");
    output.push_str("import kotlinx.coroutines.Job\n");
    output.push_str("import kotlinx.coroutines.currentCoroutineContext\n");
    for import in KOTLIN_IMPORTS {
        writeln!(output, "import uniffi.acyclic_actors_uniffi.{import}").unwrap();
    }
    output.push_str("import uniffi.acyclic_actors_uniffi.connectActors as generatedConnectActors\n");
    output.push_str("import uniffi.acyclic_actors_uniffi.connectActorsWithCa as generatedConnectActorsWithCa\n\n");
    output.push_str(KOTLIN_HELPER);
    output.push('\n');

    for method in KOTLIN_TOP_LEVEL_METHODS {
        write!(output, "suspend fun {}(", method.wrapper_name).unwrap();
        for (index, parameter) in method.parameters.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            write!(output, "{}: {}", parameter.name, parameter.ty).unwrap();
        }
        write!(output, "): {} = automaticRustCancellation {{ handle ->\n    {}(", method.return_type, method.generated_alias).unwrap();
        for (index, parameter) in method.parameters.iter().enumerate() {
            if index > 0 {
                output.push_str(", ");
            }
            output.push_str(parameter.name);
        }
        output.push_str(", handle)\n}\n\n");
    }

    for method in KOTLIN_CLIENT_METHODS {
        writeln!(
            output,
            "suspend fun ActorsClient.{}({}: {}): {} = automaticRustCancellation {{ handle -> {}({}, handle) }}\n",
            method.wrapper_name,
            method.argument.name,
            method.argument.ty,
            method.return_type,
            method.generated_name,
            method.argument.name,
        )
        .unwrap();
    }
    output
}