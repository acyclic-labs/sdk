"""Names the generator executables the bundle runs without starting a second Bazel."""

GENERATOR_LABELS = [
    "//rust/crates/actors-wasm:actors-wasm-rpc-inventory",
    "//rust/crates/actors:actors-authorization_example",
    "//rust/crates/actors:actors-http-routes_example",
    "//rust/crates/actors:actors-rpc-inventory",
    "//rust/crates/auth:generate_example",
    "//rust/crates/filesystem-wire:filesystem-authorization_example",
    "//rust/crates/filesystem:filesystem-contracts_example",
    "//rust/crates/filesystem:filesystem-object-reference-contract_example",
    "//rust/crates/harness-core:agent-record-corpus_example",
    "//rust/crates/harness:file-tools-contract_example",
    "//rust/crates/harness:model-contract-fixtures_example",
    "//rust/crates/inference-wasm:inference-typescript-http-routes_example",
    "//rust/crates/inference-wasm:inference-typescript-terminal-metadata_example",
    "//rust/crates/inference-wasm:inference-typescript-widths_example",
    "//rust/crates/inference:inference-typescript-defaults_example",
    "//rust/crates/machines:machines-authorization_example",
    "//rust/crates/machines:machines-managed-oci-contract",
    "//rust/crates/manifest:deployment-manifest_example",
    "//rust/crates/objects:objects-authorization_example",
    "//rust/crates/objects:objects-rpc-inventory",
    "//rust/crates/proto-codegen:sdk-proto-codegen",
    "//rust/crates/stream:stream-authorization_example",
    "//rust/crates/stream:stream-http-contract",
    "//rust/crates/stream:stream-rpc-inventory",
    "//rust/crates/stream:token-operations_example",
    "//rust/crates/workers:workers-authorization_example",
]

def _generator_executables_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    manifest = ctx.actions.declare_file(ctx.label.name + ".json")
    executables = [target[DefaultInfo].files_to_run.executable for target in ctx.attr.targets]
    entries = {
        "//%s:%s" % (target.label.package, target.label.name): executable.basename
        for target, executable in zip(ctx.attr.targets, executables)
    }
    ctx.actions.write(manifest, json.encode(entries) + "\n")
    arguments = [output.path, manifest.path] + [executable.path for executable in executables]
    commands = ["mkdir -p \"$1\"", "cp \"$2\" \"$1/executables.json\""]
    for index in range(len(executables)):
        commands.append("cp \"$%d\" \"$1/%s\"" % (index + 3, executables[index].basename))
    ctx.actions.run_shell(
        inputs = [manifest] + executables,
        outputs = [output],
        arguments = arguments,
        command = " && ".join(commands),
        mnemonic = "GeneratorExecutables",
    )
    return [DefaultInfo(files = depset([output]), runfiles = ctx.runfiles(files = [output]))]

generator_executables = rule(
    implementation = _generator_executables_impl,
    attrs = {
        "targets": attr.label_list(mandatory = True, allow_files = False, providers = [DefaultInfo], cfg = "exec"),
    },
)
