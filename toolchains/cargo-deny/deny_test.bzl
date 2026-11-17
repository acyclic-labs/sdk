"""Offline cargo-deny test whose action key follows Cargo's inputs."""

def _relative_external(file):
    parts = file.short_path.split("/")
    if len(parts) < 3 or parts[0] != "..":
        fail("expected external input, got " + file.short_path)
    return "/".join(parts[2:])

def _deny_test_impl(ctx):
    inputs = depset(ctx.files.srcs).to_list()
    mapping = ctx.actions.declare_file(ctx.label.name + ".inputs.json")
    ctx.actions.write(mapping, json.encode([[file.path, file.short_path] for file in inputs]))
    snapshot = ctx.actions.declare_directory(ctx.label.name + ".workspace")
    ctx.actions.run(
        executable = ctx.executable._node,
        arguments = [ctx.file._snapshot.path, snapshot.path, mapping.path],
        inputs = depset(inputs + [mapping, ctx.file._snapshot]),
        tools = [ctx.attr._node[DefaultInfo].files_to_run],
        outputs = [snapshot],
        mnemonic = "CargoDenyWorkspace",
    )
    links = {
        "deny-workspace": snapshot,
        "deny-tool/cargo-deny": ctx.file._deny,
        "deny-compiler/bin/cargo": ctx.file._cargo,
        "deny-compiler/bin/rustc": ctx.file._rustc,
    }
    for file in ctx.files._registry:
        links["deny-registry/" + _relative_external(file)] = file
    for file in ctx.files._advisories:
        links["deny-advisories/" + _relative_external(file)] = file
    for file in ctx.files._rustc_lib:
        links["deny-compiler/" + _relative_external(file)] = file
    runfiles = ctx.runfiles(files = [ctx.file._runner], symlinks = links).merge(ctx.attr._node[DefaultInfo].default_runfiles)
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(
        script,
        "#!/bin/sh\ncd \"$TEST_SRCDIR/$TEST_WORKSPACE\" || exit 1\nexec \"%s\" \"%s\"\n" % (ctx.executable._node.short_path, ctx.file._runner.short_path),
        is_executable = True,
    )
    return [DefaultInfo(executable = script, runfiles = runfiles)]

deny_test = rule(
    implementation = _deny_test_impl,
    test = True,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
        "_snapshot": attr.label(default = "//toolchains/cargo-deny:snapshot.mjs", allow_single_file = True),
        "_runner": attr.label(default = "//toolchains/cargo-deny:offline.mjs", allow_single_file = True),
        "_deny": attr.label(default = "@cargo_deny_binary//:cargo-deny", allow_single_file = True),
        "_cargo": attr.label(default = "@rust_linux_x86_64__x86_64-unknown-linux-gnu__stable_tools//:bin/cargo", allow_single_file = True),
        "_rustc": attr.label(default = "@rust_linux_x86_64__x86_64-unknown-linux-gnu__stable_tools//:bin/rustc", allow_single_file = True),
        "_rustc_lib": attr.label(default = "@rust_linux_x86_64__x86_64-unknown-linux-gnu__stable_tools//:rustc_lib", allow_files = True),
        "_advisories": attr.label(default = "@cargo_deny_advisories//:data", allow_files = True),
        "_registry": attr.label(default = "@cargo_deny_registry//:data", allow_files = True),
    },
)
