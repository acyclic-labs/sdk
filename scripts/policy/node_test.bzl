"""Run policy conformance against the repository's pinned Node executable."""

def _node_test_impl(ctx):
    node = ctx.executable._node
    runner = ctx.actions.declare_file(ctx.label.name + ".sh")
    pattern = ["--test-name-pattern", ctx.attr.name_pattern] if ctx.attr.name_pattern else []
    reporter = ctx.file._selection_reporter if pattern else None
    reporters = "--test-reporter=tap --test-reporter-destination=stdout"
    if reporter:
        reporters += ' --test-reporter="$runfiles/%s" --test-reporter-destination=stderr' % reporter.short_path
    node_options = "--preserve-symlinks --preserve-symlinks-main " if ctx.attr.preserve_symlinks else ""
    args = " ".join(["'%s'" % arg.replace("'", "'\\''") for arg in pattern])
    # The entry is repository-relative; the tool short paths are runfiles-relative.
    repo = ctx.label.workspace_name
    workdir = '"$TEST_SRCDIR/%s"' % repo if repo else '"$runfiles"'
    bun = ctx.executable.bun
    environment = 'export BUN_BINARY="$TEST_SRCDIR/$TEST_WORKSPACE/%s"\n' % bun.short_path if bun else ""
    # Policy runs the pinned Python and rustfmt, never whichever ones the host has on its PATH.
    python = ctx.toolchains["@rules_python//python:toolchain_type"].py3_runtime
    rustfmt = ctx.toolchains["@rules_rust//rust/rustfmt:toolchain_type"]
    environment += 'export PYTHON_BINARY="$TEST_SRCDIR/$TEST_WORKSPACE/%s"\n' % python.interpreter.short_path
    environment += 'export RUSTFMT_BINARY="$TEST_SRCDIR/$TEST_WORKSPACE/%s"\n' % rustfmt.rustfmt.short_path
    ctx.actions.write(runner, "#!/bin/sh\nrunfiles=\"$TEST_SRCDIR/$TEST_WORKSPACE\"\ncd %s || exit 1\n%sexec \"$runfiles/%s\" %s--test %s %s \"%s\"\n" % (workdir, environment, node.short_path, node_options, reporters, args, ctx.attr.entry), is_executable = True)
    links = {path: target.files.to_list()[0] for target, path in ctx.attr.links.items()}
    runfiles = ctx.runfiles(files = ctx.files.srcs + ([reporter] if reporter else []), symlinks = links).merge_all([
        ctx.attr._node[DefaultInfo].default_runfiles,
        ctx.runfiles(transitive_files = depset([python.interpreter], transitive = [python.files])),
        ctx.runfiles(transitive_files = rustfmt.all_files),
    ])
    if bun:
        runfiles = runfiles.merge(ctx.attr.bun[DefaultInfo].default_runfiles).merge(ctx.runfiles(files = [bun]))
    return [DefaultInfo(executable = runner, runfiles = runfiles)]

node_test = rule(
    implementation = _node_test_impl,
    test = True,
    attrs = {
        "entry": attr.string(mandatory = True),
        "name_pattern": attr.string(),
        "preserve_symlinks": attr.bool(),
        "srcs": attr.label_list(allow_files = True),
        "links": attr.label_keyed_string_dict(allow_files = True),
        "bun": attr.label(executable = True, allow_files = True, cfg = "exec"),
        "_selection_reporter": attr.label(default = Label("//scripts/policy:test-selection-reporter.mjs"), allow_single_file = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
    },
    toolchains = ["@rules_python//python:toolchain_type", "@rules_rust//rust/rustfmt:toolchain_type"],
)
