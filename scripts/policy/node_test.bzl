"""Run policy conformance against the repository's pinned Node executable."""

def _node_test_impl(ctx):
    node = ctx.executable._node
    runner = ctx.actions.declare_file(ctx.label.name + ".sh")
    pattern = ["--test-name-pattern", ctx.attr.name_pattern] if ctx.attr.name_pattern else []
    args = " ".join(["'%s'" % arg.replace("'", "'\\''") for arg in pattern])
    bun = ctx.executable.bun
    environment = 'export BUN_BINARY="$TEST_SRCDIR/$TEST_WORKSPACE/%s"\n' % bun.short_path if bun else ""
    ctx.actions.write(runner, "#!/bin/sh\ncd \"$TEST_SRCDIR/$TEST_WORKSPACE\" || exit 1\n%sexec \"%s\" --test %s \"%s\"\n" % (environment, node.short_path, args, ctx.attr.entry), is_executable = True)
    runfiles = ctx.runfiles(files = ctx.files.srcs).merge(ctx.attr._node[DefaultInfo].default_runfiles)
    if bun:
        runfiles = runfiles.merge(ctx.attr.bun[DefaultInfo].default_runfiles).merge(ctx.runfiles(files = [bun]))
    return [DefaultInfo(executable = runner, runfiles = runfiles)]

node_test = rule(
    implementation = _node_test_impl,
    test = True,
    attrs = {
        "entry": attr.string(mandatory = True),
        "name_pattern": attr.string(),
        "srcs": attr.label_list(allow_files = True),
        "bun": attr.label(executable = True, allow_files = True, cfg = "exec"),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
    },
)
