"""Run policy conformance against the repository's pinned Node executable."""

def _node_test_impl(ctx):
    node = ctx.executable._node
    runner = ctx.actions.declare_file(ctx.label.name + ".sh")
    pattern = ["--test-name-pattern", ctx.attr.name_pattern] if ctx.attr.name_pattern else []
    args = " ".join(["'%s'" % arg.replace("'", "'\\''") for arg in pattern])
    ctx.actions.write(runner, "#!/bin/sh\ncd \"$TEST_SRCDIR/$TEST_WORKSPACE\" || exit 1\nexec \"%s\" --test %s \"%s\"\n" % (node.short_path, args, ctx.attr.entry), is_executable = True)
    return [DefaultInfo(executable = runner, runfiles = ctx.runfiles(files = ctx.files.srcs).merge(ctx.attr._node[DefaultInfo].default_runfiles))]

node_test = rule(
    implementation = _node_test_impl,
    test = True,
    attrs = {
        "entry": attr.string(mandatory = True),
        "name_pattern": attr.string(),
        "srcs": attr.label_list(allow_files = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
    },
)
