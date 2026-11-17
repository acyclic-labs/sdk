"""Build a single advisory index from the pinned archive."""

def _index_impl(ctx):
    out = ctx.actions.declare_file(ctx.label.name + ".json")
    ctx.actions.run(
        executable = ctx.executable._node,
        arguments = [ctx.file._generator.path, ctx.file._archive.path, out.path],
        inputs = [ctx.file._generator, ctx.file._ranges, ctx.file._archive],
        tools = [ctx.attr._node[DefaultInfo].files_to_run],
        outputs = [out],
        mnemonic = "RustSecIndex",
    )
    return [DefaultInfo(files = depset([out]))]

advisory_index = rule(
    implementation = _index_impl,
    attrs = {
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
        "_generator": attr.label(default = "//toolchains/cargo-deny:build-advisory-index.mjs", allow_single_file = True),
        "_ranges": attr.label(default = "//toolchains/cargo-deny:advisory-ranges.mjs", allow_single_file = True),
        "_archive": attr.label(default = "@cargo_deny_advisories//:advisories.tar.gz", allow_single_file = True),
    },
)

def _index_test_impl(ctx):
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(script, "#!/bin/sh\ncd \"$TEST_SRCDIR/$TEST_WORKSPACE\" || exit 1\nexec \"%s\" --test \"%s\"\n" % (ctx.executable._node.short_path, ctx.file._test.short_path), is_executable = True)
    runfiles = ctx.runfiles(files = [ctx.file._test, ctx.file._ranges]).merge(ctx.attr._node[DefaultInfo].default_runfiles)
    return [DefaultInfo(executable = script, runfiles = runfiles)]

index_test = rule(
    implementation = _index_test_impl,
    test = True,
    attrs = {
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
        "_test": attr.label(default = "//toolchains/cargo-deny:advisory-ranges.test.mjs", allow_single_file = True),
        "_ranges": attr.label(default = "//toolchains/cargo-deny:advisory-ranges.mjs", allow_single_file = True),
    },
)
