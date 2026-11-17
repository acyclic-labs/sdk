"""Standalone proofs over declared production sources, without Cargo or host tools."""

def _tool_impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(output = executable, target_file = ctx.file.driver, is_executable = True)
    files = [executable, ctx.file.driver] + ctx.files.data
    return [DefaultInfo(executable = executable, files = depset(files), runfiles = ctx.runfiles(files = files))]

kani_tool = rule(
    implementation = _tool_impl,
    attrs = {
        "driver": attr.label(allow_single_file = True, mandatory = True),
        "data": attr.label_list(allow_files = True),
    },
    executable = True,
)

def _quote(value):
    return "'" + value.replace("'", "'\"'\"'") + "'"

def _test_impl(ctx):
    if not ctx.attr.harness:
        fail("name one exact proof harness")
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(executable, """#!/usr/bin/env bash
set -euo pipefail
root="${TEST_SRCDIR:?}/${TEST_WORKSPACE:?}"
exec "$root/%s" "$root/%s" --target-dir "${TEST_TMPDIR:?}" --harness %s --exact --output-format terse
""" % (ctx.executable._kani.short_path, ctx.file.src.short_path, _quote(ctx.attr.harness)), is_executable = True)
    runfiles = ctx.runfiles(files = [ctx.file.src] + ctx.files.srcs)
    runfiles = runfiles.merge(ctx.attr._kani[DefaultInfo].default_runfiles)
    return [DefaultInfo(executable = executable, runfiles = runfiles)]

kani_test = rule(
    implementation = _test_impl,
    attrs = {
        "src": attr.label(allow_single_file = [".rs"], mandatory = True),
        "srcs": attr.label_list(allow_files = [".rs"]),
        "harness": attr.string(mandatory = True),
        "_kani": attr.label(default = Label("@kani//:kani"), executable = True, cfg = "exec"),
    },
    test = True,
)
