"""Standalone proofs over declared production sources, without Cargo or host tools."""

KaniInfo = provider(fields = ["bin"])

def _tool_impl(ctx):
    executable = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(output = executable, target_file = ctx.file.driver, is_executable = True)
    files = [executable, ctx.file.driver] + ctx.files.data
    return [
        DefaultInfo(executable = executable, files = depset(files), runfiles = ctx.runfiles(files = files)),
        KaniInfo(bin = ctx.file.driver.short_path.rpartition("/")[0]),
    ]

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

def _rustfmt_checks(ctx):
    toolchain = ctx.toolchains["@rules_rust//rust/rustfmt:toolchain_type"]
    marker = ctx.actions.declare_file(ctx.label.name + ".rustfmt.ok")
    sources = [ctx.file.src] + ctx.files.srcs
    args = ctx.actions.args()
    args.add("--touch-file", marker)
    args.add("--")
    args.add(toolchain.rustfmt)
    args.add("--config-path", ctx.file._rustfmt_config)
    args.add("--edition", "2024")
    args.add("--config", "skip_children=true")
    args.add("--check")
    args.add_all(sources)
    ctx.actions.run(
        executable = ctx.executable._process_wrapper,
        inputs = sources + [ctx.file._rustfmt_config],
        outputs = [marker],
        tools = [toolchain.all_files],
        arguments = [args],
        mnemonic = "Rustfmt",
        progress_message = "Rustfmt %{label}",
        toolchain = "@rules_rust//rust/rustfmt:toolchain_type",
    )
    return [OutputGroupInfo(rustfmt_checks = depset([marker]))]

def _test_impl(ctx):
    if not ctx.attr.harness:
        fail("name one exact proof harness")
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    command = "\"$root/%s\" \"$root/%s\" --target-dir \"${TEST_TMPDIR:?}\" --harness %s --exact --output-format terse" % (
        ctx.executable._kani.short_path,
        ctx.file.src.short_path,
        _quote(ctx.attr.harness),
    )
    for argument in ctx.attr.args:
        command += " " + _quote(argument)
    if ctx.attr.expect_failure:
        # A negative control must find a counterexample. A compile error or a
        # missing harness also exits nonzero, so require the solver's own
        # single-harness failure summary instead of the exit code alone.
        body = """#!/usr/bin/env bash
set -euo pipefail
root="${TEST_SRCDIR:?}/${TEST_WORKSPACE:?}"
export PATH="$root/%s:$PATH"
output="$(%s 2>&1)" && status=0 || status=$?
printf '%%s\\n' "$output"
if [ "$status" -eq 0 ]; then
  echo "expected a counterexample, but the harness verified" >&2
  exit 1
fi
grep -q "Complete - 0 successfully verified harnesses, 1 failures, 1 total." <<<"$output"
""" % (ctx.attr._kani[KaniInfo].bin, command)
    else:
        body = """#!/usr/bin/env bash
set -euo pipefail
root="${TEST_SRCDIR:?}/${TEST_WORKSPACE:?}"
export PATH="$root/%s:$PATH"
exec %s
""" % (ctx.attr._kani[KaniInfo].bin, command)
    ctx.actions.write(executable, body, is_executable = True)
    runfiles = ctx.runfiles(files = [ctx.file.src] + ctx.files.srcs)
    runfiles = runfiles.merge(ctx.attr._kani[DefaultInfo].default_runfiles)
    return [DefaultInfo(executable = executable, runfiles = runfiles)] + _rustfmt_checks(ctx)

kani_test = rule(
    implementation = _test_impl,
    attrs = {
        "src": attr.label(allow_single_file = [".rs"], mandatory = True),
        "srcs": attr.label_list(allow_files = [".rs"]),
        "harness": attr.string(mandatory = True),
        "args": attr.string_list(),
        "expect_failure": attr.bool(),
        "_kani": attr.label(default = Label("@kani//:kani"), executable = True, cfg = "exec"),
        "_process_wrapper": attr.label(default = Label("@rules_rust//util/process_wrapper"), executable = True, cfg = "exec"),
        "_rustfmt_config": attr.label(default = Label("@rules_rust//rust/settings:rustfmt.toml"), allow_single_file = True),
    },
    toolchains = ["@rules_rust//rust/rustfmt:toolchain_type"],
    test = True,
)
