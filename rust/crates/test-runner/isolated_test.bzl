"""A test that runs each libtest case of a Rust test executable in its own process."""

def _isolated_test_impl(ctx):
    runner = ctx.executable._runner
    extension = "." + runner.extension if runner.extension else ""
    executable = ctx.actions.declare_file(ctx.label.name + extension)
    ctx.actions.symlink(output = executable, target_file = runner, is_executable = True)
    runfiles = ctx.runfiles(files = [runner, ctx.executable.binary])
    runfiles = runfiles.merge(ctx.attr.binary[DefaultInfo].default_runfiles)
    return [
        DefaultInfo(executable = executable, runfiles = runfiles),
        RunEnvironmentInfo(environment = {
            "ACYCLIC_TEST_BINARY": ctx.executable.binary.short_path,
            "ACYCLIC_TEST_LIMIT_SECONDS": str(ctx.attr.limit_seconds),
            "ACYCLIC_TEST_PACKAGE": ctx.label.package,
        }),
    ]

isolated_test = rule(
    implementation = _isolated_test_impl,
    test = True,
    attrs = {
        "binary": attr.label(executable = True, cfg = "target", mandatory = True),
        "limit_seconds": attr.int(mandatory = True),
        "_runner": attr.label(
            default = "//rust/crates/test-runner:acyclic-test-runner",
            executable = True,
            cfg = "target",
        ),
    },
)
