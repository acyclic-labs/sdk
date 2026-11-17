"""A test that runs each libtest case of a Rust test executable in its own process."""

def _isolated_test_impl(ctx):
    runner = ctx.executable._runner
    extension = "." + runner.extension if runner.extension else ""
    executable = ctx.actions.declare_file(ctx.label.name + extension)
    ctx.actions.symlink(output = executable, target_file = runner, is_executable = True)
    runfiles = ctx.runfiles(files = [runner, ctx.executable.binary])
    runfiles = runfiles.merge(ctx.attr.binary[DefaultInfo].default_runfiles)
    environment = {
        "ACYCLIC_TEST_BINARY": ctx.executable.binary.short_path,
        "ACYCLIC_TEST_ID": ctx.attr.id,
        "ACYCLIC_TEST_LIMIT_SECONDS": str(ctx.attr.limit_seconds),
        "ACYCLIC_TEST_PACKAGE": ctx.label.package,
    }
    if ctx.attr.only:
        environment["ACYCLIC_TEST_ONLY"] = ctx.attr.only
    if ctx.attr.exclude:
        environment["ACYCLIC_TEST_EXCLUDE"] = "\n".join(ctx.attr.exclude)
    if ctx.file.wrapper:
        environment["ACYCLIC_TEST_WRAPPER"] = ctx.file.wrapper.short_path
        runfiles = runfiles.merge(ctx.runfiles(files = [ctx.file.wrapper] + ctx.files.wrapper_data))

    # Cargo's CARGO_BIN_EXE_<name>, relative to the runfiles root; the runner makes it absolute.
    for target, name in ctx.attr.binaries.items():
        info = target[DefaultInfo]
        environment["CARGO_BIN_EXE_" + name] = info.files_to_run.executable.short_path
        runfiles = runfiles.merge(info.default_runfiles)
    return [
        DefaultInfo(executable = executable, runfiles = runfiles),
        RunEnvironmentInfo(environment = environment),
    ]

isolated_test = rule(
    implementation = _isolated_test_impl,
    test = True,
    attrs = {
        "binary": attr.label(executable = True, cfg = "target", mandatory = True),
        "binaries": attr.label_keyed_string_dict(cfg = "target"),
        # The executable's nextest binary id, which names each of its cases in the report.
        "id": attr.string(mandatory = True),
        # A nextest profile's cases (`test(<pattern>)`): `only` runs them alone, `exclude` leaves them.
        "only": attr.string(),
        "exclude": attr.string_list(),
        # The profile's nextest run wrapper, which runs each case as `<wrapper> <binary> <arguments>`,
        # and the files it runs beside it.
        "wrapper": attr.label(allow_single_file = True),
        "wrapper_data": attr.label_list(allow_files = True),
        "limit_seconds": attr.int(mandatory = True),
        "_runner": attr.label(
            default = "//rust/crates/test-runner:acyclic-test-runner",
            executable = True,
            cfg = "target",
        ),
    },
)
