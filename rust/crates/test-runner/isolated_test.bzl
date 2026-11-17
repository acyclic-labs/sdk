"""A test that runs each libtest case of a Rust test executable in its own process."""

# A label in this file's own repository, whose canonical name is empty in the SDK's workspace.
_SDK = Label("//:BUILD.bazel")

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
        # This SDK's repository in the runfiles, which source_path resolves SDK files against:
        # the workspace directory itself only when the SDK is the main repository.
        "ACYCLIC_TEST_SOURCE": "../" + _SDK.repo_name if _SDK.repo_name else ".",
    }
    if ctx.attr.only:
        environment["ACYCLIC_TEST_ONLY"] = ctx.attr.only
    if ctx.attr.workers:
        environment["ACYCLIC_TEST_WORKERS"] = str(ctx.attr.workers)
    if ctx.attr.exclude:
        environment["ACYCLIC_TEST_EXCLUDE"] = "\n".join(ctx.attr.exclude)
    if ctx.file.wrapper:
        environment["ACYCLIC_TEST_WRAPPER"] = ctx.file.wrapper.short_path
        runfiles = runfiles.merge(ctx.runfiles(files = [ctx.file.wrapper] + ctx.files.wrapper_data))

    # Cargo's CARGO_BIN_EXE_<name>, relative to the runfiles root; the runner makes it absolute.
    # A filegroup with one file, such as the toolchain's Cargo, is that executable.
    for target, name in ctx.attr.binaries.items():
        info = target[DefaultInfo]
        files = info.files.to_list()
        tool = info.files_to_run.executable
        if tool == None:
            if len(files) != 1:
                fail("{}: {} names {} files, not one executable".format(ctx.label, name, len(files)))
            tool = files[0]
        environment["CARGO_BIN_EXE_" + name] = tool.short_path
        runfiles = runfiles.merge(info.default_runfiles)
        runfiles = runfiles.merge(ctx.runfiles(files = files))
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
        # Cases to run at once; zero uses the host's parallelism. A suite whose cases each start
        # Cargo sets this so they do not contend for the same cores.
        "workers": attr.int(default = 0),
        "wrapper_data": attr.label_list(allow_files = True),
        "limit_seconds": attr.int(mandatory = True),
        "_runner": attr.label(
            default = "//rust/crates/test-runner:acyclic-test-runner",
            executable = True,
            cfg = "target",
        ),
    },
)
