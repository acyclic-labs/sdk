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
        "ACYCLIC_TEST_CPUS": str(ctx.attr.cpus),
        "ACYCLIC_TEST_ID": ctx.attr.id,
        "ACYCLIC_TEST_LIMIT_SECONDS": str(ctx.attr.limit_seconds),
        "ACYCLIC_TEST_PACKAGE": ctx.label.package,
        # This SDK's repository in the runfiles, which source_path resolves SDK files against:
        # the workspace directory itself only when the SDK is the main repository.
        "ACYCLIC_TEST_SOURCE": "../" + _SDK.repo_name if _SDK.repo_name else ".",
        # Cases' temporary directories are RAM-backed on Linux unless the target declares that
        # it needs a real disk's semantics.
        "ACYCLIC_TEST_TEMP_ROOT": "disk" if "requires-disk-tmp" in ctx.attr.tags else "memory",
    }
    for name, value in ctx.attr.env.items():
        environment[name] = ctx.expand_location(value, targets = ctx.attr.data)
    runfiles = runfiles.merge(ctx.runfiles(files = ctx.files.data))
    if ctx.attr.only:
        environment["ACYCLIC_TEST_ONLY"] = ctx.attr.only
    if ctx.attr.workers:
        environment["ACYCLIC_TEST_WORKERS"] = str(ctx.attr.workers)
    if ctx.attr.exclude:
        environment["ACYCLIC_TEST_EXCLUDE"] = "\n".join(ctx.attr.exclude)
    if ctx.file.wrapper and ctx.attr.privileges:
        fail("{}: a privileged test runs under the one privileged wrapper, not a profile's".format(ctx.label))
    if ctx.file.wrapper:
        environment["ACYCLIC_TEST_WRAPPER"] = ctx.file.wrapper.short_path
        runfiles = runfiles.merge(ctx.runfiles(files = [ctx.file.wrapper] + ctx.files.wrapper_data))
    inherited = list(ctx.attr.env_inherit)
    if ctx.attr.privileges:
        environment["ACYCLIC_TEST_WRAPPER"] = ctx.file._privileged.short_path
        environment["ACYCLIC_TEST_PRIVILEGES"] = " ".join(ctx.attr.privileges)
        runfiles = runfiles.merge(ctx.runfiles(files = [ctx.file._privileged, ctx.file._privileges]))
        if "fs-verity" in ctx.attr.privileges:
            inherited.append("ACYCLIC_TEST_VERITY_DIR")

    # Cargo's CARGO_BIN_EXE_<name>, relative to the runfiles root; the runner makes it absolute.
    # A filegroup with one file, such as the toolchain's Cargo, is that executable.
    # An external tool keeps its repository layout, including companion files beside the
    # executable. Locally built tools use aliases to avoid collisions with test data paths.
    for target, name in ctx.attr.binaries.items():
        info = target[DefaultInfo]
        files = info.files.to_list()
        tool = info.files_to_run.executable
        if tool == None:
            if len(files) != 1:
                fail("{}: {} names {} files, not one executable".format(ctx.label, name, len(files)))
            tool = files[0]
        data = info.data_runfiles
        if tool.short_path.startswith("../"):
            environment["CARGO_BIN_EXE_" + name] = tool.short_path
            runfiles = runfiles.merge(ctx.runfiles(files = [tool])).merge(data)
            continue
        extension = "." + tool.extension if tool.extension else ""
        package = ctx.label.package + "/" if ctx.label.package else ""
        path = package + ".cargo-bin/" + name + extension
        environment["CARGO_BIN_EXE_" + name] = path
        runfiles = runfiles.merge(ctx.runfiles(symlinks = {path: tool}))
        # The tool's own data runfiles never need its original executable entry; that entry
        # is what can obscure a test data directory with the same short path. Each link is a
        # SymlinkEntry, so read its path and target file rather than unpacking it.
        runfiles = runfiles.merge(ctx.runfiles(
            files = [file for file in data.files.to_list() if file != tool],
            symlinks = {
                entry.path: entry.target_file
                for entry in data.symlinks.to_list()
                if entry.target_file != tool
            },
            root_symlinks = {
                entry.path: entry.target_file
                for entry in data.root_symlinks.to_list()
                if entry.target_file != tool
            },
        ))
    return [
        DefaultInfo(executable = executable, runfiles = runfiles),
        # Only the variables this target declares reach it from the invoking environment, so a
        # host fact one test needs never enters every other test's action key.
        RunEnvironmentInfo(environment = environment, inherited_environment = inherited),
    ]

_isolated_test = rule(
    implementation = _isolated_test_impl,
    test = True,
    attrs = {
        # The CPUs the test reserves from Bazel's local resources, as its `cpu:N` tag: the runner
        # runs this many cases at once and never more, so tests Bazel runs together never
        # oversubscribe the host. Set only through `isolated_test`, which writes the tag.
        "cpus": attr.int(mandatory = True),
        "binary": attr.label(executable = True, cfg = "target", mandatory = True),
        "binaries": attr.label_keyed_string_dict(cfg = "target"),
        # The executable's nextest binary id, which names each of its cases in the report.
        "id": attr.string(mandatory = True),
        # A nextest profile's or privileged group's cases (`only`: newline-separated) run alone; `exclude` leaves them.
        "only": attr.string(),
        "exclude": attr.string_list(),
        # The profile's nextest run wrapper, which runs each case as `<wrapper> <binary> <arguments>`,
        # and the files it runs beside it.
        "wrapper": attr.label(allow_single_file = True),
        # Cases to run at once, capped by the CPU reservation; zero uses the reservation. A suite whose cases each start
        # Cargo sets this so they do not contend for the same cores.
        "workers": attr.int(default = 0),
        "wrapper_data": attr.label_list(allow_files = True),
        # Variables this test sets, with `$(rootpath ...)` expanded against `data`.
        "env": attr.string_dict(),
        # Files the test reads at run time, and the targets `env` expands against.
        "data": attr.label_list(allow_files = True),
        "limit_seconds": attr.int(mandatory = True),
        # Variables this test reads from the invoking environment (a host fact such as a scratch
        # directory on a filesystem with a required feature); the value is part of its action key.
        "env_inherit": attr.string_list(),
        # The host privileges the cases need; they run under `_privileged`, which grants only these. Set only
        # through `isolated_test`, which writes the `privileged` and `requires-<privilege>` tags.
        "privileges": attr.string_list(),
        "_privileged": attr.label(default = "//rust/crates/test-runner:privileged.sh", allow_single_file = True),
        "_privileges": attr.label(default = "//rust/crates/test-runner:privileges.txt", allow_single_file = True),
        "_runner": attr.label(
            default = "//rust/crates/test-runner:acyclic-test-runner",
            executable = True,
            cfg = "target",
        ),
    },
)

def isolated_test(name, cpus = 2, tags = [], privileges = [], **kwargs):
    """Runs each case of `binary` in its own process, at most `cpus` cases at once.

    The test reserves `cpus` of Bazel's local CPU resources through its `cpu:N` tag, and the
    runner's worker count cannot exceed that reservation; workers may reduce it.

    Args:
      name: The test's name.
      cpus: The CPUs the test reserves, and so the cases it runs at once.
      tags: The test's other tags; a `cpu:` or `privileged` tag of its own is refused.
      privileges: The host privileges its cases need (privileged.sh names them), which tag it for the privileged leg.
      **kwargs: The rule's other attributes.
    """
    if cpus < 1:
        fail("isolated_test %s: cpus must be at least 1, not %d" % (name, cpus))
    if [tag for tag in tags if tag.startswith("cpu:")]:
        fail("isolated_test %s: the reservation is `cpus`, not a cpu: tag" % name)
    if "privileged" in tags:
        fail("isolated_test %s: `privileges` writes the privileged tag" % name)
    privileged = ["privileged"] + ["requires-" + privilege for privilege in privileges] if privileges else []
    if "fs-verity" in privileges:
        # Its cases check host root's ownership, which linux-sandbox's user namespace hides (privileged.sh).
        if len(privileges) > 1:
            fail("isolated_test %s: fs-verity is a target's only privilege, not with %s" % (name, privileges))
        if "no-sandbox" not in tags:
            privileged.append("no-sandbox")
    _isolated_test(name = name, cpus = cpus, tags = tags + privileged + ["cpu:%d" % cpus], privileges = privileges, **kwargs)
