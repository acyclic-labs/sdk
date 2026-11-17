"""Names the generator executables the bundle runs without starting a second Bazel."""

def _generator_executables_impl(ctx):
    output = ctx.actions.declare_file(ctx.label.name + ".json")
    executables = [target[DefaultInfo].files_to_run.executable for target in ctx.attr.targets]
    entries = {
        "//%s:%s" % (target.label.package, target.label.name): executable.path
        for target, executable in zip(ctx.attr.targets, executables)
    }
    ctx.actions.write(output, json.encode(entries) + "\n")
    return [DefaultInfo(files = depset([output]), runfiles = ctx.runfiles(files = executables))]

generator_executables = rule(
    implementation = _generator_executables_impl,
    attrs = {
        "targets": attr.label_list(mandatory = True, allow_files = False, providers = [DefaultInfo], cfg = "exec"),
    },
)
