"""Run an existing test binary case as a separately cached REAPI test action."""

def _case(ctx):
    binary = ctx.executable.binary
    script = "#!/bin/bash\nset -euo pipefail\nexec \"${TEST_SRCDIR}/${TEST_WORKSPACE}/%s\" --exact %s \"$@\"\n" % (binary.short_path, ctx.attr.case)
    ctx.actions.write(ctx.outputs.executable, script, is_executable = True)
    return [DefaultInfo(
        executable = ctx.outputs.executable,
        runfiles = ctx.runfiles(files = [binary]).merge(ctx.attr.binary[DefaultInfo].default_runfiles),
    )]

binary_case_test = rule(
    implementation = _case,
    test = True,
    attrs = {
        "binary": attr.label(executable = True, cfg = "target", mandatory = True),
        "case": attr.string(mandatory = True),
    },
)