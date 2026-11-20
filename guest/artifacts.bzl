"""The prepared virtio-fs guest as one directory output."""

def _guest_artifacts_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name + "/guest")
    args = ctx.actions.args()
    args.add("--output", out.path)
    args.add("--vmm", ctx.file.vmm.path)
    args.add("--vmm-sha256", ctx.attr.vmm_sha256)
    ctx.actions.run(
        executable = ctx.executable.prepare,
        arguments = [args],
        inputs = [ctx.file.script, ctx.file.vmm],
        tools = [ctx.attr.prepare[DefaultInfo].files_to_run],
        outputs = [out],
        use_default_shell_env = True,
        execution_requirements = {"no-sandbox": ""},
        mnemonic = "GuestArtifacts",
        progress_message = "Preparing the virtio-fs guest",
    )
    return [DefaultInfo(files = depset([out]))]

guest_artifacts = rule(
    implementation = _guest_artifacts_impl,
    attrs = {
        "prepare": attr.label(executable = True, cfg = "exec", mandatory = True),
        "script": attr.label(allow_single_file = True, mandatory = True),
        "vmm": attr.label(allow_single_file = True, mandatory = True),
        "vmm_sha256": attr.string(mandatory = True),
    },
)
