"""Assemble the SDK docs test metadata from the generated package records."""

def _assemble_impl(ctx):
    output = ctx.outputs.out
    args = ctx.actions.args()
    args.add(output)
    args.add(ctx.file.header)
    args.add_all(ctx.files.packages)
    ctx.actions.run(
        executable = ctx.executable._node,
        arguments = [ctx.file._script.path, args],
        inputs = [ctx.file._script, ctx.file.header] + ctx.files.packages,
        outputs = [output],
        mnemonic = "WorkspaceMetadata",
    )
    return [DefaultInfo(files = depset([output]))]

assemble_workspace_metadata = rule(
    implementation = _assemble_impl,
    attrs = {
        "header": attr.label(allow_single_file = True, mandatory = True),
        "packages": attr.label_list(allow_files = [".json"], mandatory = True),
        "out": attr.output(mandatory = True),
        "_node": attr.label(default = "@nodejs//:node", executable = True, cfg = "exec"),
        "_script": attr.label(default = ":assemble.mjs", allow_single_file = True),
    },
)
